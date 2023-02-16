use crate::{cloud_provider_db, config::Config};
use async_std::{sync::RwLock, task::block_on};
use client::Client;
use futures::prelude::*;
use futures_timer::Delay;
use geohash::Coord;
use libp2p::{
    identify,
    kad::KademliaEvent,
    multiaddr::{Multiaddr, Protocol},
    ping, PeerId,
};
use log::info;
use maxminddb::{geoip2, Reader};
use node_store::{Node, NodeStore};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::{
    convert::TryInto,
    error::Error,
    net::IpAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

mod client;
pub(crate) mod node_store;

const TICK_INTERVAL: Duration = Duration::from_secs(1);
const RANDOM_LOOKUP_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) struct Exporter {
    // TODO: Introduce dht id new type.
    client: Client,
    node_store: Arc<RwLock<NodeStore>>,
    ip_db: Option<Reader<Vec<u8>>>,
    cloud_provider_db: Option<cloud_provider_db::Db>,
    tick: Delay,
    random_lookup_delay: Delay,
    metrics: Metrics,
    /// An exporter periodically reconnects to each discovered node to probe
    /// whether it is still online.
    nodes_to_probe_periodically: Vec<PeerId>,
}

impl Exporter {
    pub(crate) fn new(
        config: Config,
        ip_db: Option<Reader<Vec<u8>>>,
        cloud_provider_db: Option<cloud_provider_db::Db>,
        registry: &mut Registry,
    ) -> Result<Self, Box<dyn Error>> {
        let client = client::Client::new(config, registry).unwrap();

        let sub_registry = registry.sub_registry_with_prefix("kademlia_exporter");

        let metrics = Metrics::register(sub_registry);

        let node_store_metrics = node_store::Metrics::register(sub_registry);
        let node_store = Arc::new(RwLock::new(NodeStore::new(node_store_metrics)));

        Ok(Exporter {
            client,
            metrics,
            ip_db,
            cloud_provider_db,
            node_store,

            tick: futures_timer::Delay::new(TICK_INTERVAL),
            random_lookup_delay: futures_timer::Delay::new(RANDOM_LOOKUP_INTERVAL),
            nodes_to_probe_periodically: vec![],
        })
    }

    async fn record_event(&mut self, event: client::ClientEvent) {
        match event {
            client::ClientEvent::Behaviour(client::MyBehaviourEvent::Ping(ping::Event {
                peer,
                result,
            })) => {
                // Update node store.
                match result {
                    Ok(_) => self.node_store.write().await.observed_node(Node::new(peer)),
                    Err(_) => self.node_store.write().await.observed_down(&peer),
                }
            }
            client::ClientEvent::Behaviour(client::MyBehaviourEvent::Identify(event)) => {
                match event {
                    identify::Event::Error { .. } => {}
                    identify::Event::Sent { .. } => {}
                    identify::Event::Received { peer_id, info } => {
                        self.observe_with_address(peer_id, info.listen_addrs).await;
                        self.node_store
                            .write()
                            .await
                            .observed_node(Node::new(peer_id));
                    }
                    identify::Event::Pushed { .. } => {
                        unreachable!("Exporter never pushes identify information.")
                    }
                }
            }
            client::ClientEvent::Behaviour(client::MyBehaviourEvent::Kademlia(event)) => {
                match event {
                    KademliaEvent::RoutablePeer { peer, address } => {
                        self.observe_with_address(peer, vec![address]).await;
                    }
                    KademliaEvent::PendingRoutablePeer { peer, address } => {
                        self.observe_with_address(peer, vec![address]).await;
                    }
                    KademliaEvent::RoutingUpdated {
                        peer, addresses, ..
                    } => {
                        self.observe_with_address(peer, addresses.into_vec()).await;
                    }
                    _ => {}
                }
            }
            client::ClientEvent::Behaviour(client::MyBehaviourEvent::KeepAlive(v)) => {
                void::unreachable(v)
            }
            client::ClientEvent::AllConnectionsClosed(peer_id) => {
                self.node_store.write().await.observed_down(&peer_id);
            }
        }
    }

    async fn observe_with_address(&mut self, peer: PeerId, addresses: Vec<Multiaddr>) {
        let mut node = Node::new(peer);

        for address in addresses {
            if let Some(ip) = match address.iter().next() {
                Some(Protocol::Ip4(addr)) => Some(IpAddr::V4(addr)),
                Some(Protocol::Ip6(addr)) => Some(IpAddr::V6(addr)),
                _ => None,
            } {
                if ip.is_global() {
                    node.address = Some(ip);
                    break;
                }
            }
        }

        if let Some(address) = node.address {
            node.country = self.ip_to_country_code(address);
            node.provider = self.ip_to_cloud_provider(address);
            node.geohash = self.ip_to_geohash(address);
        }

        self.node_store.write().await.observed_node(node);
    }

    fn ip_to_cloud_provider(&self, address: IpAddr) -> Option<String> {
        if let IpAddr::V4(addr) = address {
            if let Some(db) = &self.cloud_provider_db {
                return db.get_provider(addr);
            }
        }
        None
    }

    fn ip_to_country_code(&self, address: IpAddr) -> Option<String> {
        if let Some(ip_db) = &self.ip_db {
            return Some(
                ip_db
                    .lookup::<geoip2::City>(address)
                    .ok()?
                    .country?
                    .iso_code?
                    .to_string(),
            );
        }
        None
    }

    fn ip_to_geohash(&self, address: IpAddr) -> Option<String> {
        if let Some(ip_db) = &self.ip_db {
            let city = ip_db.lookup::<geoip2::City>(address).ok()?;
            let location = city.location?;

            if let (Some(latitude), Some(longitude)) = (location.latitude, location.longitude) {
                let c = Coord {
                    y: latitude,
                    x: longitude,
                };
                return Some(geohash::encode(c, 5).expect("Geohash encoding failed."));
            }
        }
        None
    }

    pub fn node_store(&self) -> Arc<RwLock<NodeStore>> {
        self.node_store.clone()
    }
}

impl Future for Exporter {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;

        if let Poll::Ready(()) = this.tick.poll_unpin(ctx) {
            this.tick = Delay::new(TICK_INTERVAL);

            block_on(async {
                this.node_store.write().await.tick();
            });

            match this.nodes_to_probe_periodically.pop() {
                Some(peer_id) => {
                    info!("Checking if {:?} is still online.", &peer_id);
                    match this.client.dial(&peer_id) {
                        // New connection was established.
                        Ok(true) => {
                            this.metrics.meta_node_still_online_check_triggered.inc();
                        }
                        // Already connected to node.
                        Ok(false) => {}
                        // Connection limit reached. Retry later.
                        Err(_) => this.nodes_to_probe_periodically.insert(0, peer_id),
                    }
                }
                // List is empty. Reconnected to every peer. Refill the
                // list.
                None => block_on(async {
                    this.nodes_to_probe_periodically.append(
                        &mut this
                            .node_store
                            .read()
                            .await
                            .iter()
                            .map(|n| n.peer_id)
                            .collect(),
                    );
                }),
            }

            let info = this.client.network_info();
            this.metrics
                .meta_libp2p_network_info_num_peers
                .set(info.num_peers().try_into().unwrap());
            this.metrics
                .meta_libp2p_network_info_num_connections
                .set(info.connection_counters().num_connections().into());
            this.metrics
                .meta_libp2p_network_info_num_connections_established
                .set(info.connection_counters().num_established().into());
            this.metrics
                .meta_libp2p_network_info_num_connections_pending
                .set(info.connection_counters().num_pending().into());
            this.metrics
                .meta_libp2p_bandwidth_inbound
                .set(this.client.total_inbound() as i64);
            this.metrics
                .meta_libp2p_bandwidth_outbound
                .set(this.client.total_outbound() as i64);
        }

        if let Poll::Ready(()) = this.random_lookup_delay.poll_unpin(ctx) {
            this.random_lookup_delay = Delay::new(RANDOM_LOOKUP_INTERVAL);

            // Trigger a random lookup.
            this.metrics.meta_random_node_lookup_triggered.inc();
            let random_peer = PeerId::random();
            this.client.get_closest_peers(random_peer);
        }

        loop {
            match this.client.poll_next_unpin(ctx) {
                Poll::Ready(Some(event)) => block_on(async {
                    this.record_event(event).await;
                }),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }

        Poll::Pending
    }
}

struct Metrics {
    meta_random_node_lookup_triggered: Counter,
    meta_node_still_online_check_triggered: Counter,
    meta_libp2p_network_info_num_peers: Gauge,
    meta_libp2p_network_info_num_connections: Gauge,
    meta_libp2p_network_info_num_connections_pending: Gauge,
    meta_libp2p_network_info_num_connections_established: Gauge,
    meta_libp2p_bandwidth_inbound: Gauge,
    meta_libp2p_bandwidth_outbound: Gauge,
}

impl Metrics {
    fn register(registry: &mut Registry) -> Metrics {
        let meta_random_node_lookup_triggered = Counter::default();
        registry.register(
            "meta_random_node_lookup_triggered",
            "Number of times a random Kademlia node lookup was triggered",
            meta_random_node_lookup_triggered.clone(),
        );

        let meta_node_still_online_check_triggered = Counter::default();
        registry.register(
            "meta_node_still_online_check_triggered",
            "Number of times a connection to a node was established to ensure it is still online",
            meta_node_still_online_check_triggered.clone(),
        );

        let meta_libp2p_network_info_num_peers = Gauge::default();
        registry.register(
            "meta_libp2p_network_info_num_peers",
            "The total number of connected peers",
            meta_libp2p_network_info_num_peers.clone(),
        );

        let meta_libp2p_network_info_num_connections = Gauge::default();
        registry.register(
            "meta_libp2p_network_info_num_connections",
            "The total number of connections, both established and pending",
            meta_libp2p_network_info_num_peers.clone(),
        );

        let meta_libp2p_network_info_num_connections_pending = Gauge::default();
        registry.register(
            "meta_libp2p_network_info_num_connections_pending",
            "The total number of pending connections, both incoming and outgoing",
            meta_libp2p_network_info_num_connections_pending.clone(),
        );

        let meta_libp2p_network_info_num_connections_established = Gauge::default();
        registry.register(
            "meta_libp2p_network_info_num_connections_established",
            "The total number of established connections",
            meta_libp2p_network_info_num_connections_established.clone(),
        );

        let meta_libp2p_bandwidth_inbound = Gauge::default();
        registry.register(
            "meta_libp2p_bandwidth_inbound",
            "The total number of bytes received on the socket",
            meta_libp2p_bandwidth_inbound.clone(),
        );

        let meta_libp2p_bandwidth_outbound = Gauge::default();
        registry.register(
            "meta_libp2p_bandwidth_outbound",
            "The total number of bytes sent on the socket",
            meta_libp2p_bandwidth_outbound.clone(),
        );

        Metrics {
            meta_random_node_lookup_triggered,
            meta_node_still_online_check_triggered,
            meta_libp2p_network_info_num_peers,
            meta_libp2p_network_info_num_connections,
            meta_libp2p_network_info_num_connections_pending,
            meta_libp2p_network_info_num_connections_established,
            meta_libp2p_bandwidth_inbound,
            meta_libp2p_bandwidth_outbound,
        }
    }
}

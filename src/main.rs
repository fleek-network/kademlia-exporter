#![feature(ip)]
#![feature(hash_drain_filter)]

use crate::exporter::node_store::{Node, NodeStore};
use async_std::{sync::RwLock, task};
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;
use serde_derive::Serialize;
use serde_json::json;
use std::{
    collections::HashMap,
    error::Error,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use structopt::StructOpt;

mod cloud_provider_db;
mod config;
mod exporter;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Kademlia exporter",
    about = "Monitor the state of a Kademlia Dht."
)]
struct Opt {
    #[structopt(long)]
    config_file: PathBuf,
}

type State = tide::Request<(Arc<Mutex<Registry>>, Arc<RwLock<NodeStore>>)>;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let opt = Opt::from_args();
    let config = config::Config::from_file(opt.config_file);

    let (signal, exit) = exit_future::signal();
    let signal = Arc::new(Mutex::new(Some(signal)));

    ctrlc::set_handler(move || {
        if let Some(signal) = signal.lock().unwrap().take() {
            signal.fire().unwrap();
        }
    })
    .unwrap();

    let mut registry = Registry::default();

    let ip_db = config
        .max_mind_db_path
        .clone()
        .map(|path| maxminddb::Reader::open_readfile(path).expect("Failed to open max mind db."));
    let cloud_provider_db = config
        .cloud_provider_cidr_db_path
        .clone()
        .map(|path| cloud_provider_db::Db::new(path).expect("Failed to parse cloud provider db."));
    let exporter = exporter::Exporter::new(config, ip_db, cloud_provider_db, &mut registry)?;

    let registry = Arc::new(Mutex::new(registry));

    let exit_clone = exit.clone();
    let node_store = exporter.node_store();
    let metrics_server = std::thread::spawn(move || {
        let mut app = tide::with_state((registry, node_store));

        app.at("/metrics").get(|req: State| async move {
            let mut buffer = String::new();
            encode(&mut buffer, &req.state().0.lock().unwrap()).unwrap();

            Ok(buffer)
        });
        app.at("/http_sd").get(|req: State| async move {
            let node_store = req.state().1.read().await;
            let chunks: Vec<PrometheusDiscoveryChunk> =
                node_store.iter().map(|node| node.into()).collect();

            Ok(json!(chunks))
        });

        let endpoint = app.listen("0.0.0.0:8080");
        futures::pin_mut!(endpoint);
        task::block_on(exit_clone.until(endpoint))
    });

    task::block_on(exit.until(exporter));

    metrics_server.join().unwrap();
    Ok(())
}

impl From<&Node> for PrometheusDiscoveryChunk {
    fn from(node: &Node) -> Self {
        let mut labels = HashMap::new();
        labels.insert("id".to_string(), node.peer_id.to_string());

        if let Some(country) = node.country.clone() {
            labels.insert("country_code".into(), country);
        }
        
        if let Some(geohash) = node.geohash.clone() {
            labels.insert("geohash".to_string(), geohash);
        }

        let mut targets = vec![];
        if let Some(addr) = node.address {
            // our current provided NGINX configuration won't forward port 80
            // from the ip address, so we assume http is on 4069 (internal)
            targets.push(format!("{addr}:4069"));
        }

        PrometheusDiscoveryChunk {
            targets,
            labels,
        }
    }
}

/// Prometheus HTTP service discovery chunk.
/// Targets are expected to provide a `/metrics` endpoint
#[derive(Serialize, Debug)]
pub struct PrometheusDiscoveryChunk {
    targets: Vec<String>,
    labels: HashMap<String, String>,
}

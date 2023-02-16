docker-build:
	docker build . -t ursa-exporter
    
docker-run:
	docker run -p 8080:8080 --workdir /root \
	-v $(PWD):/root \
	-v /var/lib/GeoIP:/var/lib/GeoIP \
	ursa-exporter --config-file config.toml

docker: docker-build docker-run


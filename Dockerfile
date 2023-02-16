FROM rustlang/rust:nightly-bullseye as builder
WORKDIR /usr/src/ursa-exporter

RUN apt-get update && apt-get install -y cmake protobuf-compiler

# Cache dependencies between test runs,
# See https://blog.mgattozzi.dev/caching-rust-docker-builds/
# And https://github.com/rust-lang/cargo/issues/2644

RUN mkdir -p ./src/
RUN echo "fn main() {}" > ./src/main.rs
COPY ./Cargo.* ./
RUN cargo +nightly build --release

COPY . .
# This is in order to make sure `main.rs`s mtime timestamp is updated to avoid the dummy `main`
# remaining in the binary.
# https://github.com/rust-lang/cargo/issues/9598
RUN touch ./src/main.rs
RUN cargo +nightly build --release

FROM debian:bullseye-slim
COPY --from=builder /usr/src/ursa-exporter/target/release/ursa-exporter /usr/local/bin/ursa-exporter
ENTRYPOINT [ "ursa-exporter"]

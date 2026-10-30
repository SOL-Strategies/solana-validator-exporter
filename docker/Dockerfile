FROM rust:1.82-slim AS builder
RUN apt-get update -y && apt-get upgrade -y && apt-get install -y libssl-dev pkg-config
WORKDIR /app
COPY ../ /app
RUN cargo build --release

FROM debian:stable-slim AS release
RUN apt-get update && apt-get upgrade -y && apt-get install ca-certificates adduser -y
RUN adduser --disabled-password --home /home/exporter --gecos "" exporter
USER exporter
COPY --from=builder /app/target/release/solana-validator-exporter /usr/local/bin
ENTRYPOINT ["solana-validator-exporter"]

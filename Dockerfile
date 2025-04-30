FROM rust:slim as dev

ENV CARGO_TARGET_DIR /tmp/target/

RUN rustup component add clippy rustfmt

RUN apt update && apt install -y libacl1-dev g++ cmake git fuse3 libfuse3-dev pkg-config

RUN cargo install -f portable-network-archive

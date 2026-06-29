FROM rust:1.90-slim-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY nervi-core/Cargo.toml nervi-core/Cargo.toml
COPY nervi-server/Cargo.toml nervi-server/Cargo.toml
# Dummy source for dependency caching
RUN mkdir -p nervi-core/src nervi-server/src && \
    echo "pub fn _dummy() {}" > nervi-core/src/lib.rs && \
    echo "fn main() {}" > nervi-server/src/main.rs && \
    cargo build --release --bin nervi-server && \
    rm -rf nervi-core/src nervi-server/src
# Real source
COPY nervi-core/src nervi-core/src
COPY nervi-server/src nervi-server/src
RUN touch nervi-core/src/lib.rs nervi-server/src/main.rs && \
    cargo build --release --bin nervi-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/nervi-server /usr/local/bin/nervi-server
EXPOSE 8080
CMD ["nervi-server"]

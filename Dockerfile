# Build stage — the FIPS module builds from source and needs CMake + Go.
FROM rust:1.97-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake golang-go perl && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release -p rsearch-server

# Runtime stage — single static-ish binary, no JVM, no GC.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /usr/sbin/nologin rsearch \
    && mkdir -p /var/lib/rsearch && chown rsearch /var/lib/rsearch
COPY --from=build /src/target/release/rsearch /usr/local/bin/rsearch
USER rsearch
ENV RSEARCH_NODE__DATA_DIR=/var/lib/rsearch
EXPOSE 9200
ENTRYPOINT ["/usr/local/bin/rsearch"]

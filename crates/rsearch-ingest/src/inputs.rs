//! Network log inputs: syslog (UDP + TCP, optional TLS) and GELF
//! (TCP null-framed, optional TLS). Messages are micro-batched before
//! hitting the WAL so datagram floods don't fsync per message.

use std::time::Duration;

use rsearch_common::config::{GelfInputConfig, InputsConfig, SyslogInputConfig};
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

use crate::gelf::parse_gelf;
use crate::pipeline::IngestPipeline;
use crate::syslog::parse_syslog;

const BATCH_MAX: usize = 500;
const BATCH_AGE_MS: u64 = 200;

/// Start all enabled inputs. Returns an error only for configuration
/// problems (bad TLS material, unbindable ports).
pub async fn spawn_inputs(
    config: &InputsConfig,
    pipeline: IngestPipeline,
) -> Result<(), String> {
    if config.syslog.enabled {
        spawn_syslog(&config.syslog, pipeline.clone()).await?;
    }
    if config.gelf.enabled {
        spawn_gelf(&config.gelf, pipeline.clone()).await?;
    }
    Ok(())
}

fn tls_acceptor(cert: &str, key: &str) -> Result<Option<TlsAcceptor>, String> {
    if cert.is_empty() && key.is_empty() {
        return Ok(None);
    }
    let config = rsearch_common::tls::fips_server_config(cert, key)
        .map_err(|e| format!("input TLS config: {e}"))?;
    Ok(Some(TlsAcceptor::from(config)))
}

/// Micro-batcher: collects parsed docs and flushes them to the pipeline
/// by size or age.
fn spawn_batcher(pipeline: IngestPipeline, stream: String) -> mpsc::Sender<Value> {
    let (tx, mut rx) = mpsc::channel::<Value>(10_000);
    tokio::spawn(async move {
        let mut buffer: Vec<Value> = Vec::new();
        let mut ticker = tokio::time::interval(Duration::from_millis(BATCH_AGE_MS));
        loop {
            tokio::select! {
                doc = rx.recv() => match doc {
                    Some(doc) => {
                        buffer.push(doc);
                        if buffer.len() >= BATCH_MAX
                            && let Err(e) = pipeline
                                .ingest_external(&stream, std::mem::take(&mut buffer))
                                .await
                        {
                            warn!(error = %e, stream, "input batch failed");
                        }
                    }
                    None => {
                        if !buffer.is_empty() {
                            let _ = pipeline.ingest_external(&stream, buffer).await;
                        }
                        return;
                    }
                },
                _ = ticker.tick() => {
                    if !buffer.is_empty()
                        && let Err(e) = pipeline
                            .ingest_external(&stream, std::mem::take(&mut buffer))
                            .await
                    {
                        warn!(error = %e, stream, "input batch failed");
                    }
                }
            }
        }
    });
    tx
}

async fn spawn_syslog(
    config: &SyslogInputConfig,
    pipeline: IngestPipeline,
) -> Result<(), String> {
    let batcher = spawn_batcher(pipeline, config.stream.clone());
    let acceptor = tls_acceptor(&config.tls_cert_path, &config.tls_key_path)?;

    if !config.bind_udp.is_empty() {
        let socket = UdpSocket::bind(&config.bind_udp)
            .await
            .map_err(|e| format!("binding syslog UDP {}: {e}", config.bind_udp))?;
        info!(addr = %config.bind_udp, "syslog UDP input listening");
        let tx = batcher.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, _)) => {
                        let line = String::from_utf8_lossy(&buf[..n]);
                        for msg in line.split('\n').filter(|l| !l.trim().is_empty()) {
                            let _ = tx.send(parse_syslog(msg)).await;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "syslog UDP receive failed");
                    }
                }
            }
        });
    }

    if !config.bind_tcp.is_empty() {
        let listener = TcpListener::bind(&config.bind_tcp)
            .await
            .map_err(|e| format!("binding syslog TCP {}: {e}", config.bind_tcp))?;
        info!(addr = %config.bind_tcp, tls = acceptor.is_some(), "syslog TCP input listening");
        let tx = batcher.clone();
        tokio::spawn(async move {
            loop {
                let Ok((socket, peer)) = listener.accept().await else {
                    continue;
                };
                let tx = tx.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let result = match acceptor {
                        Some(acceptor) => match acceptor.accept(socket).await {
                            Ok(tls) => read_lines(tls, &tx).await,
                            Err(e) => {
                                warn!(%peer, error = %e, "syslog TLS handshake failed");
                                return;
                            }
                        },
                        None => read_lines(socket, &tx).await,
                    };
                    if let Err(e) = result {
                        warn!(%peer, error = %e, "syslog connection error");
                    }
                });
            }
        });
    }
    Ok(())
}

/// Read newline-framed syslog messages from a stream.
async fn read_lines(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    tx: &mpsc::Sender<Value>,
) -> std::io::Result<()> {
    let mut pending = Vec::new();
    let mut chunk = vec![0u8; 16 * 1024];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            if !pending.is_empty() {
                let line = String::from_utf8_lossy(&pending);
                if !line.trim().is_empty() {
                    let _ = tx.send(parse_syslog(&line)).await;
                }
            }
            return Ok(());
        }
        pending.extend_from_slice(&chunk[..n]);
        // Frame via a cursor and compact once per chunk — a per-message
        // drain() shifts the whole remaining buffer every message, which
        // goes quadratic on floods of small messages.
        let mut start = 0;
        while let Some(rel) = pending[start..].iter().position(|&b| b == b'\n') {
            let end = start + rel;
            let line = String::from_utf8_lossy(&pending[start..end]);
            if !line.trim().is_empty() {
                let _ = tx.send(parse_syslog(&line)).await;
            }
            start = end + 1;
        }
        if start > 0 {
            pending.drain(..start);
        }
        // Guard against unframed floods.
        if pending.len() > 1 << 20 {
            pending.clear();
        }
    }
}

async fn spawn_gelf(config: &GelfInputConfig, pipeline: IngestPipeline) -> Result<(), String> {
    let batcher = spawn_batcher(pipeline, config.stream.clone());
    let acceptor = tls_acceptor(&config.tls_cert_path, &config.tls_key_path)?;
    let listener = TcpListener::bind(&config.bind_tcp)
        .await
        .map_err(|e| format!("binding GELF TCP {}: {e}", config.bind_tcp))?;
    info!(addr = %config.bind_tcp, tls = acceptor.is_some(), "GELF TCP input listening");
    tokio::spawn(async move {
        loop {
            let Ok((socket, peer)) = listener.accept().await else {
                continue;
            };
            let tx = batcher.clone();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let result = match acceptor {
                    Some(acceptor) => match acceptor.accept(socket).await {
                        Ok(tls) => read_gelf(tls, &tx).await,
                        Err(e) => {
                            warn!(%peer, error = %e, "GELF TLS handshake failed");
                            return;
                        }
                    },
                    None => read_gelf(socket, &tx).await,
                };
                if let Err(e) = result {
                    warn!(%peer, error = %e, "GELF connection error");
                }
            });
        }
    });
    Ok(())
}

/// Read null-byte-framed GELF messages from a stream.
async fn read_gelf(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    tx: &mpsc::Sender<Value>,
) -> std::io::Result<()> {
    let mut pending = Vec::new();
    let mut chunk = vec![0u8; 16 * 1024];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            if !pending.is_empty()
                && let Some(doc) = parse_gelf(&pending)
            {
                let _ = tx.send(doc).await;
            }
            return Ok(());
        }
        pending.extend_from_slice(&chunk[..n]);
        // Cursor + single compaction per chunk (see read_lines).
        let mut start = 0;
        while let Some(rel) = pending[start..].iter().position(|&b| b == 0) {
            let end = start + rel;
            match parse_gelf(&pending[start..end]) {
                Some(doc) => {
                    let _ = tx.send(doc).await;
                }
                None => warn!("dropping invalid GELF frame"),
            }
            start = end + 1;
        }
        if start > 0 {
            pending.drain(..start);
        }
        if pending.len() > 1 << 20 {
            pending.clear();
        }
    }
}

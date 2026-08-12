//! Node-to-node object transfer client for the replicated backend.
//! Talks to the peer endpoints under `/_rsearch/internal/`, authenticated
//! by the shared cluster token. TLS uses the FIPS provider with webpki
//! roots, so cluster certificates must chain to a trusted root.

use std::ops::Range;
use std::path::Path;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::error::{StorageError, StorageResult};
use crate::fs::FsStorage;

/// Header carrying the shared cluster token on internal requests.
pub const INTERNAL_TOKEN_HEADER: &str = "x-rsearch-internal-token";

/// TCP connect budget for any peer request.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Budget for small operations: ranged reads, deletes, replicate control.
const SHORT_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Budget for whole-object transfers (pushes, downloads, replicate pulls).
const TRANSFER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

type Body = BoxBody<Bytes, std::io::Error>;

fn empty_body() -> Body {
    Full::new(Bytes::new()).map_err(io_never).boxed()
}

fn full_body(data: Bytes) -> Body {
    Full::new(data).map_err(io_never).boxed()
}

fn io_never(never: std::convert::Infallible) -> std::io::Error {
    match never {}
}

/// HTTP client for peer object transfer, cloneable and connection-pooled.
#[derive(Clone)]
pub struct PeerClient {
    client: Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Body,
    >,
    token: String,
}

impl PeerClient {
    /// `ca_file`: optional PEM bundle for a private cluster CA; without it
    /// peer TLS validates against the webpki (public) roots.
    pub fn new(token: &str, ca_file: Option<&str>) -> StorageResult<Self> {
        let backend_err = |message: String| StorageError::Backend {
            key: String::new(),
            message,
        };
        // A wedged peer must never hang the data path (ingest publishes,
        // search fallback reads): bound the connect here, and every
        // request via the per-operation timeouts in send().
        let mut http = hyper_util::client::legacy::connect::HttpConnector::new();
        http.set_connect_timeout(Some(CONNECT_TIMEOUT));
        http.enforce_http(false);
        let provider = std::sync::Arc::new(rustls::crypto::default_fips_provider());
        let https = if let Some(path) = ca_file {
            use rustls::pki_types::pem::PemObject;
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls::pki_types::CertificateDer::pem_file_iter(path)
                .map_err(|e| backend_err(format!("reading peer CA {path}: {e}")))?
            {
                let cert =
                    cert.map_err(|e| backend_err(format!("parsing peer CA {path}: {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| backend_err(format!("loading peer CA {path}: {e}")))?;
            }
            if roots.is_empty() {
                return Err(backend_err(format!("no certificates in peer CA {path}")));
            }
            let tls = rustls::ClientConfig::builder_with_provider(provider)
                .with_protocol_versions(rustls::DEFAULT_VERSIONS)
                .map_err(|e| backend_err(format!("selecting peer TLS versions: {e}")))?
                .with_root_certificates(roots)
                .with_no_client_auth();
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config(tls)
                .https_or_http()
                .enable_http1()
                .wrap_connector(http)
        } else {
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_provider_and_webpki_roots(provider.as_ref().clone())
                .map_err(|e| backend_err(format!("building peer TLS connector: {e}")))?
                .https_or_http()
                .enable_http1()
                .wrap_connector(http)
        };
        Ok(Self {
            client: Client::builder(TokioExecutor::new()).build(https),
            token: token.to_string(),
        })
    }

    /// `{base}/_rsearch/internal/objects/{key}` with each key segment
    /// percent-encoded (keys contain slashes that must stay separators).
    fn object_uri(base: &str, key: &str) -> StorageResult<hyper::Uri> {
        let mut url = url::Url::parse(base).map_err(|e| StorageError::Backend {
            key: key.to_string(),
            message: format!("invalid peer address '{base}': {e}"),
        })?;
        url.path_segments_mut()
            .map_err(|_| StorageError::Backend {
                key: key.to_string(),
                message: format!("peer address '{base}' cannot carry a path"),
            })?
            .extend(["_rsearch", "internal", "objects"])
            .extend(key.split('/'));
        url.as_str().parse().map_err(|e| StorageError::Backend {
            key: key.to_string(),
            message: format!("building peer uri: {e}"),
        })
    }

    fn request(
        &self,
        method: &str,
        uri: hyper::Uri,
        body: Body,
    ) -> StorageResult<hyper::Request<Body>> {
        hyper::Request::builder()
            .method(method)
            .uri(uri)
            .header(INTERNAL_TOKEN_HEADER, &self.token)
            .body(body)
            .map_err(|e| StorageError::Backend {
                key: String::new(),
                message: format!("building peer request: {e}"),
            })
    }

    /// Bound an entire peer operation (request + body). A wedged peer
    /// surfaces as an error instead of hanging the data path.
    async fn bounded<T>(
        key: &str,
        timeout: std::time::Duration,
        op: impl Future<Output = StorageResult<T>>,
    ) -> StorageResult<T> {
        tokio::time::timeout(timeout, op)
            .await
            .map_err(|_| StorageError::Backend {
                key: key.to_string(),
                message: format!("peer operation timed out after {}s", timeout.as_secs()),
            })?
    }

    async fn send(
        &self,
        key: &str,
        request: hyper::Request<Body>,
    ) -> StorageResult<hyper::Response<hyper::body::Incoming>> {
        let response = self
            .client
            .request(request)
            .await
            .map_err(|e| StorageError::Backend {
                key: key.to_string(),
                message: format!("peer request failed: {e}"),
            })?;
        match response.status().as_u16() {
            200 | 206 => Ok(response),
            404 => Err(StorageError::NotFound(key.to_string())),
            status => Err(StorageError::Backend {
                key: key.to_string(),
                message: format!("peer returned status {status}"),
            }),
        }
    }

    /// Fetch a byte range (end exclusive) of an object from a peer.
    pub async fn get_range(
        &self,
        base: &str,
        key: &str,
        range: Range<u64>,
    ) -> StorageResult<Bytes> {
        let uri = Self::object_uri(base, key)?;
        let mut request = self.request("GET", uri, empty_body())?;
        // HTTP ranges are inclusive; ours are end-exclusive.
        let end = range.end.saturating_sub(1);
        request.headers_mut().insert(
            hyper::header::RANGE,
            format!("bytes={}-{end}", range.start)
                .parse()
                .map_err(|e| StorageError::Backend {
                    key: key.to_string(),
                    message: format!("building range header: {e}"),
                })?,
        );
        Self::bounded(key, SHORT_OP_TIMEOUT, async {
            let response = self.send(key, request).await?;
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|e| StorageError::Backend {
                    key: key.to_string(),
                    message: format!("reading peer response: {e}"),
                })?;
            Ok(body.to_bytes())
        })
        .await
    }

    /// Fetch an entire object from a peer into memory.
    pub async fn get(&self, base: &str, key: &str) -> StorageResult<Bytes> {
        let uri = Self::object_uri(base, key)?;
        let request = self.request("GET", uri, empty_body())?;
        Self::bounded(key, TRANSFER_TIMEOUT, async {
            let response = self.send(key, request).await?;
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|e| StorageError::Backend {
                    key: key.to_string(),
                    message: format!("reading peer response: {e}"),
                })?;
            Ok(body.to_bytes())
        })
        .await
    }

    /// Stream an entire object from a peer into the local root (atomic
    /// tmp+rename via `FsStorage`). Returns the byte count.
    pub async fn download_to(
        &self,
        base: &str,
        key: &str,
        local: &FsStorage,
    ) -> StorageResult<u64> {
        let uri = Self::object_uri(base, key)?;
        let request = self.request("GET", uri, empty_body())?;
        Self::bounded(key, TRANSFER_TIMEOUT, async {
            let response = self.send(key, request).await?;
            let stream = response.into_body().into_data_stream();
            local.put_stream(key, stream).await
        })
        .await
    }

    /// Push a local file to a peer as a streamed PUT. The receiver writes
    /// atomically and records its own placement row.
    pub async fn push_file(&self, base: &str, key: &str, local: &Path) -> StorageResult<()> {
        use futures::TryStreamExt;
        let file = tokio::fs::File::open(local).await.map_err(|e| StorageError::Io {
            key: key.to_string(),
            source: e,
        })?;
        let stream = tokio_util::io::ReaderStream::new(file).map_ok(Frame::data);
        let body = BodyExt::boxed(StreamBody::new(stream));
        let uri = Self::object_uri(base, key)?;
        let request = self.request("PUT", uri, body)?;
        Self::bounded(key, TRANSFER_TIMEOUT, async {
            self.send(key, request).await?;
            Ok(())
        })
        .await
    }

    /// Push an in-memory object to a peer.
    pub async fn push_bytes(&self, base: &str, key: &str, data: Bytes) -> StorageResult<()> {
        let uri = Self::object_uri(base, key)?;
        let request = self.request("PUT", uri, full_body(data))?;
        Self::bounded(key, TRANSFER_TIMEOUT, async {
            self.send(key, request).await?;
            Ok(())
        })
        .await
    }

    /// Delete an object copy on a peer (missing objects are not an error).
    pub async fn delete(&self, base: &str, key: &str) -> StorageResult<()> {
        let uri = Self::object_uri(base, key)?;
        let request = self.request("DELETE", uri, empty_body())?;
        match Self::bounded(key, SHORT_OP_TIMEOUT, self.send(key, request)).await {
            Ok(_) | Err(StorageError::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Instruct a peer to pull `key` from `source_addr` (repair/drain).
    pub async fn replicate(&self, base: &str, key: &str, source_addr: &str) -> StorageResult<()> {
        let mut url = url::Url::parse(base).map_err(|e| StorageError::Backend {
            key: key.to_string(),
            message: format!("invalid peer address '{base}': {e}"),
        })?;
        url.path_segments_mut()
            .map_err(|_| StorageError::Backend {
                key: key.to_string(),
                message: format!("peer address '{base}' cannot carry a path"),
            })?
            .extend(["_rsearch", "internal", "replicate"]);
        let uri: hyper::Uri = url.as_str().parse().map_err(|e| StorageError::Backend {
            key: key.to_string(),
            message: format!("building peer uri: {e}"),
        })?;
        let payload = serde_json::json!({ "key": key, "source_addr": source_addr });
        let mut request = self.request("POST", uri, full_body(Bytes::from(payload.to_string())))?;
        request.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static("application/json"),
        );
        Self::bounded(key, TRANSFER_TIMEOUT, async {
            self.send(key, request).await?;
            Ok(())
        })
        .await
    }

    /// POST a raw body to an already-built internal URL on a peer and
    /// return `(status, response bytes)` verbatim — unlike [`send`], no
    /// status is treated as an error, so a proxying caller (bulk handoff)
    /// can relay the peer's real response to its own client.
    pub async fn post_raw(&self, url: &str, body: Bytes) -> StorageResult<(u16, Bytes)> {
        let uri: hyper::Uri = url.parse().map_err(|e| StorageError::Backend {
            key: String::new(),
            message: format!("building peer uri '{url}': {e}"),
        })?;
        let request = self.request("POST", uri, full_body(body))?;
        Self::bounded("", TRANSFER_TIMEOUT, async {
            let response =
                self.client
                    .request(request)
                    .await
                    .map_err(|e| StorageError::Backend {
                        key: String::new(),
                        message: format!("peer request failed: {e}"),
                    })?;
            let status = response.status().as_u16();
            let bytes = response
                .into_body()
                .collect()
                .await
                .map_err(|e| StorageError::Backend {
                    key: String::new(),
                    message: format!("reading peer response: {e}"),
                })?
                .to_bytes();
            Ok((status, bytes))
        })
        .await
    }
}

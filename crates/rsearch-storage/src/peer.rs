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
    pub fn new(token: &str) -> StorageResult<Self> {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_provider_and_webpki_roots(rustls::crypto::default_fips_provider())
            .map_err(|e| StorageError::Backend {
                key: String::new(),
                message: format!("building peer TLS connector: {e}"),
            })?
            .https_or_http()
            .enable_http1()
            .build();
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
    }

    /// Fetch an entire object from a peer into memory.
    pub async fn get(&self, base: &str, key: &str) -> StorageResult<Bytes> {
        let uri = Self::object_uri(base, key)?;
        let request = self.request("GET", uri, empty_body())?;
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
        let response = self.send(key, request).await?;
        let stream = response.into_body().into_data_stream();
        local.put_stream(key, stream).await
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
        self.send(key, request).await?;
        Ok(())
    }

    /// Push an in-memory object to a peer.
    pub async fn push_bytes(&self, base: &str, key: &str, data: Bytes) -> StorageResult<()> {
        let uri = Self::object_uri(base, key)?;
        let request = self.request("PUT", uri, full_body(data))?;
        self.send(key, request).await?;
        Ok(())
    }

    /// Delete an object copy on a peer (missing objects are not an error).
    pub async fn delete(&self, base: &str, key: &str) -> StorageResult<()> {
        let uri = Self::object_uri(base, key)?;
        let request = self.request("DELETE", uri, empty_body())?;
        match self.send(key, request).await {
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
        self.send(key, request).await?;
        Ok(())
    }
}

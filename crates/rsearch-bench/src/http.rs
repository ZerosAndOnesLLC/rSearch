use anyhow::Context;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;

/// Minimal HTTP/1.1 client over hyper-util (no TLS — localhost bench).
pub struct HttpClient {
    client: Client<HttpConnector, Full<Bytes>>,
}

impl HttpClient {
    pub fn new() -> Self {
        Self {
            client: Client::builder(TokioExecutor::new()).build_http(),
        }
    }

    pub async fn post(
        &self,
        url: &str,
        body: String,
        content_type: &'static str,
    ) -> anyhow::Result<(u16, String)> {
        let request = hyper::Request::builder()
            .method("POST")
            .uri(url)
            .header("content-type", content_type)
            .body(Full::new(Bytes::from(body)))
            .context("building request")?;
        let response = self.client.request(request).await.context("sending")?;
        let status = response.status().as_u16();
        let bytes = response
            .into_body()
            .collect()
            .await
            .context("reading body")?
            .to_bytes();
        Ok((status, String::from_utf8_lossy(&bytes).to_string()))
    }
}

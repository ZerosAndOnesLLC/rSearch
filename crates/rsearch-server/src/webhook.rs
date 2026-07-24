use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

/// Outbound HTTPS/HTTP client for alert webhooks, TLS on the FIPS
/// provider with webpki roots.
pub struct WebhookClient {
    client: Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Full<Bytes>,
    >,
}

impl WebhookClient {
    pub fn new() -> anyhow::Result<Self> {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_provider_and_webpki_roots(rustls::crypto::default_fips_provider())?
            .https_or_http()
            .enable_http1()
            .build();
        Ok(Self {
            client: Client::builder(TokioExecutor::new()).build(https),
        })
    }

    /// POST a JSON payload; returns the response status code.
    pub async fn post_json(
        &self,
        url: &str,
        payload: &serde_json::Value,
    ) -> anyhow::Result<u16> {
        let request = hyper::Request::builder()
            .method("POST")
            .uri(url)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload.to_string())))?;
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.client.request(request),
        )
        .await
        .map_err(|_| anyhow::anyhow!("webhook timed out"))??;
        let status = response.status().as_u16();
        // Drain the body so the connection can be reused.
        let _ = response.into_body().collect().await;
        Ok(status)
    }
}

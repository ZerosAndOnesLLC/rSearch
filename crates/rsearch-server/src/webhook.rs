use std::net::IpAddr;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

/// Validate an alert webhook URL against SSRF. Requires https (unless
/// `allow_insecure` is explicitly configured for trusted networks) and
/// rejects hosts that resolve to loopback, link-local, or private ranges
/// — so a webhook can't be pointed at the cloud metadata service or an
/// internal admin port. A bare hostname that later resolves to a private
/// address is still blocked at send time by `guard_addr`.
pub fn validate_webhook_url(url: &str, allow_insecure: bool) -> Result<(), String> {
    let parsed = url::Url::parse(url).map_err(|e| format!("invalid webhook_url: {e}"))?;
    match parsed.scheme() {
        "https" => {}
        "http" if allow_insecure => {}
        "http" => {
            return Err(
                "webhook_url must use https (set control.allow_insecure_webhooks to permit http \
                 and internal addresses)"
                    .to_string(),
            );
        }
        other => return Err(format!("unsupported webhook scheme '{other}'")),
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| "webhook_url has no host".to_string())?;
    // Reject non-global literal IPs unless the operator has explicitly
    // opted into trusted-network mode (which also permits internal
    // webhook targets like a private monitoring endpoint).
    if !allow_insecure
        && let Ok(ip) = host.parse::<IpAddr>()
    {
        guard_addr(&ip)?;
    }
    Ok(())
}

/// Reject non-globally-routable IPs (loopback, link-local, private,
/// unspecified, multicast).
fn guard_addr(ip: &IpAddr) -> Result<(), String> {
    let blocked = match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_multicast()
                || v4.is_broadcast()
                // carrier-grade NAT / metadata-adjacent
                || v4.octets()[0] == 169 && v4.octets()[1] == 254
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique-local
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local
        }
    };
    if blocked {
        return Err(format!("webhook host {ip} is not a routable public address"));
    }
    Ok(())
}

/// Outbound HTTPS/HTTP client for alert webhooks, TLS on the FIPS
/// provider with webpki roots.
pub struct WebhookClient {
    client: Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Full<Bytes>,
    >,
    /// Trusted-network mode: permit http and internal/private addresses.
    allow_insecure: bool,
}

impl WebhookClient {
    pub fn new(allow_insecure: bool) -> anyhow::Result<Self> {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_provider_and_webpki_roots(rustls::crypto::default_fips_provider())?
            .https_or_http()
            .enable_http1()
            .build();
        Ok(Self {
            client: Client::builder(TokioExecutor::new()).build(https),
            allow_insecure,
        })
    }

    /// POST a JSON payload; returns the response status code.
    pub async fn post_json(
        &self,
        url: &str,
        payload: &serde_json::Value,
    ) -> anyhow::Result<u16> {
        // Re-resolve the host at send time and guard every resolved
        // address, closing the DNS-rebinding gap that a create-time check
        // alone would leave open. Skipped in trusted-network mode.
        if !self.allow_insecure {
            let parsed = url::Url::parse(url)?;
            if let Some(host) = parsed.host_str() {
                let port = parsed.port_or_known_default().unwrap_or(443);
                for addr in tokio::net::lookup_host((host, port)).await? {
                    guard_addr(&addr.ip()).map_err(|e| anyhow::anyhow!(e))?;
                }
            }
        }
        let request = hyper::Request::builder()
            .method("POST")
            .uri(url)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload.to_string())))?;
        // The timeout covers the whole exchange including the body drain:
        // a response that returns headers and then drips its body forever
        // must not hang the control loop (every leader job runs on it).
        let status = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let response = self.client.request(request).await?;
            let status = response.status().as_u16();
            // Drain a bounded amount so the connection can be reused; a
            // larger body is abandoned — dropping it closes the connection
            // instead of buffering an arbitrary payload in memory.
            const DRAIN_CAP: usize = 64 * 1024;
            let mut body = response.into_body();
            let mut drained = 0usize;
            while let Some(frame) = body.frame().await {
                let Ok(frame) = frame else { break };
                if let Some(data) = frame.data_ref() {
                    drained += data.len();
                    if drained > DRAIN_CAP {
                        break;
                    }
                }
            }
            Ok::<u16, anyhow::Error>(status)
        })
        .await
        .map_err(|_| anyhow::anyhow!("webhook timed out"))??;
        Ok(status)
    }
}

//! HTTP client for Arcus public market data and signed trading.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use nautilus_network::http::{HttpClient, HttpClientError, Method, create_standard_nautilus_headers};

use crate::common::enums::ArcusEnvironment;
use crate::common::urls::rest_base;
use crate::http::models::{
    BboRest, CancelOrderRequest, MarketInfo, MarketsResponse, ModifyOrderRequest, OrderAck,
    PlaceOrderRequest,
};
use crate::signing::ArcusCredential;

/// REST client (public reads; signed writes when credential is set).
#[derive(Debug, Clone)]
pub struct ArcusHttpClient {
    client: HttpClient,
    base_url: String,
    environment: ArcusEnvironment,
}

impl ArcusHttpClient {
    pub fn new(environment: ArcusEnvironment) -> std::result::Result<Self, HttpClientError> {
        Self::with_base(environment, rest_base(environment))
    }

    pub fn with_base(
        environment: ArcusEnvironment,
        base_url: &str,
    ) -> std::result::Result<Self, HttpClientError> {
        let headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        Ok(Self {
            client: HttpClient::builder()
                .headers(headers)
                .timeout_secs(30)
                .build()?,
            base_url: base_url.trim_end_matches('/').to_string(),
            environment,
        })
    }

    #[must_use]
    pub fn environment(&self) -> ArcusEnvironment {
        self.environment
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{path}", self.base_url);
        let resp = self
            .client
            .request(Method::GET, url.clone(), None, None, None, None, None)
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status.as_u16();
        let body = String::from_utf8_lossy(&resp.body);
        if !(200..300).contains(&status) {
            bail!(
                "GET {url} status={status} body={}",
                body.chars().take(240).collect::<String>()
            );
        }
        serde_json::from_slice(&resp.body).with_context(|| format!("decode GET {url}"))
    }

    async fn post_signed_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
        credential: &ArcusCredential,
        signature: &str,
        timestamp_ns: u64,
    ) -> Result<(u16, T)> {
        let url = format!("{}{path}", self.base_url);
        let payload = serde_json::to_vec(body).context("serialize body")?;
        let mut headers = HashMap::new();
        headers.insert("Content-Type".into(), "application/json".into());
        headers.insert("X-API-Key".into(), credential.api_key_hex.clone());
        headers.insert("X-Timestamp".into(), timestamp_ns.to_string());
        headers.insert("X-Signature".into(), signature.to_string());

        let resp = self
            .client
            .request(
                Method::POST,
                url.clone(),
                None,
                Some(headers),
                Some(payload),
                None,
                None,
            )
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status.as_u16();
        let text = String::from_utf8_lossy(&resp.body);
        if !(200..300).contains(&status) {
            bail!(
                "POST {url} status={status} body={}",
                text.chars().take(300).collect::<String>()
            );
        }
        let parsed: T = serde_json::from_slice(&resp.body)
            .with_context(|| format!("decode POST {url} body={text}"))?;
        Ok((status, parsed))
    }

    /// `GET /v1/markets` — all markets, or filter by display name.
    pub async fn list_markets(&self, market: Option<&str>) -> Result<Vec<MarketInfo>> {
        let path = match market {
            Some(m) => format!("/v1/markets?market={m}"),
            None => "/v1/markets".to_string(),
        };
        // Response may be `{ markets: [...] }` or a bare array depending on gateway.
        let url = format!("{}{path}", self.base_url);
        let resp = self
            .client
            .request(Method::GET, url.clone(), None, None, None, None, None)
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status.as_u16();
        if !(200..300).contains(&status) {
            let body = String::from_utf8_lossy(&resp.body);
            bail!(
                "GET {url} status={status} body={}",
                body.chars().take(240).collect::<String>()
            );
        }
        if let Ok(wrapped) = serde_json::from_slice::<MarketsResponse>(&resp.body) {
            return Ok(wrapped.markets);
        }
        serde_json::from_slice(&resp.body).context("decode markets")
    }

    /// Online markets only.
    pub async fn list_online_markets(&self) -> Result<Vec<MarketInfo>> {
        let mut markets = self.list_markets(None).await?;
        markets.retain(|m| m.status.eq_ignore_ascii_case("ONLINE"));
        markets.sort_by(|a, b| a.market_display_name.cmp(&b.market_display_name));
        Ok(markets)
    }

    /// `GET /v1/bbo?market=ZEC-USD`
    pub async fn bbo(&self, market: &str) -> Result<BboRest> {
        let path = format!("/v1/bbo?market={market}");
        self.get_json(&path).await
    }

    /// Signed `POST /v1/placeOrder`.
    pub async fn place_order(
        &self,
        req: &PlaceOrderRequest,
        credential: &ArcusCredential,
        canonical: &str,
        timestamp_ns: u64,
    ) -> Result<(u16, OrderAck)> {
        let signature = credential.sign_str(canonical);
        self.post_signed_json("/v1/placeOrder", req, credential, &signature, timestamp_ns)
            .await
    }

    /// Signed `POST /v1/modifyOrder`.
    pub async fn modify_order(
        &self,
        req: &ModifyOrderRequest,
        credential: &ArcusCredential,
        canonical: &str,
        timestamp_ns: u64,
    ) -> Result<(u16, OrderAck)> {
        let signature = credential.sign_str(canonical);
        self.post_signed_json("/v1/modifyOrder", req, credential, &signature, timestamp_ns)
            .await
    }

    /// Signed `POST /v1/cancelOrder`.
    pub async fn cancel_order(
        &self,
        req: &CancelOrderRequest,
        credential: &ArcusCredential,
        canonical: &str,
        timestamp_ns: u64,
    ) -> Result<(u16, OrderAck)> {
        let signature = credential.sign_str(canonical);
        self.post_signed_json("/v1/cancelOrder", req, credential, &signature, timestamp_ns)
            .await
    }
}

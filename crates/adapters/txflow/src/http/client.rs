//! HTTP client for TxFlow public `/info` reads.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use nautilus_network::http::{HttpClient, HttpClientError, Method, create_standard_nautilus_headers};
use serde_json::json;

use crate::common::urls::{TXFLOW_API_VERSION, TXFLOW_INFO_URL};
use crate::http::models::{InfoEnvelope, PerpMarket, PerpMeta, WebData2};

/// Public REST client (no API key for market data).
#[derive(Debug, Clone)]
pub struct TxFlowHttpClient {
    client: HttpClient,
    info_url: String,
}

impl TxFlowHttpClient {
    pub fn new() -> std::result::Result<Self, HttpClientError> {
        Self::with_info_url(TXFLOW_INFO_URL)
    }

    pub fn with_info_url(info_url: &str) -> std::result::Result<Self, HttpClientError> {
        let mut headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.insert("x-api-version".into(), TXFLOW_API_VERSION.into());
        headers.insert("content-type".into(), "application/json".into());
        Ok(Self {
            client: HttpClient::builder()
                .headers(headers)
                .timeout_secs(60)
                .build()?,
            info_url: info_url.to_string(),
        })
    }

    async fn post_info<T: serde::de::DeserializeOwned>(&self, body: serde_json::Value) -> Result<T> {
        let bytes = serde_json::to_vec(&body).context("encode TxFlow /info body")?;
        let resp = self
            .client
            .request(
                Method::POST,
                self.info_url.clone(),
                None,
                None,
                Some(bytes),
                None,
                None,
            )
            .await
            .with_context(|| format!("POST {}", self.info_url))?;
        let status = resp.status.as_u16();
        let body_txt = String::from_utf8_lossy(&resp.body);
        if !(200..300).contains(&status) {
            bail!(
                "POST {} status={status} body={}",
                self.info_url,
                body_txt.chars().take(200).collect::<String>()
            );
        }
        let env: InfoEnvelope<T> =
            serde_json::from_slice(&resp.body).context("decode TxFlow /info envelope")?;
        if env.success == Some(false) {
            bail!(
                "TxFlow /info error code={:?} message={}",
                env.code,
                env.message.unwrap_or_default()
            );
        }
        env.data.context("TxFlow /info missing data")
    }

    /// Active USDC perps from `perpMeta`.
    pub async fn list_usdc_perpetuals(&self) -> Result<Vec<PerpMarket>> {
        let meta: PerpMeta = self
            .post_info(json!({ "type": "perpMeta", "includeDelisted": false }))
            .await?;
        let mut out = Vec::new();
        for inst in meta.universe {
            if inst.delisted || inst.halt_trading {
                continue;
            }
            if !inst.quote_currency.eq_ignore_ascii_case("USDC") {
                continue;
            }
            out.push(PerpMarket {
                coin_id: inst.index.to_string(),
                name: inst.name,
                base_asset: inst.base_currency.to_uppercase(),
            });
        }
        out.sort_by(|a, b| a.base_asset.cmp(&b.base_asset));
        Ok(out)
    }

    /// 24h notional volume (USDC) keyed by numeric instrument id (`perpMeta.index` as string).
    ///
    /// Uses `webData2.assetCtxs[].dayNtlVlm`. Spot rows (`coin` containing `/`) are skipped.
    pub async fn day_ntl_vlm_by_instrument_id(&self) -> Result<HashMap<String, f64>> {
        let data: WebData2 = self.post_info(json!({ "type": "webData2" })).await?;
        let mut out = HashMap::new();
        for ctx in data.asset_ctxs {
            if ctx.coin.contains('/') {
                continue; // spot-style ids like BTC/USDC
            }
            let v = ctx.day_ntl_vlm.parse::<f64>().unwrap_or(0.0);
            out.insert(ctx.instrument_id.to_string(), v);
        }
        Ok(out)
    }

    /// Active USDC perps ranked by 24h notional volume (desc), optionally truncated to `limit`.
    ///
    /// Markets missing from `webData2` sort last (volume 0). Stable tie-break by base name.
    pub async fn list_usdc_perpetuals_by_volume(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<PerpMarket>> {
        let mut markets = self.list_usdc_perpetuals().await?;
        let volumes = match self.day_ntl_vlm_by_instrument_id().await {
            Ok(v) => v,
            Err(e) => {
                log::warn!("TxFlow webData2 volume fetch failed ({e:#}); ranking by name only");
                HashMap::new()
            }
        };
        markets.sort_by(|a, b| {
            let va = volumes.get(&a.coin_id).copied().unwrap_or(0.0);
            let vb = volumes.get(&b.coin_id).copied().unwrap_or(0.0);
            vb.partial_cmp(&va)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.base_asset.cmp(&b.base_asset))
        });
        if let Some(n) = limit {
            if markets.len() > n {
                markets.truncate(n);
            }
        }
        Ok(markets)
    }
}

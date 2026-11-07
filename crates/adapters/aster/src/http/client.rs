//! HTTP client for AsterDEX public fapi endpoints.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use nautilus_network::http::{HttpClient, HttpClientError, Method, create_standard_nautilus_headers};

use crate::common::urls::ASTER_FAPI_REST;
use crate::http::models::{
    BookTickerRest, ExchangeInfo, ExchangeSymbol, PerpMarket, PremiumIndex,
};

/// Public REST client (no API key required for market data).
#[derive(Debug, Clone)]
pub struct AsterHttpClient {
    client: HttpClient,
    base_url: String,
}

impl AsterHttpClient {
    /// Builds a client against the production fapi base URL.
    pub fn new() -> std::result::Result<Self, HttpClientError> {
        Self::with_base(ASTER_FAPI_REST)
    }

    /// Builds a client against a custom base URL (tests / mirrors).
    pub fn with_base(base_url: &str) -> std::result::Result<Self, HttpClientError> {
        let headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        Ok(Self {
            client: HttpClient::builder()
                .headers(headers)
                .timeout_secs(30)
                .build()?,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
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
            bail!("GET {url} status={status} body={}", body.chars().take(200).collect::<String>());
        }
        serde_json::from_slice(&resp.body).with_context(|| format!("decode GET {url}"))
    }

    /// `GET /fapi/v1/exchangeInfo` — full symbol catalog.
    pub async fn exchange_info(&self) -> Result<ExchangeInfo> {
        self.get_json("/fapi/v1/exchangeInfo").await
    }

    /// Active `TRADING` + `PERPETUAL` + quote `USDT` markets.
    pub async fn list_usdt_perpetuals(&self) -> Result<Vec<PerpMarket>> {
        let info = self.exchange_info().await?;
        let mut out = Vec::new();
        for s in info.symbols {
            if keep_usdt_perp(&s) {
                out.push(PerpMarket {
                    symbol: s.symbol,
                    base_asset: s.base_asset,
                });
            }
        }
        out.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        Ok(out)
    }

    /// Best bid/ask for one symbol, or all when `symbol` is `None`.
    pub async fn book_ticker(&self, symbol: Option<&str>) -> Result<Vec<BookTickerRest>> {
        let path = match symbol {
            Some(s) => format!("/fapi/v1/ticker/bookTicker?symbol={s}"),
            None => "/fapi/v1/ticker/bookTicker".to_string(),
        };
        // Endpoint returns object for one symbol, array for all.
        let url = format!("{}{path}", self.base_url);
        let resp = self
            .client
            .request(Method::GET, url.clone(), None, None, None, None, None)
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status.as_u16();
        if !(200..300).contains(&status) {
            let body = String::from_utf8_lossy(&resp.body);
            bail!("GET {url} status={status} body={}", body.chars().take(200).collect::<String>());
        }
        if symbol.is_some() {
            let one: BookTickerRest = serde_json::from_slice(&resp.body).context("decode bookTicker")?;
            Ok(vec![one])
        } else {
            serde_json::from_slice(&resp.body).context("decode bookTicker[]")
        }
    }

    /// `GET /fapi/v1/premiumIndex` — all symbols funding / mark.
    pub async fn premium_index_all(&self) -> Result<Vec<PremiumIndex>> {
        self.get_json("/fapi/v1/premiumIndex").await
    }
}

fn keep_usdt_perp(s: &ExchangeSymbol) -> bool {
    s.status == "TRADING" && s.quote_asset == "USDT" && s.contract_type == "PERPETUAL"
}

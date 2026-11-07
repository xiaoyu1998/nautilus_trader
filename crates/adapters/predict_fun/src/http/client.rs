// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! HTTP client for Predict.fun public and authenticated REST endpoints.

use std::collections::HashMap;

use nautilus_network::http::{HttpClient, HttpClientError, Method, create_standard_nautilus_headers};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::config::PredictFunHttpConfig;
use crate::http::error::{Error, Result, decode_response};
use crate::http::models::{
    ApiResponse, AuthMessageData, Category, Market, MarketStats, Orderbook, SearchData,
};
use crate::http::query::{GetCategoriesParams, GetMarketsParams, GetSearchParams};

/// HTTP client for the Predict.fun REST API.
#[derive(Debug, Clone)]
pub struct PredictFunHttpClient {
    client: HttpClient,
    base_url: String,
    api_key: Option<String>,
}

impl PredictFunHttpClient {
    /// Creates a client from [`PredictFunHttpConfig`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn new(config: PredictFunHttpConfig) -> std::result::Result<Self, HttpClientError> {
        let mut headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("Accept".to_string(), "application/json".to_string());
        if let Some(api_key) = config.api_key.as_ref() {
            headers.insert("x-api-key".to_string(), api_key.clone());
        }

        Ok(Self {
            client: HttpClient::builder()
                .headers(headers)
                .timeout_secs(config.timeout_secs)
                .build()?,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            api_key: config.api_key,
        })
    }

    /// Creates a testnet client (no API key) with defaults.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn testnet() -> std::result::Result<Self, HttpClientError> {
        Self::new(PredictFunHttpConfig::testnet())
    }

    /// Creates a client from environment variables (defaults to testnet).
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn from_env() -> std::result::Result<Self, HttpClientError> {
        Self::new(PredictFunHttpConfig::from_env())
    }

    /// Returns the configured REST base URL.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns whether an API key is configured.
    #[must_use]
    pub fn has_api_key(&self) -> bool {
        self.api_key.as_ref().is_some_and(|k| !k.is_empty())
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn send_get<P: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        params: Option<&P>,
    ) -> Result<T> {
        let url = self.url(path);
        let response = self
            .client
            .request_with_params(Method::GET, url, params, None, None, None, None)
            .await
            .map_err(Error::from_http_client)?;
        decode_response(&response)
    }

    fn unwrap_data<T>(envelope: ApiResponse<T>) -> Result<T> {
        if !envelope.success {
            let msg = envelope
                .message
                .or(envelope.error)
                .unwrap_or_else(|| "unknown API error".to_string());
            return Err(Error::Api(msg));
        }
        envelope
            .data
            .ok_or_else(|| Error::Decode("missing data field in success response".into()))
    }

    /// `GET /v1/categories` page — returns categories plus the next cursor.
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn get_categories_page(
        &self,
        params: GetCategoriesParams,
    ) -> Result<(Vec<Category>, Option<String>)> {
        let envelope: ApiResponse<Vec<Category>> =
            self.send_get("/v1/categories", Some(&params)).await?;
        if !envelope.success {
            let msg = envelope
                .message
                .or(envelope.error)
                .unwrap_or_else(|| "unknown API error".to_string());
            return Err(Error::Api(msg));
        }
        Ok((envelope.data.unwrap_or_default(), envelope.cursor))
    }

    /// `GET /v1/categories` — list categories (events) with nested markets.
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn get_categories(&self, params: GetCategoriesParams) -> Result<Vec<Category>> {
        Ok(self.get_categories_page(params).await?.0)
    }

    /// `GET /v1/markets` page — returns markets plus the next cursor.
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn get_markets_page(
        &self,
        params: GetMarketsParams,
    ) -> Result<(Vec<Market>, Option<String>)> {
        let envelope: ApiResponse<Vec<Market>> =
            self.send_get("/v1/markets", Some(&params)).await?;
        if !envelope.success {
            let msg = envelope
                .message
                .or(envelope.error)
                .unwrap_or_else(|| "unknown API error".to_string());
            return Err(Error::Api(msg));
        }
        Ok((envelope.data.unwrap_or_default(), envelope.cursor))
    }

    /// `GET /v1/markets` — list markets (first page only).
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn get_markets(&self, params: GetMarketsParams) -> Result<Vec<Market>> {
        Ok(self.get_markets_page(params).await?.0)
    }

    /// `GET /v1/markets/{id}` — market detail.
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn get_market(&self, market_id: i64) -> Result<Market> {
        let path = format!("/v1/markets/{market_id}");
        let envelope: ApiResponse<Market> = self.send_get::<(), _>(&path, None).await?;
        Self::unwrap_data(envelope)
    }

    /// `GET /v1/markets/{id}/stats` — volume / liquidity (incl. `volume24hUsd`).
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn get_market_stats(&self, market_id: i64) -> Result<MarketStats> {
        let path = format!("/v1/markets/{market_id}/stats");
        let envelope: ApiResponse<MarketStats> = self.send_get::<(), _>(&path, None).await?;
        Self::unwrap_data(envelope)
    }

    /// `GET /v1/markets/{id}/orderbook` — YES-priced book snapshot.
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn get_orderbook(&self, market_id: i64) -> Result<Orderbook> {
        let path = format!("/v1/markets/{market_id}/orderbook");
        let envelope: ApiResponse<Orderbook> = self.send_get::<(), _>(&path, None).await?;
        Self::unwrap_data(envelope)
    }

    /// `GET /v1/search` — full-text search across categories and markets.
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn search(&self, params: GetSearchParams) -> Result<SearchData> {
        let envelope: ApiResponse<SearchData> =
            self.send_get("/v1/search", Some(&params)).await?;
        Self::unwrap_data(envelope)
    }

    /// `GET /v1/auth/message` — message to sign for JWT (no private key used here).
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn get_auth_message(&self) -> Result<AuthMessageData> {
        let envelope: ApiResponse<AuthMessageData> =
            self.send_get::<(), _>("/v1/auth/message", None).await?;
        Self::unwrap_data(envelope)
    }

    /// `GET /v1/categories/{slug}` — category + nested markets (any status).
    ///
    /// Prefer this over paginating `GET /v1/categories?status=OPEN` when you
    /// already know the slug (e.g. from a predict.fun URL). OPEN list order is
    /// publish-time biased and may bury the live rolling window dozens of pages deep.
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call. HTTP 404 is
    /// mapped to [`Ok`]`(None)`.
    pub async fn get_category_by_slug(&self, slug: &str) -> Result<Option<Category>> {
        let slug = slug.trim().trim_matches('/');
        if slug.is_empty() {
            return Ok(None);
        }
        let path = format!("/v1/categories/{slug}");
        match self.send_get::<(), ApiResponse<Category>>(&path, None).await {
            Ok(envelope) => {
                if !envelope.success {
                    let msg = envelope
                        .message
                        .or(envelope.error)
                        .unwrap_or_else(|| "unknown API error".to_string());
                    if msg.to_ascii_lowercase().contains("not found") {
                        return Ok(None);
                    }
                    return Err(Error::Api(msg));
                }
                Ok(envelope.data)
            }
            Err(Error::Http { status, .. }) if status == 404 => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Finds a market whose category slug equals `slug` (exact match).
    ///
    /// Uses `GET /v1/categories/{slug}` first so website URLs resolve even when
    /// the category is already `RESOLVED` (not present in `status=OPEN` lists).
    /// Does **not** apply venue-specific "live window" selection — BTC 5m live
    /// picking belongs in sample/strategy code (see `bin/btc_5m.rs`), not HTTP.
    ///
    /// # Errors
    ///
    /// Returns transport / decode / API errors from the REST call.
    pub async fn find_market_by_category_slug(&self, slug: &str) -> Result<Option<Market>> {
        let slug = slug.trim();
        if slug.is_empty() {
            return Ok(None);
        }

        if let Some(cat) = self.get_category_by_slug(slug).await? {
            if let Some(market) = first_market_from_category(cat) {
                return Ok(Some(market));
            }
        }

        if let Ok(markets) = self
            .get_markets(GetMarketsParams {
                status: None,
                trading_status: None,
                category_slug: Some(slug.to_string()),
                market_variant: None,
                has_active_rewards: None,
                sort: None,
                first: Some(10),
                after: None,
                limit: Some(10),
                cursor: None,
            })
            .await
        {
            if let Some(m) = markets.into_iter().find(|m| {
                m.category_slug
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(slug))
            }) {
                return Ok(Some(m));
            }
        }

        Ok(None)
    }
}

fn first_market_from_category(cat: Category) -> Option<Market> {
    let cat_slug = cat.slug.clone();
    for mut market in cat.markets {
        if market.category_slug.is_none() {
            market.category_slug = cat_slug.clone();
        }
        return Some(market);
    }
    None
}

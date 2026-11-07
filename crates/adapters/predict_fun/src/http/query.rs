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

//! Query parameter builders for Predict.fun REST endpoints.

use serde::Serialize;

/// Query params for `GET /v1/categories`.
///
/// Predict.fun uses Relay-style `first` / `after` pagination (not `limit` / `cursor`
/// as query names). The response still returns the next page token in `cursor`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetCategoriesParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Page size (`first` in the OpenAPI spec).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<u32>,
    /// Keyset cursor from the previous response (`after` in the OpenAPI spec).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// e.g. `CRYPTO_UP_DOWN` to narrow rolling crypto windows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_variant: Option<String>,
    /// e.g. `VOLUME_24H_DESC`, `PUBLISHED_AT_DESC`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<String>,
}

/// Query params for `GET /v1/markets`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetMarketsParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Not in current OpenAPI query list; kept for callers / possible server support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trading_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category_slug: Option<String>,
    /// e.g. `CRYPTO_UP_DOWN`, `DEFAULT` (OpenAPI `marketVariant`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_variant: Option<String>,
    /// When true, only markets with an active reward period.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_active_rewards: Option<bool>,
    /// e.g. `REWARD_RATE_DESC`, `VOLUME_24H_DESC`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    // Backward-compatible aliases some docs still mention.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Query params for `GET /v1/search`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetSearchParams {
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

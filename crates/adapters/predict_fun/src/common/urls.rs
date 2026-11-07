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

//! URL resolution for Predict.fun REST and WebSocket endpoints.

const HTTP_TESTNET: &str = "https://api-testnet.predict.fun";
const HTTP_MAINNET: &str = "https://api.predict.fun";
// Predict docs currently publish a single WS host. `ws-testnet.predict.fun` does not
// resolve in DNS; testnet samples use the same public WS endpoint (override with
// PREDICT_FUN_WS_URL if Predict adds a dedicated testnet host later).
const WS_TESTNET: &str = "wss://ws.predict.fun/ws";
const WS_MAINNET: &str = "wss://ws.predict.fun/ws";

/// Predict.fun deployment environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PredictFunEnvironment {
    /// Public testnet (default for sample bins; no API key required).
    #[default]
    Testnet,
    /// Mainnet (requires `x-api-key` for most routes).
    Mainnet,
}

impl PredictFunEnvironment {
    /// Parses `testnet` / `mainnet` (case-insensitive). Unknown values map to testnet.
    #[must_use]
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "mainnet" | "main" | "prod" | "production" => Self::Mainnet,
            _ => Self::Testnet,
        }
    }

    /// REST base URL for this environment.
    #[must_use]
    pub const fn http_base_url(self) -> &'static str {
        match self {
            Self::Testnet => HTTP_TESTNET,
            Self::Mainnet => HTTP_MAINNET,
        }
    }

    /// WebSocket URL for this environment.
    #[must_use]
    pub const fn ws_url(self) -> &'static str {
        match self {
            Self::Testnet => WS_TESTNET,
            Self::Mainnet => WS_MAINNET,
        }
    }
}

/// Resolves the REST base URL from env overrides and `PREDICT_FUN_ENV`.
#[must_use]
pub fn resolve_http_base_url() -> String {
    if let Ok(url) = std::env::var("PREDICT_FUN_HTTP_URL") {
        let trimmed = url.trim().trim_end_matches('/').to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    let env = std::env::var("PREDICT_FUN_ENV").unwrap_or_default();
    PredictFunEnvironment::parse(&env)
        .http_base_url()
        .to_string()
}

/// Resolves the WebSocket URL from env overrides and `PREDICT_FUN_ENV`.
#[must_use]
pub fn resolve_ws_url() -> String {
    if let Ok(url) = std::env::var("PREDICT_FUN_WS_URL") {
        let trimmed = url.trim().to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    let env = std::env::var("PREDICT_FUN_ENV").unwrap_or_default();
    PredictFunEnvironment::parse(&env).ws_url().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env() {
        assert_eq!(
            PredictFunEnvironment::parse("mainnet"),
            PredictFunEnvironment::Mainnet
        );
        assert_eq!(
            PredictFunEnvironment::parse("TESTNET"),
            PredictFunEnvironment::Testnet
        );
    }
}

// -------------------------------------------------------------------------------------------------
//! Configuration for the Predict.fun adapter.

use nautilus_model::instruments::InstrumentAny;

use crate::common::urls::{PredictFunEnvironment, resolve_http_base_url, resolve_ws_url};

/// HTTP / connection settings for Predict.fun.
#[derive(Debug, Clone)]
pub struct PredictFunHttpConfig {
    /// REST base URL (no trailing slash).
    pub base_url: String,
    /// WebSocket URL.
    pub ws_url: String,
    /// Optional API key for mainnet (`x-api-key`).
    pub api_key: Option<String>,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
}

impl Default for PredictFunHttpConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl PredictFunHttpConfig {
    /// Builds config from environment variables (defaults to testnet).
    #[must_use]
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        let api_key = std::env::var("PREDICT_FUN_API_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Self {
            base_url: resolve_http_base_url(),
            ws_url: resolve_ws_url(),
            api_key,
            timeout_secs: 60,
        }
    }

    /// Explicit testnet defaults (no API key).
    #[must_use]
    pub fn testnet() -> Self {
        Self {
            base_url: PredictFunEnvironment::Testnet.http_base_url().to_string(),
            ws_url: PredictFunEnvironment::Testnet.ws_url().to_string(),
            api_key: None,
            timeout_secs: 60,
        }
    }

    /// Explicit mainnet defaults; pass API key via [`Self::with_api_key`].
    #[must_use]
    pub fn mainnet() -> Self {
        Self {
            base_url: PredictFunEnvironment::Mainnet.http_base_url().to_string(),
            ws_url: PredictFunEnvironment::Mainnet.ws_url().to_string(),
            api_key: None,
            timeout_secs: 60,
        }
    }

    /// Sets the API key used for the `x-api-key` header.
    #[must_use]
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Sets the HTTP timeout in seconds.
    #[must_use]
    pub fn with_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }
}

/// Live data-client configuration for Predict.fun (used by [`crate::factories`]).
#[derive(Debug, Clone)]
pub struct PredictFunDataClientConfig {
    /// Underlying HTTP / WS endpoints + API key.
    pub http: PredictFunHttpConfig,
    /// Instruments published during `connect` (so strategies can subscribe immediately).
    pub seed_instruments: Vec<InstrumentAny>,
}

impl Default for PredictFunDataClientConfig {
    fn default() -> Self {
        Self {
            http: PredictFunHttpConfig::from_env(),
            seed_instruments: Vec::new(),
        }
    }
}

impl PredictFunDataClientConfig {
    /// Builds from environment (mainnet/testnet via `PREDICT_FUN_ENV`).
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            http: PredictFunHttpConfig::from_env(),
            seed_instruments: Vec::new(),
        }
    }

    /// Explicit mainnet HTTP defaults.
    #[must_use]
    pub fn mainnet() -> Self {
        Self {
            http: PredictFunHttpConfig::mainnet(),
            seed_instruments: Vec::new(),
        }
    }

    /// Sets the API key.
    #[must_use]
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.http = self.http.with_api_key(api_key);
        self
    }

    /// Seeds instruments that will be published on connect.
    #[must_use]
    pub fn with_seed_instruments(mut self, instruments: Vec<InstrumentAny>) -> Self {
        self.seed_instruments = instruments;
        self
    }
}

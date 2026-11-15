//! Arcus data / execution client configuration.

use nautilus_core::string::secret::SecretString;
use nautilus_model::{identifiers::AccountId, instruments::InstrumentAny};

use crate::common::enums::ArcusEnvironment;

/// Live data-client config (public market data).
#[derive(Debug, Clone, Default)]
pub struct ArcusDataClientConfig {
    pub environment: ArcusEnvironment,
    /// Instruments published on connect (and used for subscribe symbol maps).
    pub seed_instruments: Vec<InstrumentAny>,
}

impl ArcusDataClientConfig {
    #[must_use]
    pub fn with_seed_instruments(mut self, instruments: Vec<InstrumentAny>) -> Self {
        self.seed_instruments = instruments;
        self
    }

    #[must_use]
    pub fn with_environment(mut self, environment: ArcusEnvironment) -> Self {
        self.environment = environment;
        self
    }
}

/// Live execution-client config (Ed25519 signed place/cancel).
#[derive(Debug, Clone)]
pub struct ArcusExecutionClientConfig {
    pub environment: ArcusEnvironment,
    pub account_id: AccountId,
    /// Master Ethereum address (`0x…`). Falls back to `ARCUS_ADDRESS`.
    pub address: Option<String>,
    /// Subaccount index (default 0). Falls back to `ARCUS_ACCOUNT_INDEX`.
    pub account_index: Option<u32>,
    /// Ed25519 private key hex. Falls back to `ARCUS_API_SECRET`.
    pub private_key: Option<SecretString>,
}

impl Default for ArcusExecutionClientConfig {
    fn default() -> Self {
        Self {
            environment: ArcusEnvironment::Mainnet,
            account_id: AccountId::from("ARCUS-001"),
            address: None,
            account_index: None,
            private_key: None,
        }
    }
}

impl ArcusExecutionClientConfig {
    #[must_use]
    pub fn resolved_address(&self) -> anyhow::Result<String> {
        self.address
            .clone()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| std::env::var("ARCUS_ADDRESS").ok())
            .map(|s| s.trim().to_lowercase())
            .filter(|s| s.starts_with("0x") && s.len() == 42)
            .ok_or_else(|| {
                anyhow::anyhow!("Arcus address missing (config.address or ARCUS_ADDRESS)")
            })
    }

    #[must_use]
    pub fn resolved_account_index(&self) -> u32 {
        self.account_index.or_else(|| {
            std::env::var("ARCUS_ACCOUNT_INDEX")
                .ok()
                .and_then(|s| s.parse().ok())
        }).unwrap_or(0)
    }
}

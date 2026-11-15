//! Arcus environment selection.

use serde::{Deserialize, Serialize};

/// Target Arcus deployment environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ArcusEnvironment {
    #[default]
    Mainnet,
    Testnet,
}

impl ArcusEnvironment {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        }
    }
}

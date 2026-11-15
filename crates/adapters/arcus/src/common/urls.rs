//! Arcus REST / WS base URLs.

use crate::common::enums::ArcusEnvironment;

/// Mainnet REST base.
pub const ARCUS_REST_MAINNET: &str = "https://api.arcus.xyz";
/// Testnet REST base.
pub const ARCUS_REST_TESTNET: &str = "https://api.testnet.arcus.xyz";
/// Mainnet WebSocket.
pub const ARCUS_WS_MAINNET: &str = "wss://api.arcus.xyz/v1/ws";
/// Testnet WebSocket.
pub const ARCUS_WS_TESTNET: &str = "wss://api.testnet.arcus.xyz/v1/ws";

#[must_use]
pub fn rest_base(env: ArcusEnvironment) -> &'static str {
    match env {
        ArcusEnvironment::Mainnet => ARCUS_REST_MAINNET,
        ArcusEnvironment::Testnet => ARCUS_REST_TESTNET,
    }
}

#[must_use]
pub fn ws_url(env: ArcusEnvironment) -> &'static str {
    match env {
        ArcusEnvironment::Mainnet => ARCUS_WS_MAINNET,
        ArcusEnvironment::Testnet => ARCUS_WS_TESTNET,
    }
}

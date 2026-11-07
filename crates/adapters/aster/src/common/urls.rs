//! AsterDEX public REST / WS base URLs.

/// Futures REST base (USDT-M style fapi).
pub const ASTER_FAPI_REST: &str = "https://fapi.asterdex.com";

/// Futures market-data WebSocket base.
pub const ASTER_FAPI_WS: &str = "wss://fstream.asterdex.com";

/// Raw stream URL for a lowercase stream name, e.g. `btcusdt@bookTicker`.
pub fn raw_stream_url(stream: &str) -> String {
    format!("{ASTER_FAPI_WS}/ws/{stream}")
}

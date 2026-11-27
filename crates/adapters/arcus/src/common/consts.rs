//! Venue / client identifiers for the Arcus adapter.

use std::sync::LazyLock;
use std::time::Duration;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

/// Match Lighter market-data heartbeat (empty WS Ping → Pong RTT).
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Three heartbeat cycles without any frame → tear down (same ratio as Lighter).
pub const HEARTBEAT_TIMEOUT: Duration =
    Duration::from_secs(HEARTBEAT_INTERVAL.as_secs().saturating_mul(3));

/// Venue identifier string.
pub const ARCUS: &str = "ARCUS";

/// Static venue instance.
pub static ARCUS_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(ARCUS)));

/// Static client ID instance.
pub static ARCUS_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(ARCUS)));

/// Default price / size precision for quote ticks (MVP).
pub const ARCUS_PRICE_PRECISION: u8 = 8;
pub const ARCUS_SIZE_PRECISION: u8 = 8;

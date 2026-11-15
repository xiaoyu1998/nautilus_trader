//! Venue / client identifiers for the Arcus adapter.

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

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

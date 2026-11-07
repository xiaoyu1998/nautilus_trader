//! Instrument helpers for TxFlow USDC perps.

use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    enums::CurrencyType,
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Price, Quantity},
};

use crate::common::consts::{TXFLOW_PRICE_PRECISION, TXFLOW_SIZE_PRECISION, TXFLOW_VENUE};

/// `BTC` + coin_id `1` → `BTC-USDC-PERP.TXFLOW` with raw_symbol = coin id.
#[must_use]
pub fn instrument_id_for_base(base: &str) -> InstrumentId {
    InstrumentId::new(Symbol::new(format!("{base}-USDC-PERP")), *TXFLOW_VENUE)
}

pub fn coin_id_from_instrument_id(id: &InstrumentId) -> anyhow::Result<String> {
    // Prefer raw_symbol when available via seed instruments; strategy uses providers map.
    // Fallback: reject — callers should track coin_id separately.
    Err(anyhow::anyhow!(
        "TxFlow coin_id must come from seed raw_symbol; got {}",
        id
    ))
}

pub fn perpetual_from_market(coin_id: &str, base_asset: &str) -> anyhow::Result<InstrumentAny> {
    let ts = get_atomic_clock_realtime().get_time_ns();
    let base = Currency::new(base_asset, 8, 0, base_asset, CurrencyType::Crypto);
    let usdc = Currency::new("USDC", 6, 0, "USDC", CurrencyType::Crypto);
    let perp = CryptoPerpetual::builder()
        .instrument_id(instrument_id_for_base(base_asset))
        .raw_symbol(Symbol::new(coin_id))
        .base_currency(base)
        .quote_currency(usdc)
        .settlement_currency(usdc)
        .is_inverse(false)
        .price_precision(TXFLOW_PRICE_PRECISION)
        .size_precision(TXFLOW_SIZE_PRECISION)
        .price_increment(Price::new(1e-8, TXFLOW_PRICE_PRECISION))
        .size_increment(Quantity::new(1e-8, TXFLOW_SIZE_PRECISION))
        .ts_event(ts)
        .ts_init(ts)
        .build()
        .map_err(|e| anyhow::anyhow!("build TxFlow perpetual {base_asset}: {e}"))?;
    Ok(InstrumentAny::CryptoPerpetual(perp))
}

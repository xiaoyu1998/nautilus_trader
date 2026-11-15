//! Instrument helpers for Arcus perps.

use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::common::consts::{ARCUS_PRICE_PRECISION, ARCUS_SIZE_PRECISION, ARCUS_VENUE};
use crate::http::MarketInfo;
use crate::signing::{parse_decimal, to_engine_int};

/// Nautilus instrument id: `ZEC-USD` → `ZEC-USD-PERP.ARCUS`.
#[must_use]
pub fn instrument_id_for_ticker(ticker: &str) -> InstrumentId {
    InstrumentId::new(Symbol::new(format!("{ticker}-PERP")), *ARCUS_VENUE)
}

/// Wire ticker (`ZEC-USD`) from a Nautilus instrument id.
pub fn ticker_from_instrument_id(id: &InstrumentId) -> anyhow::Result<String> {
    let sym = id.symbol.as_str();
    let base = sym
        .strip_suffix("-PERP")
        .ok_or_else(|| anyhow::anyhow!("Arcus instrument symbol must end with -PERP, got {sym}"))?;
    Ok(base.to_string())
}

/// Precision from a decimal step string (e.g. `0.01` → 2).
fn precision_from_step(step: &str) -> u8 {
    let Ok(d) = parse_decimal(step) else {
        return ARCUS_PRICE_PRECISION;
    };
    let s = d.normalize().to_string();
    if let Some(dot) = s.find('.') {
        ((s.len() - dot - 1) as u8).min(16)
    } else {
        0
    }
}

/// Build a `CryptoPerpetual` from Arcus market metadata.
pub fn perpetual_from_market(m: &MarketInfo) -> anyhow::Result<InstrumentAny> {
    let ts = get_atomic_clock_realtime().get_time_ns();
    let price_precision = precision_from_step(&m.tick_size);
    let size_precision = precision_from_step(&m.step_size);
    let tick: f64 = m.tick_size.parse().unwrap_or(1e-8);
    let step: f64 = m.step_size.parse().unwrap_or(1e-8);
    let base = Currency::new(
        &m.base_asset,
        size_precision,
        0,
        &m.base_asset,
        nautilus_model::enums::CurrencyType::Crypto,
    );
    let quote = Currency::USD();
    let perp = CryptoPerpetual::builder()
        .instrument_id(instrument_id_for_ticker(&m.market_display_name))
        .raw_symbol(Symbol::new(m.market_display_name.as_str()))
        .base_currency(base)
        .quote_currency(quote)
        .settlement_currency(quote)
        .is_inverse(false)
        .price_precision(price_precision)
        .size_precision(size_precision)
        .price_increment(Price::new(tick.max(1e-12), price_precision))
        .size_increment(Quantity::new(step.max(1e-12), size_precision))
        .ts_event(ts)
        .ts_init(ts)
        .build()
        .map_err(|e| {
            anyhow::anyhow!(
                "build Arcus perpetual {}: {e}",
                m.market_display_name
            )
        })?;
    Ok(InstrumentAny::CryptoPerpetual(perp))
}

/// Minimal seed instrument when only a ticker is known.
pub fn perpetual_from_ticker(ticker: &str, base_asset: &str) -> anyhow::Result<InstrumentAny> {
    let ts = get_atomic_clock_realtime().get_time_ns();
    let base = Currency::new(
        base_asset,
        ARCUS_SIZE_PRECISION,
        0,
        base_asset,
        nautilus_model::enums::CurrencyType::Crypto,
    );
    let quote = Currency::USD();
    let perp = CryptoPerpetual::builder()
        .instrument_id(instrument_id_for_ticker(ticker))
        .raw_symbol(Symbol::new(ticker))
        .base_currency(base)
        .quote_currency(quote)
        .settlement_currency(quote)
        .is_inverse(false)
        .price_precision(ARCUS_PRICE_PRECISION)
        .size_precision(ARCUS_SIZE_PRECISION)
        .price_increment(Price::new(1e-8, ARCUS_PRICE_PRECISION))
        .size_increment(Quantity::new(1e-8, ARCUS_SIZE_PRECISION))
        .ts_event(ts)
        .ts_init(ts)
        .build()
        .map_err(|e| anyhow::anyhow!("build Arcus perpetual {ticker}: {e}"))?;
    Ok(InstrumentAny::CryptoPerpetual(perp))
}

/// Cached market metadata for signing conversions.
#[derive(Debug, Clone)]
pub struct MarketMeta {
    pub ticker: String,
    pub market_id: u32,
    pub tick_size: Decimal,
    pub step_size: Decimal,
    pub instrument_id: InstrumentId,
}

impl MarketMeta {
    pub fn from_info(m: &MarketInfo) -> anyhow::Result<Self> {
        Ok(Self {
            ticker: m.market_display_name.clone(),
            market_id: m.market_id,
            tick_size: parse_decimal(&m.tick_size)?,
            step_size: parse_decimal(&m.step_size)?,
            instrument_id: instrument_id_for_ticker(&m.market_display_name),
        })
    }

    pub fn price_ticks(&self, price: Decimal) -> anyhow::Result<u64> {
        to_engine_int(price, self.tick_size)
    }

    pub fn qty_quantums(&self, qty: Decimal) -> anyhow::Result<u64> {
        to_engine_int(qty, self.step_size)
    }
}

//! Custom data: Predict.fun market Predict Points (PP) rewards.

use std::{any::Any, sync::Arc};

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{CustomDataTrait, HasTsInit},
    identifiers::InstrumentId,
};
use serde::{Deserialize, Serialize};

use crate::http::models::Market;

/// [`DataType`] / [`CustomDataTrait`] type name for market PP rewards.
pub const PREDICT_FUN_MARKET_REWARDS: &str = "PredictFunMarketRewards";

/// Market PP reward snapshot delivered as Nautilus [`nautilus_model::data::CustomData`].
///
/// Source: REST `GET /v1/markets/{id}` → `rewards.current.hourlyRate` (PP/hr).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredictFunMarketRewards {
    /// Nautilus instrument for this market (`{market_id}.PREDICT_FUN`).
    pub instrument_id: InstrumentId,
    /// Predict.fun numeric market id.
    pub market_id: i64,
    /// Current window PP per hour (`rewards.current.hourlyRate`), if accruing.
    pub hourly_rate: Option<i32>,
    /// Active period start (`rewards.current.startsAt`), if any.
    pub starts_at: Option<String>,
    /// Active period end (`rewards.current.endsAt`), if any.
    pub ends_at: Option<String>,
    /// Scheduled next PP/hr when `current` is null (`rewards.schedule[0]`).
    pub scheduled_hourly_rate: Option<i32>,
    /// Event timestamp (when the snapshot was taken).
    pub ts_event: UnixNanos,
    /// Init timestamp.
    pub ts_init: UnixNanos,
}

impl PredictFunMarketRewards {
    /// Canonical type name (matches [`DataType::type_name`] / subscribe topic).
    #[must_use]
    pub const fn type_name_str() -> &'static str {
        PREDICT_FUN_MARKET_REWARDS
    }

    /// Builds a snapshot from a REST [`Market`] and instrument id.
    #[must_use]
    pub fn from_market(market: &Market, instrument_id: InstrumentId, ts: UnixNanos) -> Self {
        let (hourly_rate, starts_at, ends_at) = market
            .rewards
            .as_ref()
            .and_then(|r| r.current.as_ref())
            .map(|p| {
                (
                    Some(p.hourly_rate),
                    Some(p.starts_at.clone()),
                    Some(p.ends_at.clone()),
                )
            })
            .unwrap_or((None, None, None));

        Self {
            instrument_id,
            market_id: market.id,
            hourly_rate,
            starts_at,
            ends_at,
            scheduled_hourly_rate: market.scheduled_hourly_rate(),
            ts_event: ts,
            ts_init: ts,
        }
    }
}

impl HasTsInit for PredictFunMarketRewards {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for PredictFunMarketRewards {
    fn type_name(&self) -> &'static str {
        PREDICT_FUN_MARKET_REWARDS
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|o| self == o)
    }

    fn type_name_static() -> &'static str {
        PREDICT_FUN_MARKET_REWARDS
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        let parsed: Self = serde_json::from_value(value)?;
        Ok(Arc::new(parsed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::models::Market;

    #[test]
    fn from_market_current_hourly_rate() {
        let market: Market = serde_json::from_value(serde_json::json!({
            "id": 12345,
            "rewards": {
                "current": {
                    "hourlyRate": 3000,
                    "startsAt": "2026-10-25T10:00:00.000Z",
                    "endsAt": "2026-10-25T10:05:00.000Z"
                },
                "schedule": []
            }
        }))
        .unwrap();
        let iid = InstrumentId::from("12345.PREDICT_FUN");
        let snap = PredictFunMarketRewards::from_market(&market, iid, UnixNanos::from(1));
        assert_eq!(snap.market_id, 12345);
        assert_eq!(snap.hourly_rate, Some(3000));
        assert_eq!(snap.type_name(), PREDICT_FUN_MARKET_REWARDS);
    }
}

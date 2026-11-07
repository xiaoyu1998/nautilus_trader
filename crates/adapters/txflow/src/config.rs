//! TxFlow data client configuration.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nautilus_model::instruments::InstrumentAny;

/// Maker/taker (+ hash) for one public trade, keyed by venue `tid` for strategy ingest.
#[derive(Debug, Clone)]
pub struct TxFlowTradeExtra {
    pub symbol: String,
    pub taker: String,
    pub maker: String,
    pub hash: Option<String>,
    pub ts_ms: i64,
}

/// Shared tape of recent trade extras (`tid` → fields not on [`nautilus_model::data::TradeTick`]).
#[derive(Debug, Default)]
pub struct TxFlowTradeTape {
    inner: Mutex<HashMap<String, TxFlowTradeExtra>>,
}

impl TxFlowTradeTape {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, tid: String, extra: TxFlowTradeExtra) {
        let mut guard = self.inner.lock().unwrap();
        // Bound memory: drop oldest-ish by clearing when large.
        if guard.len() > 50_000 {
            guard.clear();
        }
        guard.insert(tid, extra);
    }

    pub fn take(&self, tid: &str) -> Option<TxFlowTradeExtra> {
        self.inner.lock().unwrap().remove(tid)
    }
}

#[derive(Debug, Clone, Default)]
pub struct TxFlowDataClientConfig {
    /// Instruments whose BBO + l2BookDepth streams start at connect (quote venue).
    pub seed_instruments: Vec<InstrumentAny>,
    /// Extra instruments resolvable for `subscribe_trades` (not auto BBO/L2).
    pub trade_instruments: Vec<InstrumentAny>,
    /// Optional side-channel for maker/taker addresses (strategy maker_db ingest).
    pub trade_tape: Option<Arc<TxFlowTradeTape>>,
}

impl TxFlowDataClientConfig {
    #[must_use]
    pub fn with_seed_instruments(mut self, instruments: Vec<InstrumentAny>) -> Self {
        self.seed_instruments = instruments;
        self
    }

    #[must_use]
    pub fn with_trade_instruments(mut self, instruments: Vec<InstrumentAny>) -> Self {
        self.trade_instruments = instruments;
        self
    }

    #[must_use]
    pub fn with_trade_tape(mut self, tape: Arc<TxFlowTradeTape>) -> Self {
        self.trade_tape = Some(tape);
        self
    }
}

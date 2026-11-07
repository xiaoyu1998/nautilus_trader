use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarDto {
    pub ts_event_ns: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternSignalView {
    pub ts_event_ns: u64,
    pub pattern_type: String,
    pub strength: f64,
    pub first_pivot_ts_event_ns: u64,
    pub second_pivot_ts_event_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpDumpDetectorMarketPanel {
    pub instrument_id: String,
    pub bars: Vec<BarDto>,
    pub patterns: Vec<PatternSignalView>,
}

pub struct PumpDumpDetectorDashboardStore {
    markets: HashMap<String, PumpDumpDetectorMarketPanel>,
    order: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpDumpDetectorDashboardSnapshot {
    pub markets: Vec<PumpDumpDetectorMarketPanel>,
}

const MAX_BARS_PER_MARKET: usize = 2000;
const MAX_PATTERNS_PER_MARKET: usize = 300;

impl PumpDumpDetectorDashboardStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            markets: HashMap::new(),
            order: Vec::new(),
        }
    }

    pub fn ensure_market(&mut self, instrument_id: &str) {
        if self.markets.contains_key(instrument_id) {
            return;
        }
        self.order.push(instrument_id.to_string());
        self.markets.insert(
            instrument_id.to_string(),
            PumpDumpDetectorMarketPanel {
                instrument_id: instrument_id.to_string(),
                bars: Vec::new(),
                patterns: Vec::new(),
            },
        );
    }

    pub fn remove_market(&mut self, instrument_id: &str) {
        self.markets.remove(instrument_id);
        self.order.retain(|x| x != instrument_id);
    }

    pub fn append_bar_and_patterns(
        &mut self,
        instrument_id: &str,
        bar: BarDto,
        patterns: Vec<PatternSignalView>,
    ) {
        let Some(panel) = self.markets.get_mut(instrument_id) else {
            return;
        };
        panel.bars.push(bar);
        panel.patterns.extend(patterns);
        let bar_excess = panel.bars.len().saturating_sub(MAX_BARS_PER_MARKET);
        if bar_excess > 0 {
            panel.bars.drain(0..bar_excess);
        }
        let pat_excess = panel.patterns.len().saturating_sub(MAX_PATTERNS_PER_MARKET);
        if pat_excess > 0 {
            panel.patterns.drain(0..pat_excess);
        }
    }

    pub fn list_pool(&self) -> Vec<String> {
        self.order.clone()
    }

    pub fn list_panels(&self) -> Vec<PumpDumpDetectorMarketPanel> {
        self.order
            .iter()
            .filter_map(|id| self.markets.get(id).cloned())
            .collect()
    }

    pub fn to_snapshot(&self) -> PumpDumpDetectorDashboardSnapshot {
        PumpDumpDetectorDashboardSnapshot {
            markets: self.list_panels(),
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: PumpDumpDetectorDashboardSnapshot) {
        self.markets.clear();
        self.order.clear();
        for panel in snapshot.markets {
            self.order.push(panel.instrument_id.clone());
            self.markets.insert(panel.instrument_id.clone(), panel);
        }
    }

    pub fn load_from_path(path: &Path) -> Option<Self> {
        let raw = std::fs::read_to_string(path).ok()?;
        let snapshot = serde_json::from_str::<PumpDumpDetectorDashboardSnapshot>(&raw).ok()?;
        let mut store = Self::new();
        store.apply_snapshot(snapshot);
        Some(store)
    }

    pub fn save_to_path(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let raw = serde_json::to_string_pretty(&self.to_snapshot())?;
        std::fs::write(path, raw)?;
        Ok(())
    }
}

impl Default for PumpDumpDetectorDashboardStore {
    fn default() -> Self {
        Self::new()
    }
}

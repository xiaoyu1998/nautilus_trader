mod bearish_divergence_bars;
mod bearish_double_top_bars;
mod bearish_gravestone_bars;
mod bullish_divergence_bars;
mod bullish_double_bottom_bars;
mod bullish_hammer_bars;
mod death_cross_bars;
mod golden_cross_bars;
mod utils;

#[derive(Debug, Clone)]
pub struct TrapBarPoint {
    pub ts_event_ns: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone)]
pub struct BearishTrapBars {
    pub bars: Vec<TrapBarPoint>,
    pub strength: f64,
    pub first_pivot_ts_event_ns: u64,
    pub second_pivot_ts_event_ns: u64,
}

pub use bearish_divergence_bars::{
    BearishDivergenceBarScan,
};
pub use bearish_double_top_bars::{
    BearishDoubleTopBarScan,
};
pub use bearish_gravestone_bars::{
    BearishGravestoneBarScan,
};
pub use bullish_divergence_bars::BullishDivergenceBarScan;
pub use bullish_double_bottom_bars::BullishDoubleBottomBarScan;
pub use bullish_hammer_bars::BullishHammerBarScan;
pub use death_cross_bars::DeathCrossBarScan;
pub use golden_cross_bars::GoldenCrossBarScan;

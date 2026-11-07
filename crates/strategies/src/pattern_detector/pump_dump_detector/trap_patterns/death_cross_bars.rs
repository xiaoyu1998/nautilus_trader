use std::collections::VecDeque;

use super::super::EPS;
use super::utils::macd_line_and_signal_last_two;
use super::{BearishTrapBars, TrapBarPoint};

const DEFAULT_MAX_BARS: usize = 512;
const DEFAULT_FAST_PERIOD: usize = 12;
const DEFAULT_SLOW_PERIOD: usize = 26;
const DEFAULT_SIGNAL_PERIOD: usize = 9;
const DEFAULT_MIN_CROSS_GAP_RATIO: f64 = 0.0005;

#[derive(Debug, Clone)]
pub struct DeathCrossBarScan {
    max_bars: usize,
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
    min_cross_gap_ratio: f64,
    bars: VecDeque<TrapBarPoint>,
    closes: VecDeque<f64>,
}

impl DeathCrossBarScan {
    pub fn new(
        max_bars: usize,
        fast_period: usize,
        slow_period: usize,
        signal_period: usize,
        min_cross_gap_ratio: f64,
    ) -> Self {
        let fast = fast_period.max(2);
        let slow = slow_period.max(fast + 1);
        let signal = signal_period.max(2);
        let warmup = slow + signal;
        Self {
            max_bars: max_bars.max(warmup + 8),
            fast_period: fast,
            slow_period: slow,
            signal_period: signal,
            min_cross_gap_ratio: min_cross_gap_ratio.max(0.0),
            bars: VecDeque::new(),
            closes: VecDeque::new(),
        }
    }

    pub fn new_default() -> Self {
        Self::new(
            DEFAULT_MAX_BARS,
            DEFAULT_FAST_PERIOD,
            DEFAULT_SLOW_PERIOD,
            DEFAULT_SIGNAL_PERIOD,
            DEFAULT_MIN_CROSS_GAP_RATIO,
        )
    }

    pub fn update(
        &mut self,
        ts_event_ns: u64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
    ) -> Option<BearishTrapBars> {
        if !bar_ok(open, high, low, close, volume) {
            return None;
        }

        self.bars.push_back(TrapBarPoint {
            ts_event_ns,
            open,
            high,
            low,
            close,
            volume,
        });
        self.closes.push_back(close);
        while self.bars.len() > self.max_bars {
            self.bars.pop_front();
        }
        while self.closes.len() > self.max_bars {
            self.closes.pop_front();
        }

        let closes: Vec<f64> = self.closes.iter().copied().collect();
        let (prev_macd, prev_signal, curr_macd, curr_signal) = macd_line_and_signal_last_two(
            &closes,
            self.fast_period,
            self.slow_period,
            self.signal_period,
        )?;

        if !(prev_macd >= prev_signal && curr_macd < curr_signal) {
            return None;
        }

        let gap_ratio = (curr_signal - curr_macd) / (curr_signal.abs() + EPS);
        if gap_ratio < self.min_cross_gap_ratio {
            return None;
        }

        let take = (self.slow_period + self.signal_period).min(self.bars.len());
        let start = self.bars.len() - take;
        let bars = self.bars.iter().skip(start).cloned().collect::<Vec<_>>();

        if bars.len() < 2 {
            return None;
        }
        let prev_ts = bars[bars.len() - 2].ts_event_ns;
        let curr_ts = bars[bars.len() - 1].ts_event_ns;
        Some(BearishTrapBars {
            bars,
            strength: gap_ratio,
            first_pivot_ts_event_ns: prev_ts,
            second_pivot_ts_event_ns: curr_ts,
        })
    }
}

fn bar_ok(open: f64, high: f64, low: f64, close: f64, volume: f64) -> bool {
    open.is_finite()
        && high.is_finite()
        && low.is_finite()
        && close.is_finite()
        && volume.is_finite()
        && open > 0.0
        && high > 0.0
        && low > 0.0
        && close > 0.0
        && volume >= 0.0
        && high >= low
}

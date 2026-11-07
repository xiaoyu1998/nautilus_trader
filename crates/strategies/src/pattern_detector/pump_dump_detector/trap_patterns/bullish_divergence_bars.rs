use std::collections::VecDeque;

use super::super::EPS;
use super::utils::wilder_rsi_at;
use super::{BearishTrapBars, TrapBarPoint};

const DEFAULT_MAX_BARS: usize = 512;
const DEFAULT_RSI_PERIOD: usize = 14;
const DEFAULT_PIVOT_HALF_WIDTH: usize = 2;
const DEFAULT_MIN_PRICE_LEG_RATIO: f64 = 0.002;
const DEFAULT_MIN_RSI_GAP: f64 = 4.0;
const DEFAULT_MIN_PIVOT_GAP_BARS: usize = 4;

#[derive(Debug, Clone)]
pub struct BullishDivergenceBarScan {
    max_bars: usize,
    rsi_period: usize,
    pivot_half_width: usize,
    min_price_leg_ratio: f64,
    min_rsi_gap: f64,
    min_pivot_gap_bars: usize,
    buf: VecDeque<BarPoint>,
    prev_pivot: Option<Pivot>,
}

#[derive(Clone, Copy, Debug)]
struct BarPoint {
    ts_event_ns: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[derive(Clone, Copy, Debug)]
struct Pivot {
    idx: usize,
    ts_event_ns: u64,
    low: f64,
    rsi: f64,
}

impl BullishDivergenceBarScan {
    pub fn new(max_bars: usize, rsi_period: usize, pivot_half_width: usize) -> Self {
        Self {
            max_bars: max_bars.max(32),
            rsi_period: rsi_period.max(2),
            pivot_half_width: pivot_half_width.max(1),
            min_price_leg_ratio: DEFAULT_MIN_PRICE_LEG_RATIO,
            min_rsi_gap: DEFAULT_MIN_RSI_GAP,
            min_pivot_gap_bars: DEFAULT_MIN_PIVOT_GAP_BARS,
            buf: VecDeque::new(),
            prev_pivot: None,
        }
    }

    pub fn new_default() -> Self {
        Self::new(DEFAULT_MAX_BARS, DEFAULT_RSI_PERIOD, DEFAULT_PIVOT_HALF_WIDTH)
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
        self.buf.push_back(BarPoint {
            ts_event_ns,
            open,
            high,
            low,
            close,
            volume,
        });
        while self.buf.len() > self.max_bars {
            self.buf.pop_front();
        }

        let w = self.pivot_half_width;
        if self.buf.len() < w * 2 + 1 {
            return None;
        }
        let n = self.buf.len();
        let p = n - 1 - w;
        let slice = self.buf.iter().copied().collect::<Vec<_>>();
        if !pivot_low(&slice, p, w) {
            return None;
        }
        let closes = slice.iter().map(|b| b.close).collect::<Vec<_>>();
        let rsi_p = wilder_rsi_at(&closes, self.rsi_period, p)?;
        let current = Pivot {
            idx: p,
            ts_event_ns: slice[p].ts_event_ns,
            low: slice[p].low,
            rsi: rsi_p,
        };

        let mut out = None;
        if let Some(prev) = self.prev_pivot {
            let pivot_gap = current.idx.saturating_sub(prev.idx);
            let price_leg = (prev.low / (current.low + EPS) - 1.0).max(0.0);
            let rsi_gap = (current.rsi - prev.rsi).max(0.0);
            if pivot_gap >= self.min_pivot_gap_bars
                && current.low < prev.low
                && current.rsi > prev.rsi
                && price_leg >= self.min_price_leg_ratio
                && rsi_gap >= self.min_rsi_gap
            {
                let ts_lo = prev.ts_event_ns.min(current.ts_event_ns);
                let ts_hi = prev.ts_event_ns.max(current.ts_event_ns);
                let bars = self
                    .buf
                    .iter()
                    .filter(|b| b.ts_event_ns >= ts_lo && b.ts_event_ns <= ts_hi)
                    .map(|b| TrapBarPoint {
                        ts_event_ns: b.ts_event_ns,
                        open: b.open,
                        high: b.high,
                        low: b.low,
                        close: b.close,
                        volume: b.volume,
                    })
                    .collect::<Vec<_>>();
                if bars.len() >= 2 {
                    let rsi_leg = rsi_gap / 100.0;
                    out = Some(BearishTrapBars {
                        bars,
                        strength: price_leg * rsi_leg,
                        first_pivot_ts_event_ns: prev.ts_event_ns,
                        second_pivot_ts_event_ns: current.ts_event_ns,
                    });
                }
            }
        }
        self.prev_pivot = Some(current);
        out
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

fn pivot_low(slice: &[BarPoint], p: usize, w: usize) -> bool {
    if p < w || p + w >= slice.len() {
        return false;
    }
    let l = slice[p].low;
    for j in (p - w)..=(p + w) {
        if j != p && slice[j].low <= l {
            return false;
        }
    }
    true
}

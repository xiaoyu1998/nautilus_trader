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
pub struct BearishDivergenceBarScan {
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
    high: f64,
    rsi: f64,
}

impl BearishDivergenceBarScan {
    pub fn new(max_bars: usize, rsi_period: usize, pivot_half_width: usize) -> Self {
        let max_bars = max_bars.max(32);
        let rsi_period = rsi_period.max(2);
        let pivot_half_width = pivot_half_width.max(1);
        Self {
            max_bars,
            rsi_period,
            pivot_half_width,
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
        if !Self::bar_ok(open, high, low, close, volume) {
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

        let slice: Vec<BarPoint> = self.buf.iter().copied().collect();
        if !Self::pivot_high(&slice, p, w) {
            return None;
        }

        let closes: Vec<f64> = slice.iter().map(|b| b.close).collect();
        let rsi_p = wilder_rsi_at(&closes, self.rsi_period, p)?;

        let current = Pivot {
            idx: p,
            ts_event_ns: slice[p].ts_event_ns,
            high: slice[p].high,
            rsi: rsi_p,
        };

        let mut out = None;
        if let Some(prev) = self.prev_pivot {
            let pivot_gap = current.idx.saturating_sub(prev.idx);
            let price_leg = (current.high / (prev.high + EPS) - 1.0).max(0.0);
            let rsi_gap = (prev.rsi - current.rsi).max(0.0);
            if pivot_gap >= self.min_pivot_gap_bars
                && current.high > prev.high
                && current.rsi < prev.rsi
                && price_leg >= self.min_price_leg_ratio
                && rsi_gap >= self.min_rsi_gap
            {
                let ts_lo = prev.ts_event_ns.min(current.ts_event_ns);
                let ts_hi = prev.ts_event_ns.max(current.ts_event_ns);
                let bars: Vec<TrapBarPoint> = self
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
                    .collect();
                if bars.len() >= 2 {
                    let rsi_leg = rsi_gap / 100.0;
                    let strength = price_leg * rsi_leg;
                    out = Some(BearishTrapBars {
                        bars,
                        strength,
                        first_pivot_ts_event_ns: prev.ts_event_ns,
                        second_pivot_ts_event_ns: current.ts_event_ns,
                    });
                }
            }
        }

        self.prev_pivot = Some(current);
        out
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

    fn pivot_high(slice: &[BarPoint], p: usize, w: usize) -> bool {
        if p < w || p + w >= slice.len() {
            return false;
        }
        let h = slice[p].high;
        for j in (p - w)..=(p + w) {
            if j != p && slice[j].high >= h {
                return false;
            }
        }
        true
    }
}


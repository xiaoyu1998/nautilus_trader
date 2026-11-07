use std::collections::VecDeque;

use super::super::EPS;
use super::{BearishTrapBars, TrapBarPoint};

const DEFAULT_MAX_BARS: usize = 512;
const DEFAULT_PIVOT_HALF_WIDTH: usize = 2;
const DEFAULT_MAX_BOTTOM_DIFF_RATIO: f64 = 0.003;
const DEFAULT_MIN_REBOUND_RATIO: f64 = 0.004;
const DEFAULT_TREND_LOOKBACK: usize = 8;
const DEFAULT_MIN_DOWNTREND_RETURN: f64 = 0.008;
const DEFAULT_MIN_PATTERN_HEIGHT_RATIO: f64 = 0.008;
const DEFAULT_ATR_PERIOD: usize = 14;
const DEFAULT_MIN_HEIGHT_OVER_ATR: f64 = 1.2;

#[derive(Debug, Clone)]
pub struct BullishDoubleBottomBarScan {
    max_bars: usize,
    pivot_half_width: usize,
    max_bottom_diff_ratio: f64,
    min_rebound_ratio: f64,
    trend_lookback: usize,
    min_downtrend_return: f64,
    min_pattern_height_ratio: f64,
    atr_period: usize,
    min_height_over_atr: f64,
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
}

impl BullishDoubleBottomBarScan {
    pub fn new(
        max_bars: usize,
        pivot_half_width: usize,
        max_bottom_diff_ratio: f64,
        min_rebound_ratio: f64,
    ) -> Self {
        Self {
            max_bars: max_bars.max(32),
            pivot_half_width: pivot_half_width.max(1),
            max_bottom_diff_ratio: max_bottom_diff_ratio.max(0.0),
            min_rebound_ratio: min_rebound_ratio.max(0.0),
            trend_lookback: DEFAULT_TREND_LOOKBACK,
            min_downtrend_return: DEFAULT_MIN_DOWNTREND_RETURN,
            min_pattern_height_ratio: DEFAULT_MIN_PATTERN_HEIGHT_RATIO,
            atr_period: DEFAULT_ATR_PERIOD,
            min_height_over_atr: DEFAULT_MIN_HEIGHT_OVER_ATR,
            buf: VecDeque::new(),
            prev_pivot: None,
        }
    }

    pub fn new_default() -> Self {
        Self::new(
            DEFAULT_MAX_BARS,
            DEFAULT_PIVOT_HALF_WIDTH,
            DEFAULT_MAX_BOTTOM_DIFF_RATIO,
            DEFAULT_MIN_REBOUND_RATIO,
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
        let current = Pivot {
            idx: p,
            ts_event_ns: slice[p].ts_event_ns,
            low: slice[p].low,
        };

        let mut out = None;
        if let Some(prev) = self.prev_pivot
            && current.idx > prev.idx + 1
        {
            let avg_bottom = ((prev.low + current.low) * 0.5).max(EPS);
            let bottom_diff_ratio = ((current.low - prev.low).abs()) / avg_bottom;
            if bottom_diff_ratio <= self.max_bottom_diff_ratio {
                let peak_high = slice[(prev.idx + 1)..current.idx]
                    .iter()
                    .map(|b| b.high)
                    .fold(f64::NEG_INFINITY, f64::max);
                if peak_high.is_finite() {
                    let rebound_ratio = (peak_high / (avg_bottom + EPS) - 1.0).max(0.0);
                    if rebound_ratio >= self.min_rebound_ratio {
                        if !self.downtrend_ok(&slice, prev.idx) {
                            self.prev_pivot = Some(current);
                            return None;
                        }
                        let pattern_height_ratio = (peak_high - avg_bottom).max(0.0) / (avg_bottom + EPS);
                        if pattern_height_ratio < self.min_pattern_height_ratio {
                            self.prev_pivot = Some(current);
                            return None;
                        }
                        let atr = self.atr_around(&slice, current.idx);
                        if atr <= 0.0 || (peak_high - avg_bottom) / atr < self.min_height_over_atr {
                            self.prev_pivot = Some(current);
                            return None;
                        }

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
                        if bars.len() >= 3 {
                            let bottom_similarity =
                                (1.0 - bottom_diff_ratio / (self.max_bottom_diff_ratio + EPS))
                                    .clamp(0.0, 1.0);
                            out = Some(BearishTrapBars {
                                bars,
                                strength: rebound_ratio * bottom_similarity,
                                first_pivot_ts_event_ns: prev.ts_event_ns,
                                second_pivot_ts_event_ns: current.ts_event_ns,
                            });
                        }
                    }
                }
            }
        }
        self.prev_pivot = Some(current);
        out
    }

    fn downtrend_ok(&self, slice: &[BarPoint], pivot_idx: usize) -> bool {
        if pivot_idx == 0 {
            return false;
        }
        let lookback = self.trend_lookback.min(pivot_idx);
        if lookback == 0 {
            return false;
        }
        let base_idx = pivot_idx - lookback;
        let base = slice[base_idx].close;
        let bottom = slice[pivot_idx].low;
        let downtrend_return = base / (bottom + EPS) - 1.0;
        downtrend_return >= self.min_downtrend_return
    }

    fn atr_around(&self, slice: &[BarPoint], end_idx: usize) -> f64 {
        if end_idx == 0 || slice.len() < 2 {
            return 0.0;
        }
        let period = self.atr_period.min(end_idx);
        if period == 0 {
            return 0.0;
        }
        let start = end_idx + 1 - period;
        let mut sum_tr = 0.0;
        let mut cnt = 0usize;
        for i in start..=end_idx {
            let prev_close = if i > 0 { slice[i - 1].close } else { slice[i].close };
            let h_l = slice[i].high - slice[i].low;
            let h_pc = (slice[i].high - prev_close).abs();
            let l_pc = (slice[i].low - prev_close).abs();
            let tr = h_l.max(h_pc).max(l_pc);
            if tr.is_finite() && tr >= 0.0 {
                sum_tr += tr;
                cnt += 1;
            }
        }
        if cnt == 0 { 0.0 } else { sum_tr / cnt as f64 }
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

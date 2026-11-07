use std::collections::VecDeque;

use super::super::EPS;
use super::{BearishTrapBars, TrapBarPoint};

const DEFAULT_MAX_BARS: usize = 256;
const DEFAULT_TREND_LOOKBACK: usize = 6;
const DEFAULT_MIN_TREND_RETURN: f64 = 0.004;
const DEFAULT_MIN_UPPER_WICK_RATIO: f64 = 0.55;
const DEFAULT_MAX_BODY_RATIO: f64 = 0.30;
const DEFAULT_MAX_LOWER_WICK_RATIO: f64 = 0.20;

#[derive(Debug, Clone)]
pub struct BearishGravestoneBarScan {
    max_bars: usize,
    trend_lookback: usize,
    min_trend_return: f64,
    min_upper_wick_ratio: f64,
    max_body_ratio: f64,
    max_lower_wick_ratio: f64,
    buf: VecDeque<BarPoint>,
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

impl BearishGravestoneBarScan {
    pub fn new(
        max_bars: usize,
        trend_lookback: usize,
        min_trend_return: f64,
        min_upper_wick_ratio: f64,
        max_body_ratio: f64,
        max_lower_wick_ratio: f64,
    ) -> Self {
        Self {
            max_bars: max_bars.max(32),
            trend_lookback: trend_lookback.max(2),
            min_trend_return: min_trend_return.max(0.0),
            min_upper_wick_ratio: min_upper_wick_ratio.clamp(0.0, 1.0),
            max_body_ratio: max_body_ratio.clamp(0.0, 1.0),
            max_lower_wick_ratio: max_lower_wick_ratio.clamp(0.0, 1.0),
            buf: VecDeque::new(),
        }
    }

    pub fn new_default() -> Self {
        Self::new(
            DEFAULT_MAX_BARS,
            DEFAULT_TREND_LOOKBACK,
            DEFAULT_MIN_TREND_RETURN,
            DEFAULT_MIN_UPPER_WICK_RATIO,
            DEFAULT_MAX_BODY_RATIO,
            DEFAULT_MAX_LOWER_WICK_RATIO,
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

        if self.buf.len() < self.trend_lookback + 1 {
            return None;
        }

        let n = self.buf.len();
        let cur = self.buf[n - 1];
        let range = (cur.high - cur.low).max(EPS);
        let body = (cur.close - cur.open).abs();
        let upper_wick = (cur.high - cur.open.max(cur.close)).max(0.0);
        let lower_wick = (cur.open.min(cur.close) - cur.low).max(0.0);

        let body_ratio = body / range;
        let upper_wick_ratio = upper_wick / range;
        let lower_wick_ratio = lower_wick / range;

        if body_ratio > self.max_body_ratio
            || upper_wick_ratio < self.min_upper_wick_ratio
            || lower_wick_ratio > self.max_lower_wick_ratio
        {
            return None;
        }

        let base = self.buf[n - 1 - self.trend_lookback].close;
        let trend_return = cur.close / (base + EPS) - 1.0;
        if trend_return < self.min_trend_return {
            return None;
        }

        let start = n - 1 - self.trend_lookback;
        let bars = self
            .buf
            .iter()
            .skip(start)
            .map(|b| TrapBarPoint {
                ts_event_ns: b.ts_event_ns,
                open: b.open,
                high: b.high,
                low: b.low,
                close: b.close,
                volume: b.volume,
            })
            .collect::<Vec<_>>();
        if bars.len() < 2 {
            return None;
        }

        let shape_strength = (upper_wick_ratio - body_ratio - lower_wick_ratio).max(0.0);
        let strength = shape_strength * trend_return.max(0.0);
        Some(BearishTrapBars {
            bars,
            strength,
            first_pivot_ts_event_ns: cur.ts_event_ns,
            second_pivot_ts_event_ns: cur.ts_event_ns,
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

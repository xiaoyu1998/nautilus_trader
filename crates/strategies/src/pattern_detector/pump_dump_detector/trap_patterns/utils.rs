fn ema_series(closes: &[f64], period: usize) -> Vec<f64> {
    let n = closes.len();
    let mut out = vec![f64::NAN; n];
    if n < period || period < 2 {
        return out;
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut sum = 0.0;
    for i in 0..period {
        sum += closes[i];
    }
    out[period - 1] = sum / period as f64;
    for i in period..n {
        out[i] = alpha * closes[i] + (1.0 - alpha) * out[i - 1];
    }
    out
}

/// MACD line (EMA(fast) − EMA(slow) on close) and signal (EMA of MACD line).
/// Returns `(prev_macd, prev_signal, curr_macd, curr_signal)` for the last two bars.
pub fn macd_line_and_signal_last_two(
    closes: &[f64],
    fast: usize,
    slow: usize,
    signal_p: usize,
) -> Option<(f64, f64, f64, f64)> {
    if fast < 2 || slow < 2 || signal_p < 2 || slow <= fast {
        return None;
    }
    let n = closes.len();
    if n < slow + signal_p {
        return None;
    }
    let ef = ema_series(closes, fast);
    let es = ema_series(closes, slow);
    let mut macd = vec![f64::NAN; n];
    for i in (slow - 1)..n {
        if ef[i].is_finite() && es[i].is_finite() {
            macd[i] = ef[i] - es[i];
        }
    }
    let start = slow - 1;
    let m_slice: Vec<f64> = macd[start..n].to_vec();
    let sig_local = ema_series(&m_slice, signal_p);
    let i_prev = n - 2;
    let i_curr = n - 1;
    let j_prev = i_prev.checked_sub(start)?;
    let j_curr = i_curr.checked_sub(start)?;
    if j_prev >= sig_local.len() || j_curr >= sig_local.len() {
        return None;
    }
    let prev_sig = sig_local[j_prev];
    let curr_sig = sig_local[j_curr];
    let prev_macd = macd[i_prev];
    let curr_macd = macd[i_curr];
    if !prev_sig.is_finite()
        || !curr_sig.is_finite()
        || !prev_macd.is_finite()
        || !curr_macd.is_finite()
    {
        return None;
    }
    Some((prev_macd, prev_sig, curr_macd, curr_sig))
}

pub fn wilder_rsi_at(closes: &[f64], period: usize, end: usize) -> Option<f64> {
    if end < period || period < 2 || closes.len() <= end {
        return None;
    }
    let mut avg_g = 0.0;
    let mut avg_l = 0.0;
    for i in 1..=period {
        let d = closes[i] - closes[i - 1];
        if d >= 0.0 {
            avg_g += d;
        } else {
            avg_l -= d;
        }
    }
    avg_g /= period as f64;
    avg_l /= period as f64;
    for i in (period + 1)..=end {
        let d = closes[i] - closes[i - 1];
        let g = d.max(0.0);
        let l = (-d).max(0.0);
        let pf = period as f64;
        avg_g = (avg_g * (pf - 1.0) + g) / pf;
        avg_l = (avg_l * (pf - 1.0) + l) / pf;
    }
    if avg_l < 1e-12 {
        return Some(100.0);
    }
    let rs = avg_g / avg_l;
    Some(100.0 - (100.0 / (1.0 + rs)))
}

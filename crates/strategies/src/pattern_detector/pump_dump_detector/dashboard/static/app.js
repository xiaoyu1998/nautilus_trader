(function () {
  const chartsEl = document.getElementById('charts');
  const statusEl = document.getElementById('status');
  const marketInput = document.getElementById('market');
  const chartById = {};

  function setStatus(t) {
    statusEl.textContent = t || '';
  }

  function formatUsdt(v) {
    if (v == null || !isFinite(Number(v))) return 'N/A';
    return Number(v).toLocaleString('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  function priceFormatFromRef(refPx) {
    const abs = Math.abs(Number(refPx));
    if (!isFinite(abs) || abs === 0) return { precision: 4, minMove: 0.0001 };
    const prec = abs >= 1000 ? 2 : abs >= 1 ? 4 : 6;
    return { precision: prec, minMove: Math.pow(10, -prec) };
  }

  function pad2(v) {
    return String(v).padStart(2, '0');
  }

  function formatHmFromUnixSec(sec) {
    const d = new Date(Number(sec) * 1000);
    return pad2(d.getUTCHours()) + ':' + pad2(d.getUTCMinutes());
  }

  function representativeRefPx(bars) {
    let m = 0;
    for (const b of bars || []) {
      for (const k of ['open', 'high', 'low', 'close']) {
        const x = Math.abs(Number(b[k]));
        if (isFinite(x) && x > m) m = x;
      }
    }
    return m > 0 ? m : 1;
  }

  function upsertPanel(panel) {
    const id = panel.instrument_id;
    let card = document.getElementById('card-' + id);
    if (!card) {
      card = document.createElement('div');
      card.className = 'chart-card';
      card.id = 'card-' + id;
      const h2 = document.createElement('h2');
      h2.textContent = id;
      const meta = document.createElement('div');
      meta.className = 'meta';
      meta.id = 'meta-' + id;
      const container = document.createElement('div');
      container.className = 'chart-container';
      container.id = 'chart-' + id;
      card.appendChild(h2);
      card.appendChild(meta);
      card.appendChild(container);
      chartsEl.appendChild(card);
    }

    const bars = (panel.bars || []).map((b) => ({
      time: Math.floor(Number(b.ts_event_ns) / 1e9),
      open: Number(b.open),
      high: Number(b.high),
      low: Number(b.low),
      close: Number(b.close),
      volume: Number(b.volume),
    }));
    bars.sort((a, b) => a.time - b.time);
    const refPx = representativeRefPx(bars);
    const fmt = priceFormatFromRef(refPx);

    if (!chartById[id]) {
      const chart = LightweightCharts.createChart(document.getElementById('chart-' + id), {
        layout: { background: { type: 'solid', color: '#0d1117' }, textColor: '#8b949e' },
        grid: { vertLines: { color: '#21262d' }, horzLines: { color: '#21262d' } },
        rightPriceScale: { borderColor: '#30363d' },
        localization: {
          timeFormatter: function (time) {
            const sec = typeof time === 'number'
              ? time
              : (time && typeof time.sec === 'number' ? time.sec : NaN);
            return isFinite(sec) ? formatHmFromUnixSec(sec) : '';
          },
        },
        timeScale: {
          rightOffset: 0,
          barSpacing: 6,
          minBarSpacing: 2,
          fixLeftEdge: true,
          tickMarkFormatter: function (time) {
            const sec = typeof time === 'number'
              ? time
              : (time && typeof time.sec === 'number' ? time.sec : NaN);
            return isFinite(sec) ? formatHmFromUnixSec(sec) : '';
          },
        },
      });
      chart._candles = chart.addCandlestickSeries({
        upColor: '#3fb950',
        downColor: '#f85149',
        borderVisible: false,
        wickUpColor: '#3fb950',
        wickDownColor: '#f85149',
        priceFormat: { type: 'price', precision: fmt.precision, minMove: fmt.minMove },
      });
      chart.priceScale('right').applyOptions({
        scaleMargins: { top: 0.05, bottom: 0.28 },
      });
      chart._volume = chart.addHistogramSeries({ priceScaleId: 'volume', color: '#8b949e' });
      chart.priceScale('volume').applyOptions({
        scaleMargins: { top: 0.78, bottom: 0.0 },
      });
      const container = document.getElementById('chart-' + id);
      container.style.position = 'relative';
      const tooltip = document.createElement('div');
      tooltip.className = 'chart-tooltip';
      tooltip.style.display = 'none';
      tooltip.style.position = 'absolute';
      tooltip.style.zIndex = '10';
      tooltip.style.padding = '6px 10px';
      tooltip.style.fontSize = '12px';
      tooltip.style.background = '#21262d';
      tooltip.style.border = '1px solid #30363d';
      tooltip.style.borderRadius = '4px';
      tooltip.style.pointerEvents = 'none';
      tooltip.style.whiteSpace = 'pre-line';
      container.appendChild(tooltip);
      chart._tooltip = tooltip;
      chartById[id] = chart;
    }

    const chart = chartById[id];
    chart._candles.applyOptions({
      priceFormat: { type: 'price', precision: fmt.precision, minMove: fmt.minMove },
    });
    chart._candles.setData(bars);
    chart._volume.setData(
      bars.map((b) => ({
        time: b.time,
        value: b.volume,
        color: b.close >= b.open ? '#3fb950' : '#f85149',
      }))
    );

    function nearestBarTime(tsNs) {
      if (!bars.length) return null;
      const target = Number(tsNs) / 1e9;
      let best = bars[0].time;
      let bestDiff = Math.abs(best - target);
      for (let i = 1; i < bars.length; i++) {
        const t = bars[i].time;
        const d = Math.abs(t - target);
        if (d < bestDiff) {
          best = t;
          bestDiff = d;
        }
      }
      return best;
    }

    const markers = [];
    for (const p of panel.patterns || []) {
      const mt = nearestBarTime(p.second_pivot_ts_event_ns);
      if (mt == null) continue;
      if (p.pattern_type === 'golden_cross') {
        markers.push({
          time: mt,
          position: 'belowBar',
          color: '#3fb950',
          shape: 'arrowUp',
          text: 'GC ' + (Number(p.strength || 0)).toFixed(4),
        });
      } else if (p.pattern_type === 'death_cross') {
        markers.push({
          time: mt,
          position: 'aboveBar',
          color: '#f85149',
          shape: 'arrowDown',
          text: 'DC ' + (Number(p.strength || 0)).toFixed(4),
        });
      }
    }
    markers.sort((a, b) => a.time - b.time);
    chart._candles.setMarkers(markers);
    chart._bars = bars;
    chart._patterns = panel.patterns || [];
    chart._fmt = fmt;

    if (!chart._tooltipBound) {
      chart.subscribeCrosshairMove(function (param) {
        const tt = chart._tooltip;
        if (!tt) return;
        if (!param || !param.point || !param.time) {
          tt.style.display = 'none';
          return;
        }
        const candle = param.seriesData && param.seriesData.get && param.seriesData.get(chart._candles);
        if (!candle || candle.open == null) {
          tt.style.display = 'none';
          return;
        }
        const t = typeof param.time === 'number'
          ? param.time
          : (param.time && typeof param.time.sec === 'number' ? param.time.sec : null);
        if (t == null) {
          tt.style.display = 'none';
          return;
        }
        const targetNs = Number(t) * 1e9;
        let nearestPattern = null;
        let nearestDist = Number.POSITIVE_INFINITY;
        for (const p of (chart._patterns || [])) {
          const d = Math.abs(Number(p.second_pivot_ts_event_ns) - targetNs);
          if (d < nearestDist) {
            nearestDist = d;
            nearestPattern = p;
          }
        }
        const pText = nearestPattern && nearestDist <= 60 * 1e9
          ? (nearestPattern.pattern_type + ' ' + Number(nearestPattern.strength || 0).toFixed(4))
          : 'none';
        const vol = (chart._bars || []).find((b) => b.time === t)?.volume;
        const volUsdt = (vol != null && isFinite(Number(vol)))
          ? Number(vol) * Number(candle.close)
          : null;
        const dt = new Date(Number(t) * 1000);
        const hh = String(dt.getUTCHours()).padStart(2, '0');
        const mm = String(dt.getUTCMinutes()).padStart(2, '0');
        const ss = String(dt.getUTCSeconds()).padStart(2, '0');
        const prec = (chart._fmt && chart._fmt.precision) || 4;
        tt.textContent =
          `Time: ${hh}:${mm}:${ss}\n` +
          `O: ${Number(candle.open).toFixed(prec)}  H: ${Number(candle.high).toFixed(prec)}\n` +
          `L: ${Number(candle.low).toFixed(prec)}  C: ${Number(candle.close).toFixed(prec)}\n` +
          `Volume (USDT): ${formatUsdt(volUsdt)}\n` +
          `Pattern: ${pText}`;
        tt.style.left = (param.point.x + 12) + 'px';
        tt.style.top = (param.point.y + 12) + 'px';
        tt.style.display = 'block';
      });
      chart._tooltipBound = true;
    }

    const metaEl = document.getElementById('meta-' + id);
    const totalPatterns = (panel.patterns || []).length;
    const lastPattern = totalPatterns > 0 ? panel.patterns[totalPatterns - 1] : null;
    metaEl.textContent = 'bars=' + bars.length + ' | patterns=' + totalPatterns + (lastPattern ? (' | last=' + lastPattern.pattern_type + ' strength=' + Number(lastPattern.strength).toFixed(4)) : '');
  }

  async function refresh() {
    try {
      const r = await fetch('/api/markets');
      const panels = await r.json();
      panels.forEach(upsertPanel);
      setStatus('last update ' + new Date().toLocaleTimeString());
    } catch (_) {
      setStatus('fetch error');
    }
  }

  document.getElementById('btnAdd').addEventListener('click', async function () {
    const m = (marketInput.value || '').trim();
    if (!m) return;
    try {
      const r = await fetch('/api/pool', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ market: m }),
      });
      const j = await r.json();
      setStatus(j.ok ? 'added ' + j.instrument_id : (j.error || 'add failed'));
      await refresh();
    } catch (_) {
      setStatus('add error');
    }
  });

  document.getElementById('btnRemove').addEventListener('click', async function () {
    const m = (marketInput.value || '').trim();
    if (!m) return;
    try {
      const r = await fetch('/api/pool/remove', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ market: m }),
      });
      const j = await r.json();
      setStatus(j.ok ? 'removed ' + j.instrument_id : (j.error || 'remove failed'));
      if (j.ok && j.instrument_id) {
        const el = document.getElementById('card-' + j.instrument_id);
        if (el) el.remove();
        if (chartById[j.instrument_id]) {
          try { chartById[j.instrument_id].remove(); } catch (_) {}
          delete chartById[j.instrument_id];
        }
      }
      await refresh();
    } catch (_) {
      setStatus('remove error');
    }
  });

  setInterval(refresh, 2000);
  refresh();
})();

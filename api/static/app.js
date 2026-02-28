const symbolInput = document.getElementById('symbol');
const quoteBtn = document.getElementById('quoteBtn');
const signalBtn = document.getElementById('signalBtn');

const quoteSymbol = document.getElementById('quoteSymbol');
const quoteLast = document.getElementById('quoteLast');
const quoteTs = document.getElementById('quoteTs');
const quoteProvider = document.getElementById('quoteProvider');
const quoteRaw = document.getElementById('quoteRaw');

const signalScore = document.getElementById('signalScore');
const signalTrend = document.getElementById('signalTrend');
const signalMomentum = document.getElementById('signalMomentum');
const signalConfidence = document.getElementById('signalConfidence');
const signalConfidenceBar = document.getElementById('signalConfidenceBar');
const signalDebug = document.getElementById('signalDebug');

const chartCanvas = document.getElementById('priceChart');
const chartMeta = document.getElementById('chartMeta');
let priceChart = null;

function getSymbol() {
  const value = (symbolInput.value || '').trim().toUpperCase();
  return value || 'AAPL';
}

async function fetchJson(url) {
  const response = await fetch(url);
  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { error: 'Invalid JSON response', raw: text };
  }
  return { ok: response.ok, status: response.status, body: data };
}

function asJson(payload) {
  return JSON.stringify(payload, null, 2);
}

function resetClass(el, base, variant) {
  el.className = `${base} ${variant}`.trim();
}

function sentimentClass(value) {
  const v = (value || '').toString().toLowerCase();
  if (v.includes('bullish') || v.includes('positive')) return 'bull';
  if (v.includes('bearish') || v.includes('negative')) return 'bear';
  return 'neutral';
}

function computeMovingAverage(values, window) {
  const out = [];
  for (let i = 0; i < values.length; i += 1) {
    if (i + 1 < window) {
      out.push(null);
      continue;
    }
    const slice = values.slice(i + 1 - window, i + 1);
    const sum = slice.reduce((acc, val) => acc + val, 0);
    out.push(Number((sum / window).toFixed(4)));
  }
  return out;
}

function sortBarsAscending(bars) {
  return [...bars].sort((a, b) => {
    const ta = Date.parse(a.ts_event || a.ts_ingest || '') || 0;
    const tb = Date.parse(b.ts_event || b.ts_ingest || '') || 0;
    return ta - tb;
  });
}

function renderChart(symbol, bars) {
  if (!chartCanvas) return;

  if (!bars || bars.length === 0) {
    if (priceChart) {
      priceChart.destroy();
      priceChart = null;
    }
    chartMeta.textContent = `${symbol}: no bar data`;
    return;
  }

  const ordered = sortBarsAscending(bars);
  const labels = ordered.map((b) => (b.ts_event || b.ts_ingest || '').slice(0, 10));
  const closes = ordered.map((b) => Number(b.close || 0));
  const ma10 = computeMovingAverage(closes, 10);
  const ma20 = computeMovingAverage(closes, 20);

  const data = {
    labels,
    datasets: [
      {
        label: 'Close',
        data: closes,
        borderColor: '#60a5fa',
        backgroundColor: 'rgba(96,165,250,0.15)',
        borderWidth: 2,
        tension: 0.2,
        pointRadius: 0,
      },
      {
        label: 'MA10',
        data: ma10,
        borderColor: '#22c55e',
        borderWidth: 1.8,
        tension: 0.2,
        pointRadius: 0,
      },
      {
        label: 'MA20',
        data: ma20,
        borderColor: '#ef4444',
        borderWidth: 1.8,
        tension: 0.2,
        pointRadius: 0,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        labels: {
          color: '#e6edf3',
        },
      },
    },
    scales: {
      x: {
        ticks: { color: '#9aa6b2', maxTicksLimit: 8 },
        grid: { color: 'rgba(154,166,178,0.15)' },
      },
      y: {
        ticks: { color: '#9aa6b2' },
        grid: { color: 'rgba(154,166,178,0.15)' },
      },
    },
  };

  if (priceChart) {
    priceChart.destroy();
  }
  priceChart = new Chart(chartCanvas, { type: 'line', data, options });

  chartMeta.textContent = `${symbol} • ${ordered.length} bars • Close / MA10 / MA20`;
}

async function loadBarsChart(symbol) {
  chartMeta.textContent = `${symbol}: loading bars...`;
  const result = await fetchJson(
    `/provider/twelvedata/bars?symbol=${encodeURIComponent(symbol)}&interval=1day&outputsize=60`
  );
  if (!result.ok) {
    renderChart(symbol, []);
    return;
  }
  const bars = (result.body && result.body.bars) || [];
  renderChart(symbol, bars);
}

function renderQuote(result, requestedSymbol) {
  const body = result.body || {};
  const quote = body.quote || {};

  if (!result.ok) {
    quoteSymbol.textContent = requestedSymbol;
    quoteLast.textContent = 'Error';
    quoteTs.textContent = '-';
    quoteProvider.textContent = body.provider || 'twelvedata';
    quoteRaw.textContent = asJson(result);
    return;
  }

  quoteSymbol.textContent = body.symbol || requestedSymbol || quote.instrument_id || '-';
  quoteLast.textContent = quote.last != null ? String(quote.last) : '-';
  quoteTs.textContent = quote.ts_event || quote.ts_ingest || '-';
  quoteProvider.textContent = quote.source_provider || body.provider || 'twelvedata';
  quoteRaw.textContent = asJson(result);
}

function renderSignal(result) {
  const body = result.body || {};

  if (!result.ok || body.error) {
    signalScore.textContent = 'ERR';
    signalTrend.textContent = 'trend: -';
    signalMomentum.textContent = 'momentum: -';
    resetClass(signalTrend, 'badge', 'neutral');
    resetClass(signalMomentum, 'badge', 'neutral');
    signalConfidence.textContent = '0%';
    signalConfidenceBar.style.width = '0%';
    signalDebug.textContent = asJson(result);
    return;
  }

  signalScore.textContent = body.score != null ? String(body.score) : '-';

  const trendText = body.trend || 'neutral';
  const momentumText = body.momentum || 'neutral';
  signalTrend.textContent = `trend: ${trendText}`;
  signalMomentum.textContent = `momentum: ${momentumText}`;
  resetClass(signalTrend, 'badge', sentimentClass(trendText));
  resetClass(signalMomentum, 'badge', sentimentClass(momentumText));

  const conf = Math.max(0, Math.min(1, Number(body.confidence || 0)));
  signalConfidence.textContent = `${Math.round(conf * 100)}%`;
  signalConfidenceBar.style.width = `${Math.round(conf * 100)}%`;

  signalDebug.textContent = asJson(body.debug || {});
}

quoteBtn.addEventListener('click', async () => {
  const symbol = getSymbol();
  quoteSymbol.textContent = symbol;
  quoteLast.textContent = 'Loading...';
  quoteTs.textContent = '-';
  quoteProvider.textContent = 'twelvedata';
  quoteRaw.textContent = '{}';

  const result = await fetchJson(`/provider/twelvedata/quote?symbol=${encodeURIComponent(symbol)}`);
  renderQuote(result, symbol);
  await loadBarsChart(symbol);
});

signalBtn.addEventListener('click', async () => {
  const symbol = getSymbol();
  signalScore.textContent = '...';
  signalTrend.textContent = 'trend: -';
  signalMomentum.textContent = 'momentum: -';
  resetClass(signalTrend, 'badge', 'neutral');
  resetClass(signalMomentum, 'badge', 'neutral');
  signalConfidence.textContent = '0%';
  signalConfidenceBar.style.width = '0%';
  signalDebug.textContent = '{}';

  const result = await fetchJson(`/signal/basic?symbol=${encodeURIComponent(symbol)}`);
  renderSignal(result);
  await loadBarsChart(symbol);
});

window.addEventListener('DOMContentLoaded', async () => {
  await loadBarsChart(getSymbol());
});

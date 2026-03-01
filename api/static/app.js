const SCANNER_SYMBOLS = [
  'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AVGO', 'AMD', 'NFLX',
  'CRM', 'ORCL', 'INTC', 'ADBE', 'QCOM', 'SHOP', 'PLTR', 'UBER', 'COIN', 'PANW',
  'SNOW', 'MU', 'CRWD', 'ASML', 'TSM', 'PYPL', 'ABNB', 'DIS', 'JPM', 'V'
];

const PORTFOLIO = [
  { symbol: 'AAPL', qty: 10 },
  { symbol: 'TSLA', qty: 5 },
  { symbol: 'MSFT', qty: 8 },
];

const symbolInput = document.getElementById('symbol');
const quoteBtn = document.getElementById('quoteBtn');
const signalBtn = document.getElementById('signalBtn');

const scannerList = document.getElementById('scannerList');
const scannerToggleBtn = document.getElementById('scannerToggleBtn');
const watchlistInput = document.getElementById('watchlistInput');
const watchlistAddBtn = document.getElementById('watchlistAddBtn');
const watchlistSort = document.getElementById('watchlistSort');
const watchlistList = document.getElementById('watchlistList');
const portfolioList = document.getElementById('portfolioList');

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

const dataCache = new Map();
const inFlight = new Map();

const state = {
  scannerExpanded: false,
  selectedSymbol: 'AAPL',
  watchlist: loadWatchlist(),
  watchlistSort: 'symbol',
  expandedByPanel: {
    scanner: null,
    watchlist: null,
    portfolio: null,
  },
};

function loadWatchlist() {
  try {
    const raw = localStorage.getItem('apollo67.watchlist');
    if (!raw) return ['AAPL', 'MSFT', 'NVDA'];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return ['AAPL', 'MSFT', 'NVDA'];
    return [...new Set(parsed.map(normalizeSymbol).filter(Boolean))];
  } catch {
    return ['AAPL', 'MSFT', 'NVDA'];
  }
}

function saveWatchlist() {
  localStorage.setItem('apollo67.watchlist', JSON.stringify(state.watchlist));
}

function normalizeSymbol(value) {
  return (value || '').toString().trim().toUpperCase();
}

function getSymbol() {
  const value = normalizeSymbol(symbolInput.value);
  return value || state.selectedSymbol || 'AAPL';
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

function getQuoteView(symbol, result) {
  const body = result?.body || {};
  const quote = body.quote || {};
  return {
    symbol: body.symbol || quote.instrument_id || symbol,
    last: quote.last != null ? Number(quote.last) : null,
    ts: quote.ts_event || quote.ts_ingest || null,
    provider: quote.source_provider || body.provider || 'twelvedata',
    raw: result || {},
  };
}

function getSignalView(result) {
  const body = result?.body || {};
  return {
    score: body.score != null ? Number(body.score) : null,
    trend: body.trend || 'neutral',
    momentum: body.momentum || 'neutral',
    confidence: body.confidence != null ? Number(body.confidence) : 0,
    debug: body.debug || {},
    raw: result || {},
    error: !result?.ok || !!body.error,
  };
}

async function fetchSymbolData(symbol, force = false) {
  const key = normalizeSymbol(symbol);
  if (!key) return null;

  if (!force && dataCache.has(key)) {
    return dataCache.get(key);
  }

  if (inFlight.has(key)) {
    return inFlight.get(key);
  }

  const pending = (async () => {
    const [quoteResult, signalResult] = await Promise.all([
      fetchJson(`/provider/twelvedata/quote?symbol=${encodeURIComponent(key)}`),
      fetchJson(`/signal/basic?symbol=${encodeURIComponent(key)}`),
    ]);

    const entry = { quoteResult, signalResult, fetchedAt: Date.now() };
    dataCache.set(key, entry);
    return entry;
  })();

  inFlight.set(key, pending);
  try {
    return await pending;
  } finally {
    inFlight.delete(key);
  }
}

function warmSymbols(symbols) {
  const missing = [...new Set(symbols.map(normalizeSymbol).filter(Boolean))].filter(
    (symbol) => !dataCache.has(symbol) && !inFlight.has(symbol)
  );

  if (missing.length === 0) return;

  Promise.allSettled(missing.map((symbol) => fetchSymbolData(symbol))).then(() => {
    renderPanels();
  });
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

function formatPrice(value) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  return Number(value).toFixed(2);
}

function formatScore(value) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  return String(Math.round(Number(value)));
}

function sortByScoreDesc(a, b) {
  const as = a.signal.score == null ? -9999 : Number(a.signal.score);
  const bs = b.signal.score == null ? -9999 : Number(b.signal.score);
  if (bs !== as) return bs - as;
  return a.symbol.localeCompare(b.symbol);
}

function sortWatchlistRows(rows) {
  const mode = state.watchlistSort;
  if (mode === 'price') {
    return rows.sort((a, b) => (b.quote.last || -1) - (a.quote.last || -1));
  }
  if (mode === 'score') {
    return rows.sort((a, b) => (b.signal.score || -9999) - (a.signal.score || -9999));
  }
  return rows.sort((a, b) => a.symbol.localeCompare(b.symbol));
}

function getPanelRows(symbols, panelName) {
  return symbols.map((symbol) => {
    const cached = dataCache.get(symbol);
    const quote = getQuoteView(symbol, cached?.quoteResult);
    const signal = getSignalView(cached?.signalResult);
    const isSelected = state.selectedSymbol === symbol;
    const isExpanded = state.expandedByPanel[panelName] === symbol;
    return { symbol, quote, signal, isSelected, isExpanded };
  });
}

function renderCardDetails(row) {
  const trendClass = sentimentClass(row.signal.trend);
  const momentumClass = sentimentClass(row.signal.momentum);
  const debug = row.signal.debug || {};

  return `
    <div class="expand-content">
      <div class="expand-grid">
        <div>
          <div class="label">Quote summary</div>
          <div class="expand-line">last: ${formatPrice(row.quote.last)}</div>
          <div class="expand-line">ts_event: ${row.quote.ts || '-'}</div>
          <div class="expand-line">provider: ${row.quote.provider || '-'}</div>
        </div>
        <div>
          <div class="label">Signal summary</div>
          <div class="expand-line">score: ${formatScore(row.signal.score)}</div>
          <div class="expand-line">confidence: ${Math.round((row.signal.confidence || 0) * 100)}%</div>
          <div class="expand-line">ma10: ${debug.ma10 != null ? debug.ma10 : '-'}</div>
          <div class="expand-line">ma20: ${debug.ma20 != null ? debug.ma20 : '-'}</div>
          <div class="expand-line">rsi14: ${debug.rsi14 != null ? debug.rsi14 : '-'}</div>
        </div>
      </div>
      <details class="details details-inline">
        <summary>Raw JSON</summary>
        <pre>${asJson({ quote: row.quote.raw, signal: row.signal.raw })}</pre>
      </details>
    </div>
  `;
}

function renderSymbolList(container, rows, panelName, options = {}) {
  const showQty = Boolean(options.showQty);
  if (!rows.length) {
    container.innerHTML = '<div class="empty">No symbols.</div>';
    return;
  }

  container.innerHTML = rows
    .map((row) => {
      const trendClass = sentimentClass(row.signal.trend);
      const momentumClass = sentimentClass(row.signal.momentum);
      const qty = showQty ? `<span class="pill">qty ${options.qtyBySymbol[row.symbol] || 0}</span>` : '';
      const pl = showQty ? '<span class="pill muted-pill">P/L --</span>' : '';
      return `
      <article class="symbol-card ${row.isSelected ? 'selected' : ''}" data-panel="${panelName}" data-symbol="${row.symbol}">
        <button type="button" class="symbol-main" data-action="select" data-panel="${panelName}" data-symbol="${row.symbol}">
          <span class="sym">${row.symbol}</span>
          <span class="metric">$${formatPrice(row.quote.last)}</span>
          <span class="metric">score ${formatScore(row.signal.score)}</span>
          <span class="badge ${trendClass}">${row.signal.trend}</span>
          <span class="badge ${momentumClass}">${row.signal.momentum}</span>
          ${qty}
          ${pl}
        </button>
        ${row.isExpanded ? renderCardDetails(row) : ''}
      </article>
      `;
    })
    .join('');
}

function renderScanner() {
  const list = state.scannerExpanded ? SCANNER_SYMBOLS : SCANNER_SYMBOLS.slice(0, 15);
  scannerToggleBtn.textContent = state.scannerExpanded ? 'Show less' : 'Show more';

  const rows = getPanelRows(list, 'scanner').sort(sortByScoreDesc);
  renderSymbolList(scannerList, rows, 'scanner');
  warmSymbols(list);
}

function renderWatchlist() {
  const rows = sortWatchlistRows(getPanelRows(state.watchlist, 'watchlist'));
  renderSymbolList(watchlistList, rows, 'watchlist');
  warmSymbols(state.watchlist);
}

function renderPortfolio() {
  const symbols = PORTFOLIO.map((h) => h.symbol);
  const qtyBySymbol = Object.fromEntries(PORTFOLIO.map((h) => [h.symbol, h.qty]));
  const rows = getPanelRows(symbols, 'portfolio').sort(sortByScoreDesc);
  renderSymbolList(portfolioList, rows, 'portfolio', { showQty: true, qtyBySymbol });
  warmSymbols(symbols);
}

function renderPanels() {
  renderScanner();
  renderWatchlist();
  renderPortfolio();
}

async function selectSymbol(symbol, { force = false } = {}) {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) return;

  state.selectedSymbol = normalized;
  symbolInput.value = normalized;

  const data = await fetchSymbolData(normalized, force);
  if (!data) return;

  renderQuote(data.quoteResult, normalized);
  renderSignal(data.signalResult);
  await loadBarsChart(normalized);
  renderPanels();
}

scannerToggleBtn.addEventListener('click', () => {
  state.scannerExpanded = !state.scannerExpanded;
  renderScanner();
});

watchlistSort.addEventListener('change', () => {
  state.watchlistSort = watchlistSort.value;
  renderWatchlist();
});

watchlistAddBtn.addEventListener('click', () => {
  const symbol = normalizeSymbol(watchlistInput.value);
  if (!symbol) return;
  if (!state.watchlist.includes(symbol)) {
    state.watchlist.push(symbol);
    saveWatchlist();
  }
  watchlistInput.value = '';
  renderWatchlist();
});

watchlistInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    watchlistAddBtn.click();
  }
});

document.addEventListener('click', (event) => {
  const target = event.target.closest('[data-action="select"]');
  if (!target) return;

  const panel = target.dataset.panel;
  const symbol = normalizeSymbol(target.dataset.symbol);
  if (!panel || !symbol) return;

  state.expandedByPanel[panel] = state.expandedByPanel[panel] === symbol ? null : symbol;
  selectSymbol(symbol).catch(() => {
    renderPanels();
  });
});

quoteBtn.addEventListener('click', async () => {
  const symbol = getSymbol();
  await selectSymbol(symbol, { force: true });
});

signalBtn.addEventListener('click', async () => {
  const symbol = getSymbol();
  await selectSymbol(symbol, { force: true });
});

window.addEventListener('DOMContentLoaded', async () => {
  const initial = getSymbol();
  state.selectedSymbol = initial;
  renderPanels();
  await selectSymbol(initial);
});

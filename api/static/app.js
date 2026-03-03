const SCANNER_SYMBOLS = [
  'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AVGO', 'AMD', 'NFLX',
  'CRM', 'ORCL', 'INTC', 'ADBE', 'QCOM', 'SHOP', 'PLTR', 'UBER', 'COIN', 'PANW',
  'SNOW', 'MU', 'CRWD', 'ASML', 'TSM', 'PYPL', 'ABNB', 'DIS', 'JPM', 'V'
];

const symbolInput = document.getElementById('symbol');
const quoteBtn = document.getElementById('quoteBtn');
const signalBtn = document.getElementById('signalBtn');

// New UI bits (safe if missing)
const tradeBtn = document.getElementById('tradeBtn');
const intervalSelect = document.getElementById('interval');
const outputsizeInput = document.getElementById('outputsize');

const scannerList = document.getElementById('scannerList');
const scannerToggleBtn = document.getElementById('scannerToggleBtn');
const watchlistInput = document.getElementById('watchlistInput');
const watchlistAddBtn = document.getElementById('watchlistAddBtn');
const watchlistSort = document.getElementById('watchlistSort');
const watchlistList = document.getElementById('watchlistList');
const monitorRefreshBtn = document.getElementById('monitorRefreshBtn');
const monitorTotals = document.getElementById('monitorTotals');
const monitorList = document.getElementById('monitorList');
const portfolioAddBtn = document.getElementById('portfolioAddBtn');
const portfolioList = document.getElementById('portfolioList');
const scannerLoading = document.getElementById('scannerLoading');
const watchlistLoading = document.getElementById('watchlistLoading');
const monitorLoading = document.getElementById('monitorLoading');
const portfolioLoading = document.getElementById('portfolioLoading');

const quoteSymbol = document.getElementById('quoteSymbol');
const quoteLast = document.getElementById('quoteLast');
const quoteTs = document.getElementById('quoteTs');
const quoteProvider = document.getElementById('quoteProvider');
const quoteRaw = document.getElementById('quoteRaw');
const quoteError = document.getElementById('quoteError');

const signalScore = document.getElementById('signalScore');
const signalTrend = document.getElementById('signalTrend');
const signalMomentum = document.getElementById('signalMomentum');
const signalConfidence = document.getElementById('signalConfidence');
const signalConfidenceBar = document.getElementById('signalConfidenceBar');
const signalDebug = document.getElementById('signalDebug');
const signalError = document.getElementById('signalError');

// Trade card elements (safe if missing)
const tradeError = document.getElementById('tradeError');
const tradeAction = document.getElementById('tradeAction');
const tradeConfidence = document.getElementById('tradeConfidence');
const tradeTimeframe = document.getElementById('tradeTimeframe');
const tradeEntryZone = document.getElementById('tradeEntryZone');
const tradeTarget = document.getElementById('tradeTarget');
const tradeStop = document.getElementById('tradeStop');
const tradeTrail = document.getElementById('tradeTrail');
const tradeReasons = document.getElementById('tradeReasons');
const tradeRaw = document.getElementById('tradeRaw');
const backtestBtn = document.getElementById('backtestBtn');
const addMonitorBtn = document.getElementById('addMonitorBtn');
const btWinRate = document.getElementById('btWinRate');
const btTrades = document.getElementById('btTrades');
const btReturn = document.getElementById('btReturn');
const btMaxDd = document.getElementById('btMaxDd');

const chartCanvas = document.getElementById('priceChart');
const chartMeta = document.getElementById('chartMeta');
const chartOhlc = document.getElementById('chartOhlc');
let priceChart = null;

const dataCache = new Map();
const inFlight = new Map();
const scannerTradeCache = new Map();
const scannerTradeInFlight = new Map();
const scannerTradeQueue = [];
let scannerTradeActive = 0;
const SCANNER_TRADE_CACHE_TTL_MS = 60 * 1000;
const SCANNER_MAX_INFLIGHT = 4;
const MONITOR_STORAGE_KEY = 'apollo67_monitor_v1';
const MONITOR_QUOTE_CACHE_TTL_MS = 20 * 1000;
const MONITOR_MAX_INFLIGHT = 4;
const monitorQuoteCache = new Map();
const monitorQuoteInFlight = new Map();
const monitorQuoteQueue = [];
let monitorQuoteActive = 0;

const state = {
  scannerExpanded: false,
  scannerRows: [],
  scannerMode: 'buy',
  selectedSymbol: 'AAPL',
  watchlist: loadWatchlist(),
  watchlistSort: 'symbol',
  monitor: loadMonitor(),
  monitorLastPrice: {},
  monitorStale: {},
  monitorLastRefreshMs: 0,
  portfolio: loadPortfolio(),
  expandedByPanel: {
    scanner: null,
    watchlist: null,
    monitor: null,
    portfolio: null,
  },
  sectionLoading: {
    scanner: false,
    watchlist: false,
    monitor: false,
    portfolio: false,
  },
  scannerNeedsUpdate: false,
  scannerStatusCheckInFlight: false,
  scannerLastStatusCheckMs: 0,
  loadingByPanel: {
    scanner: new Set(),
    watchlist: new Set(),
    monitor: new Set(),
    portfolio: new Set(),
  },
  latestTrade: null,
  latestBars: [],
  hoveredChartIndex: null,
};

const chartOverlayPlugin = {
  id: 'chartOverlayPlugin',
  afterDatasetsDraw(chart, args, pluginOptions) {
    const { ctx, chartArea, scales } = chart;
    if (!chartArea || !scales?.y || !scales?.x) return;

    ctx.save();

    const hoverIndex = pluginOptions?.hoveredIndex;
    if (Number.isInteger(hoverIndex)) {
      const x = scales.x.getPixelForValue(hoverIndex);
      if (Number.isFinite(x)) {
        ctx.strokeStyle = 'rgba(148, 163, 184, 0.5)';
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.beginPath();
        ctx.moveTo(x, chartArea.top);
        ctx.lineTo(x, chartArea.bottom);
        ctx.stroke();
        ctx.setLineDash([]);
      }
    }

    ctx.restore();
  },
};

if (window.Chart) {
  Chart.register(chartOverlayPlugin);
  const zoomPlugin = window.ChartZoom || window['chartjs-plugin-zoom'];
  if (zoomPlugin) {
    Chart.register(zoomPlugin);
  }
}

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

function loadPortfolio() {
  try {
    const raw = localStorage.getItem('apollo_portfolio');
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];

    return parsed
      .map((entry) => {
        const symbol = normalizeSymbol(entry?.symbol);
        const qtyValue = Number(entry?.qty);
        const avgValue = Number(entry?.avg_cost);
        return {
          symbol,
          qty: Number.isFinite(qtyValue) ? qtyValue : null,
          avg_cost: Number.isFinite(avgValue) ? avgValue : null,
        };
      })
      .filter((entry) => entry.symbol);
  } catch {
    return [];
  }
}

function savePortfolio() {
  localStorage.setItem('apollo_portfolio', JSON.stringify(state.portfolio));
}

function loadMonitor() {
  try {
    const raw = localStorage.getItem(MONITOR_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => {
        const symbol = normalizeSymbol(item?.symbol);
        const entryPrice = Number(item?.entry_price);
        const amount = Number(item?.amount);
        const shares = Number(item?.shares);
        const createdAt = String(item?.created_at || '');
        if (!symbol || !Number.isFinite(entryPrice) || entryPrice <= 0 || !Number.isFinite(amount) || amount <= 0) {
          return null;
        }
        return {
          id: String(item?.id || `${Date.now()}_${symbol}`),
          symbol,
          entry_price: entryPrice,
          amount,
          shares: Number.isFinite(shares) && shares > 0 ? shares : amount / entryPrice,
          created_at: createdAt || new Date().toISOString(),
          notes: item?.notes ? String(item.notes) : '',
        };
      })
      .filter(Boolean);
  } catch {
    return [];
  }
}

function saveMonitor() {
  localStorage.setItem(MONITOR_STORAGE_KEY, JSON.stringify(state.monitor));
}

function makeMonitorId(symbol) {
  return `${Date.now()}_${Math.random().toString(36).slice(2, 8)}_${symbol}`;
}

function normalizeSymbol(value) {
  return (value || '').toString().trim().toUpperCase();
}

function getSymbol() {
  const value = normalizeSymbol(symbolInput.value);
  return value || state.selectedSymbol || 'AAPL';
}

function getInterval() {
  const raw = intervalSelect ? String(intervalSelect.value || '').trim() : '';
  return raw || '1day';
}

function getOutputsize() {
  if (!outputsizeInput) return 60;
  const n = Number(outputsizeInput.value);
  if (!Number.isFinite(n)) return 60;
  return Math.max(20, Math.min(500, Math.floor(n)));
}

function chunkSymbols(symbols, size) {
  const out = [];
  for (let i = 0; i < symbols.length; i += size) {
    out.push(symbols.slice(i, i + size));
  }
  return out;
}

function setSectionLoading(panelName, isLoading) {
  state.sectionLoading[panelName] = isLoading;
  let el = null;
  if (panelName === 'scanner') {
    el = scannerLoading;
    if (!el) {
      const scannerTitle = document.querySelector('.left-panels article:first-child .panel-head h2');
      if (scannerTitle) {
        el = scannerTitle.querySelector('[data-loading-fallback="scanner"]');
        if (!el) {
          el = document.createElement('span');
          el.className = 'loading-indicator';
          el.dataset.loadingFallback = 'scanner';
          el.textContent = 'Loading...';
          el.hidden = true;
          scannerTitle.appendChild(document.createTextNode(' '));
          scannerTitle.appendChild(el);
        }
      }
    }
  } else if (panelName === 'watchlist') {
    el = watchlistLoading;
  } else if (panelName === 'monitor') {
    el = monitorLoading;
  } else if (panelName === 'portfolio') {
    el = portfolioLoading;
  }
  if (!el) return;
  if (panelName === 'scanner') {
    renderScannerIndicator();
    return;
  }
  el.hidden = !isLoading;
}

function renderScannerIndicator() {
  if (!scannerLoading) return;
  if (state.sectionLoading.scanner) {
    scannerLoading.textContent = 'Loading...';
    scannerLoading.hidden = false;
    return;
  }
  if (state.scannerNeedsUpdate) {
    scannerLoading.textContent = 'Updating...';
    scannerLoading.hidden = false;
    return;
  }
  scannerLoading.hidden = true;
}

function scannerScoreText(score) {
  const num = Number(score);
  return Number.isFinite(num) ? String(Math.round(num)) : '-';
}

function scannerPriceText(value) {
  const num = Number(value);
  return Number.isFinite(num) ? `$${formatPrice(num)}` : '-';
}

function initScannerModeToggle() {
  const root = document.querySelector('.scanner-toggle');
  if (!root) return;

  root.addEventListener('click', (e) => {
    const btn = e.target && e.target.closest ? e.target.closest('button[data-mode]') : null;
    if (!btn) return;
    const mode = btn.getAttribute('data-mode') || 'buy';
    state.scannerMode = mode === 'watch' ? 'watch' : 'buy';
    root.querySelectorAll('button[data-mode]').forEach((b) => b.classList.toggle('is-active', b === btn));
    renderScanner();
  });
}

function _queueScannerTradeTask(task) {
  return new Promise((resolve, reject) => {
    scannerTradeQueue.push({ task, resolve, reject });
    _runScannerTradeQueue();
  });
}

function _runScannerTradeQueue() {
  while (scannerTradeActive < SCANNER_MAX_INFLIGHT && scannerTradeQueue.length) {
    const next = scannerTradeQueue.shift();
    if (!next) return;
    scannerTradeActive += 1;
    Promise.resolve()
      .then(next.task)
      .then(next.resolve)
      .catch(next.reject)
      .finally(() => {
        scannerTradeActive = Math.max(0, scannerTradeActive - 1);
        _runScannerTradeQueue();
      });
  }
}

function _distanceToEntryZone(price, low, high) {
  if (!Number.isFinite(price) || !Number.isFinite(low) || !Number.isFinite(high)) return Number.POSITIVE_INFINITY;
  if (price < low) return low - price;
  if (price > high) return price - high;
  return 0;
}

function _quoteFallbackPrice(symbol) {
  const cached = dataCache.get(normalizeSymbol(symbol));
  const quoteBody = cached?.quoteResult?.body || {};
  const quote = quoteBody.quote || {};
  const n = Number(quote.last);
  return Number.isFinite(n) ? n : null;
}

function _buildScannerRowFromTrade(symbol, tradePayload) {
  const trade = tradePayload && typeof tradePayload === 'object' ? tradePayload : {};
  const action = String(trade.action || '').toUpperCase();
  const confidenceRaw = Number(trade.confidence);
  const confidence = Number.isFinite(confidenceRaw) ? confidenceRaw : 0;
  const priceRaw = Number(trade.last_close);
  const price = Number.isFinite(priceRaw) ? priceRaw : _quoteFallbackPrice(symbol);
  const rrRaw = Number(trade.risk_reward_ratio);
  const rr = Number.isFinite(rrRaw) ? rrRaw : null;
  const atrRaw = Number(trade?.indicators?.atr14);
  const atr = Number.isFinite(atrRaw) ? atrRaw : null;
  const entryLow = Number(trade?.entry_zone?.low);
  const entryHigh = Number(trade?.entry_zone?.high);

  let near = false;
  let distanceToEntry = Number.POSITIVE_INFINITY;
  if (Number.isFinite(price) && Number.isFinite(entryLow) && Number.isFinite(entryHigh)) {
    const nearPad = atr != null ? Math.max(0.0025 * price, 0.25 * atr) : 0.005 * price;
    near = price >= (entryLow - nearPad) && price <= (entryHigh + nearPad);
    distanceToEntry = _distanceToEntryZone(price, entryLow, entryHigh);
  }

  return {
    symbol: normalizeSymbol(symbol),
    action,
    confidence,
    price,
    rr,
    timeframe: trade.timeframe || '1day',
    entryLow: Number.isFinite(entryLow) ? entryLow : null,
    entryHigh: Number.isFinite(entryHigh) ? entryHigh : null,
    target: Number.isFinite(Number(trade.target_sell_price)) ? Number(trade.target_sell_price) : null,
    stop: Number.isFinite(Number(trade.stop_loss_price)) ? Number(trade.stop_loss_price) : null,
    trail: Number.isFinite(Number(trade.trailing_stop_price)) ? Number(trade.trailing_stop_price) : null,
    reasons: Array.isArray(trade.reasons) ? trade.reasons.slice(0, 2) : [],
    nearEntry: near,
    distanceToEntry,
  };
}

async function refreshScannerData() {
  setSectionLoading('scanner', true);
  const symbols = SCANNER_SYMBOLS.slice(0, state.scannerExpanded ? SCANNER_SYMBOLS.length : 15);
  const rows = [];

  await Promise.allSettled(
    symbols.map((symbol) =>
      fetchTradeForSymbol(symbol).then((tradePayload) => {
        if (!tradePayload) return;
        rows.push(_buildScannerRowFromTrade(symbol, tradePayload));
      })
    )
  );

  const buyRows = rows
    .filter((row) => row.action === 'BUY')
    .sort((a, b) => {
      if (b.confidence !== a.confidence) return b.confidence - a.confidence;
      const arr = a.rr == null ? Number.NEGATIVE_INFINITY : a.rr;
      const brr = b.rr == null ? Number.NEGATIVE_INFINITY : b.rr;
      return brr - arr;
    });

  const watchRows = rows
    .filter((row) => row.action === 'HOLD' && row.nearEntry)
    .sort((a, b) => {
      if (a.distanceToEntry !== b.distanceToEntry) return a.distanceToEntry - b.distanceToEntry;
      return b.confidence - a.confidence;
    });

  state.scannerRows = [...buyRows, ...watchRows];

  setSectionLoading('scanner', false);
  renderScanner();
}

function fetchTradeForSymbol(symbol) {
  const key = normalizeSymbol(symbol);
  if (!key) return Promise.resolve(null);

  const cached = scannerTradeCache.get(key);
  if (cached && (Date.now() - cached.fetchedAt) < SCANNER_TRADE_CACHE_TTL_MS) {
    return Promise.resolve(cached.trade);
  }

  if (scannerTradeInFlight.has(key)) {
    return scannerTradeInFlight.get(key);
  }

  const pending = _queueScannerTradeTask(async () => {
    const result = await fetchTrade(key, '1day', 60);
    if (!result.ok) return null;
    const body = result.body || {};
    const trade = body.trade || body;
    if (!trade || typeof trade !== 'object') return null;
    scannerTradeCache.set(key, { trade, fetchedAt: Date.now() });
    return trade;
  }).finally(() => {
    scannerTradeInFlight.delete(key);
  });

  scannerTradeInFlight.set(key, pending);
  return pending;
}

function renderScannerRows(rows) {
  if (!scannerList) return;
  const mode = state.scannerMode === 'watch' ? 'watch' : 'buy';
  if (!rows.length) {
    scannerList.innerHTML = mode === 'watch'
      ? '<div class="scanner-empty">No setups near entry right now.</div>'
      : '<div class="scanner-empty">No BUY opportunities right now.</div>';
    return;
  }

  scannerList.innerHTML = rows.map((row) => {
    const symbol = normalizeSymbol(row.symbol);
    const isSelected = state.selectedSymbol === symbol;
    const actionPillClass = row.action === 'BUY' ? 'bull' : 'neutral';
    const rrText = row.rr != null ? `RR ${Number(row.rr).toFixed(2)}` : null;
    const confText = `${Math.round((Number(row.confidence) || 0) * 100)}%`;
    const entryText = row.entryLow != null && row.entryHigh != null
      ? `${formatPrice(row.entryLow)} - ${formatPrice(row.entryHigh)}`
      : '-';

    return `
    <article class="symbol-card scanner-card ${isSelected ? 'selected' : ''}" data-panel="scanner" data-symbol="${symbol}">
      <button type="button" class="symbol-main scanner-main" data-action="select" data-panel="scanner" data-symbol="${symbol}">
        <div class="scanner-topline">
          <span class="scanner-symbol">${symbol}</span>
          <span class="scanner-price">${scannerPriceText(row.price)}</span>
        </div>
        <div class="scanner-pills">
          <span class="badge ${actionPillClass}">${row.action || 'HOLD'}</span>
          <span class="pill">Conf ${confText}</span>
          <span class="pill">${row.timeframe || '1day'}</span>
          ${rrText ? `<span class="pill">${rrText}</span>` : ''}
          ${mode === 'watch' ? '<span class="badge neutral">Near Entry</span>' : ''}
        </div>
        <div class="scanner-levels">
          <span>Entry: ${entryText}</span>
          <span>Target: ${row.target != null ? formatPrice(row.target) : '-'}</span>
          <span>Stop: ${row.stop != null ? formatPrice(row.stop) : '-'}</span>
          <span>Trail: ${row.trail != null ? formatPrice(row.trail) : '-'}</span>
        </div>
        ${row.reasons.length ? `<div class="scanner-reasons">${row.reasons.slice(0, 2).join(' • ')}</div>` : ''}
      </button>
      <div class="scanner-actions">
        <button type="button" class="button-ghost scanner-monitor-btn" data-action="monitor-add" data-symbol="${symbol}" data-entry="${row.price != null ? Number(row.price) : ''}">Monitor</button>
      </div>
    </article>
    `;
  }).join('');
}

function parseCacheAgeSeconds(rawTs) {
  if (!rawTs) return Number.POSITIVE_INFINITY;
  const ts = Date.parse(String(rawTs));
  if (Number.isNaN(ts)) return Number.POSITIVE_INFINITY;
  return Math.max(0, (Date.now() - ts) / 1000);
}

function isCacheStaleOrMissing(statusBody) {
  const quotes = statusBody?.quotes || {};
  const signals = statusBody?.signals || {};
  const bars = statusBody?.bars || {};

  if ((quotes.count || 0) === 0 || (signals.count || 0) === 0 || (bars.count || 0) === 0) {
    return true;
  }

  const staleThresholdSeconds = 180;
  const quoteAge = parseCacheAgeSeconds(quotes.latest_created_at);
  const signalAge = parseCacheAgeSeconds(signals.latest_created_at);
  const barAge = parseCacheAgeSeconds(bars.latest_ts_ingest);
  return quoteAge > staleThresholdSeconds || signalAge > staleThresholdSeconds || barAge > staleThresholdSeconds;
}

async function refreshScannerCacheStatus({ force = false } = {}) {
  const now = Date.now();
  if (!force && now - state.scannerLastStatusCheckMs < 15000) {
    return;
  }
  if (state.scannerStatusCheckInFlight) {
    return;
  }
  state.scannerStatusCheckInFlight = true;
  try {
    const statusResult = await fetchJson('/cache/status');
    state.scannerNeedsUpdate = !statusResult.ok || isCacheStaleOrMissing(statusResult.body);
    state.scannerLastStatusCheckMs = now;
    renderScannerIndicator();
  } finally {
    state.scannerStatusCheckInFlight = false;
  }
}

function queueMonitorQuoteTask(task) {
  return new Promise((resolve, reject) => {
    monitorQuoteQueue.push({ task, resolve, reject });
    runMonitorQuoteQueue();
  });
}

function runMonitorQuoteQueue() {
  while (monitorQuoteActive < MONITOR_MAX_INFLIGHT && monitorQuoteQueue.length) {
    const next = monitorQuoteQueue.shift();
    if (!next) return;
    monitorQuoteActive += 1;
    Promise.resolve()
      .then(next.task)
      .then(next.resolve)
      .catch(next.reject)
      .finally(() => {
        monitorQuoteActive = Math.max(0, monitorQuoteActive - 1);
        runMonitorQuoteQueue();
      });
  }
}

async function fetchMonitorQuote(symbol, { force = false } = {}) {
  const key = normalizeSymbol(symbol);
  if (!key) return null;
  const cached = monitorQuoteCache.get(key);
  if (!force && cached && (Date.now() - cached.fetchedAt) < MONITOR_QUOTE_CACHE_TTL_MS) {
    return cached.price;
  }
  if (monitorQuoteInFlight.has(key)) {
    return monitorQuoteInFlight.get(key);
  }

  const pending = queueMonitorQuoteTask(async () => {
    const result = await fetchJson(`/provider/twelvedata/quote?symbol=${encodeURIComponent(key)}`);
    if (result.ok) {
      const quote = result.body?.quote || {};
      const price = Number(quote.last);
      if (Number.isFinite(price)) {
        monitorQuoteCache.set(key, { price, fetchedAt: Date.now() });
        return price;
      }
    }
    const fallback = _quoteFallbackPrice(key);
    if (fallback != null) {
      monitorQuoteCache.set(key, { price: fallback, fetchedAt: Date.now() });
      return fallback;
    }
    throw new Error(getErrorMessage(result, 'Quote unavailable'));
  }).finally(() => {
    monitorQuoteInFlight.delete(key);
  });

  monitorQuoteInFlight.set(key, pending);
  return pending;
}

function monitorDaysHeld(createdAt) {
  const ts = Date.parse(createdAt || '');
  if (Number.isNaN(ts)) return 0;
  return Math.max(0, Math.floor((Date.now() - ts) / 86400000));
}

function renderMonitorPanel() {
  if (!monitorList || !monitorTotals) return;
  if (!state.monitor.length) {
    monitorTotals.innerHTML = '<div class="monitor-total-item">No monitored positions yet.</div>';
    monitorList.innerHTML = '<div class="empty">Add a symbol from Trade or Scanner.</div>';
    return;
  }

  const rows = state.monitor.map((item) => {
    const currentPrice = Number(state.monitorLastPrice[item.id]);
    const hasCurrent = Number.isFinite(currentPrice);
    const pnlValue = hasCurrent ? (currentPrice - item.entry_price) * item.shares : null;
    const pnlPct = hasCurrent ? ((currentPrice / item.entry_price) - 1) * 100 : null;
    return {
      ...item,
      currentPrice: hasCurrent ? currentPrice : null,
      pnlValue,
      pnlPct,
      stale: Boolean(state.monitorStale[item.id]),
      daysHeld: monitorDaysHeld(item.created_at),
    };
  });

  const totalAmount = rows.reduce((sum, row) => sum + row.amount, 0);
  const totalPnl = rows.reduce((sum, row) => sum + (Number.isFinite(row.pnlValue) ? row.pnlValue : 0), 0);
  const totalPnlPct = totalAmount > 0 ? (totalPnl / totalAmount) * 100 : 0;

  monitorTotals.innerHTML = `
    <div class="monitor-total-item"><span>Total Amount</span><strong>$${formatPrice(totalAmount)}</strong></div>
    <div class="monitor-total-item"><span>Total P/L $</span><strong class="${totalPnl >= 0 ? 'up' : 'down'}">$${formatPrice(totalPnl)}</strong></div>
    <div class="monitor-total-item"><span>Total P/L %</span><strong class="${totalPnlPct >= 0 ? 'up' : 'down'}">${formatPct(totalPnlPct)}</strong></div>
  `;

  monitorList.innerHTML = rows.map((row) => `
    <article class="monitor-row">
      <div class="monitor-main">
        <div class="monitor-top">
          <span class="monitor-symbol">${row.symbol}</span>
          <span class="monitor-price">${row.currentPrice != null ? `$${formatPrice(row.currentPrice)}` : '-'}</span>
        </div>
        <div class="monitor-meta">
          <span>Entry $${formatPrice(row.entry_price)}</span>
          <span>Amount $${formatPrice(row.amount)}</span>
          <span>Move <strong class="${Number.isFinite(row.pnlPct) ? (row.pnlPct >= 0 ? 'up' : 'down') : ''}">${row.pnlPct != null ? formatPct(row.pnlPct) : '--'}</strong></span>
          <span>P/L <strong class="${Number.isFinite(row.pnlValue) ? (row.pnlValue >= 0 ? 'up' : 'down') : ''}">${row.pnlValue != null ? `$${formatPrice(row.pnlValue)}` : '--'}</strong></span>
          <span>Days ${row.daysHeld}</span>
          ${row.stale ? '<span class="monitor-stale">stale</span>' : ''}
        </div>
      </div>
      <div class="monitor-actions">
        <button type="button" class="button-ghost" data-action="monitor-edit-amount" data-id="${row.id}">Edit amount</button>
        <button type="button" class="button-ghost" data-action="monitor-edit-entry" data-id="${row.id}">Edit entry</button>
        <button type="button" class="button-ghost" data-action="monitor-remove" data-id="${row.id}">Remove</button>
      </div>
    </article>
  `).join('');
}

async function refreshMonitorQuotes({ force = false } = {}) {
  if (!state.monitor.length) {
    renderMonitorPanel();
    return;
  }
  setSectionLoading('monitor', true);
  await Promise.allSettled(
    state.monitor.map(async (item) => {
      try {
        const price = await fetchMonitorQuote(item.symbol, { force });
        if (Number.isFinite(price)) {
          state.monitorLastPrice[item.id] = price;
          state.monitorStale[item.id] = false;
        } else {
          state.monitorStale[item.id] = true;
        }
      } catch {
        state.monitorStale[item.id] = true;
      }
    })
  );
  state.monitorLastRefreshMs = Date.now();
  setSectionLoading('monitor', false);
  renderMonitorPanel();
}

function addMonitorItem(symbol, defaultEntry) {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) return;
  const parsedEntry = Number(defaultEntry);
  if (!Number.isFinite(parsedEntry) || parsedEntry <= 0) {
    window.alert('No valid entry price available for this symbol yet.');
    return;
  }
  const amountInput = window.prompt(`Amount to allocate for ${normalized}:`, '1000');
  if (amountInput == null) return;
  const amount = Number(amountInput);
  if (!Number.isFinite(amount) || amount <= 0) {
    window.alert('Amount must be a positive number.');
    return;
  }
  const notesInput = window.prompt('Notes (optional):', '') || '';
  const shares = amount / parsedEntry;
  const item = {
    id: makeMonitorId(normalized),
    symbol: normalized,
    entry_price: parsedEntry,
    amount,
    shares,
    created_at: new Date().toISOString(),
    notes: notesInput.trim(),
  };
  state.monitor.unshift(item);
  saveMonitor();
  state.monitorLastPrice[item.id] = parsedEntry;
  state.monitorStale[item.id] = false;
  renderMonitorPanel();
  refreshMonitorQuotes({ force: true });
}

function addMonitorFromCurrentTrade() {
  const symbol = getSymbol();
  const tradePrice = Number(state.latestTrade?.last_close);
  const quoteText = String(quoteLast?.textContent || '').replace(/[^0-9.-]/g, '');
  const quotePrice = Number(quoteText);
  const fallbackQuote = _quoteFallbackPrice(symbol);
  const defaultEntry = Number.isFinite(tradePrice)
    ? tradePrice
    : Number.isFinite(quotePrice)
      ? quotePrice
      : fallbackQuote;
  addMonitorItem(symbol, defaultEntry);
}

function editMonitorAmount(id) {
  const idx = state.monitor.findIndex((item) => item.id === id);
  if (idx < 0) return;
  const current = state.monitor[idx];
  const nextRaw = window.prompt(`New amount for ${current.symbol}:`, String(current.amount));
  if (nextRaw == null) return;
  const next = Number(nextRaw);
  if (!Number.isFinite(next) || next <= 0) {
    window.alert('Amount must be a positive number.');
    return;
  }
  state.monitor[idx] = { ...current, amount: next, shares: next / current.entry_price };
  saveMonitor();
  renderMonitorPanel();
}

function editMonitorEntry(id) {
  const idx = state.monitor.findIndex((item) => item.id === id);
  if (idx < 0) return;
  const current = state.monitor[idx];
  const nextRaw = window.prompt(`New entry price for ${current.symbol}:`, String(current.entry_price));
  if (nextRaw == null) return;
  const next = Number(nextRaw);
  if (!Number.isFinite(next) || next <= 0) {
    window.alert('Entry price must be a positive number.');
    return;
  }
  state.monitor[idx] = { ...current, entry_price: next, shares: current.amount / next };
  saveMonitor();
  renderMonitorPanel();
}

function removeMonitorItem(id) {
  const before = state.monitor.length;
  state.monitor = state.monitor.filter((item) => item.id !== id);
  if (state.monitor.length === before) return;
  delete state.monitorLastPrice[id];
  delete state.monitorStale[id];
  saveMonitor();
  renderMonitorPanel();
}

function displayError(el, message) {
  if (!el) return;
  if (!message) {
    el.textContent = '';
    el.hidden = true;
    return;
  }
  el.textContent = message;
  el.hidden = false;
}

function getErrorMessage(result, fallback = 'Request failed') {
  if (!result) return fallback;
  const body = result.body || {};
  if (typeof body.error === 'string' && body.error.trim()) return body.error;
  if (typeof body.detail === 'string' && body.detail.trim()) return body.detail;
  if (typeof body.message === 'string' && body.message.trim()) return body.message;
  if (!result.ok) return `HTTP ${result.status || 500} ${fallback}`;
  return '';
}

async function fetchJson(url) {
  try {
    const response = await fetch(url);
    const text = await response.text();
    let data;
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = { error: 'Invalid JSON response', raw: text };
    }

    const hasError = data && typeof data.error === 'string' && data.error.trim();
    return {
      ok: response.ok && !hasError,
      status: response.status,
      body: data,
    };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      body: { error: error?.message || 'Network request failed' },
    };
  }
}

function asJson(payload) {
  return JSON.stringify(payload, null, 2);
}

function pulseValue(el) {
  if (!el) return;
  el.classList.remove('value-flash');
  void el.offsetWidth;
  el.classList.add('value-flash');
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
    error: getErrorMessage(result, 'Quote request failed'),
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
    error: getErrorMessage(result, 'Signal request failed'),
  };
}

function normalizeBatchItem(item, fallback) {
  if (item && item.ok && item.data) {
    return { ok: true, status: 200, body: item.data };
  }
  return {
    ok: false,
    status: 503,
    body: { error: item?.error || fallback },
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

function warmSymbols(symbols, panelName) {
  const missing = [...new Set(symbols.map(normalizeSymbol).filter(Boolean))].filter(
    (symbol) => !dataCache.has(symbol) && !inFlight.has(symbol)
  );

  if (missing.length === 0) return;

  const panelLoading = state.loadingByPanel[panelName];
  missing.forEach((symbol) => panelLoading?.add(symbol));
  if (panelName) setSectionLoading(panelName, true);

  Promise.allSettled(missing.map((symbol) => fetchSymbolData(symbol))).then(() => {
    missing.forEach((symbol) => panelLoading?.delete(symbol));
    if (panelName) setSectionLoading(panelName, false);
    renderPanels();
  });
}

function warmScannerSymbols(symbols) {
  const missing = [...new Set(symbols.map(normalizeSymbol).filter(Boolean))].filter(
    (symbol) => !dataCache.has(symbol) && !inFlight.has(symbol)
  );
  if (missing.length === 0) return;

  missing.forEach((symbol) => state.loadingByPanel.scanner.add(symbol));
  setSectionLoading('scanner', true);

  const batches = chunkSymbols(missing, 25);
  let hasCacheMisses = false;

  Promise.allSettled(
    batches.map(async (batch) => {
      const joined = encodeURIComponent(batch.join(','));
      const [quoteBatch, signalBatch] = await Promise.all([
        fetchJson(`/cache/quotes?symbols=${joined}`),
        fetchJson(`/cache/signals/basic?symbols=${joined}`),
      ]);

      const quoteResults = quoteBatch.body?.results || {};
      const signalResults = signalBatch.body?.results || {};

      batch.forEach((symbol) => {
        const quoteItem = quoteResults[symbol];
        const signalItem = signalResults[symbol];
        const quoteResult = normalizeBatchItem(
          quoteItem,
          getErrorMessage(quoteBatch, 'Batch quote failed')
        );
        const signalResult = normalizeBatchItem(
          signalItem,
          getErrorMessage(signalBatch, 'Batch signal failed')
        );

        if (!quoteItem?.ok || !signalItem?.ok) {
          hasCacheMisses = true;
        }
        dataCache.set(symbol, { quoteResult, signalResult, fetchedAt: Date.now() });
      });
    })
  ).finally(() => {
    missing.forEach((symbol) => state.loadingByPanel.scanner.delete(symbol));
    setSectionLoading('scanner', false);
    state.scannerNeedsUpdate = state.scannerNeedsUpdate || hasCacheMisses;
    refreshScannerCacheStatus({ force: true });
    renderScannerIndicator();
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

function getTradeOverlays(orderedBars) {
  const trade = state.latestTrade || {};
  const action = String(trade.action || '').toUpperCase();
  const target = Number(trade.target_sell_price);
  const stop = Number(trade.stop_loss_price);
  const trail = Number(trade.trailing_stop_price);
  const entryLow = Number(trade?.entry_zone?.low);
  const entryHigh = Number(trade?.entry_zone?.high);
  const overlays = [];
  const seriesLength = orderedBars.length;

  const lineDataset = (label, value, color) => ({
    label,
    data: Array.from({ length: seriesLength }, () => value),
    borderColor: color,
    backgroundColor: color,
    borderWidth: 1.4,
    borderDash: [6, 6],
    pointRadius: 0,
    pointHoverRadius: 0,
    tension: 0,
  });

  if (Number.isFinite(target)) overlays.push(lineDataset('Target', target, '#22c55e'));
  if (Number.isFinite(stop)) overlays.push(lineDataset('Stop', stop, '#ef4444'));
  if (Number.isFinite(trail)) overlays.push(lineDataset('Trail', trail, '#f59e0b'));

  if (Number.isFinite(entryLow)) {
    overlays.push({
      label: 'Entry Low',
      data: Array.from({ length: seriesLength }, () => entryLow),
      borderColor: '#6366f1',
      backgroundColor: 'rgba(99, 102, 241, 0.08)',
      borderWidth: 1.2,
      borderDash: [3, 4],
      pointRadius: 0,
      pointHoverRadius: 0,
      tension: 0,
    });
  }
  if (Number.isFinite(entryHigh)) {
    overlays.push({
      label: 'Entry High',
      data: Array.from({ length: seriesLength }, () => entryHigh),
      borderColor: '#6366f1',
      backgroundColor: 'rgba(99, 102, 241, 0.15)',
      borderWidth: 1.2,
      borderDash: [3, 4],
      pointRadius: 0,
      pointHoverRadius: 0,
      tension: 0,
      fill: Number.isFinite(entryLow) ? '-1' : false,
    });
  }

  if (orderedBars.length && (action === 'BUY' || action === 'SELL')) {
    const lastBar = orderedBars[orderedBars.length - 1];
    const close = Number(lastBar?.close);
    const xLabel = (lastBar?.ts_event || lastBar?.ts_ingest || '').slice(0, 10);
    if (Number.isFinite(close) && xLabel) {
      overlays.push({
        type: 'scatter',
        label: action,
        data: [{ x: xLabel, y: close }],
        borderColor: action === 'BUY' ? '#22c55e' : '#ef4444',
        backgroundColor: action === 'BUY' ? '#22c55e' : '#ef4444',
        pointRadius: 7,
        pointHoverRadius: 8,
        pointStyle: action === 'BUY' ? 'triangle' : 'rectRot',
        showLine: false,
      });
    }
  }
  return overlays;
}

function renderChart(symbol, bars) {
  if (!chartCanvas) return;
  state.hoveredChartIndex = null;
  if (chartOhlc) chartOhlc.textContent = 'O:- H:- L:- C:-';

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
  const overlays = getTradeOverlays(ordered);

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
        pointRadius(context) {
          const idx = context.dataIndex;
          return state.hoveredChartIndex === idx ? 4 : 0;
        },
        pointBackgroundColor: '#93c5fd',
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
      ...overlays,
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        labels: {
          color: '#334155',
          filter(item) {
            return item.text !== 'Entry Low';
          },
        },
      },
      tooltip: {
        mode: 'index',
        intersect: false,
        callbacks: {
          afterBody(items) {
            const idx = items?.[0]?.dataIndex;
            const bar = Number.isInteger(idx) ? ordered[idx] : null;
            if (!bar) return [];
            return [
              `O: ${formatPrice(bar.open)}`,
              `H: ${formatPrice(bar.high)}`,
              `L: ${formatPrice(bar.low)}`,
              `C: ${formatPrice(bar.close)}`,
            ];
          },
        },
      },
      chartOverlayPlugin: {
        hoveredIndex: state.hoveredChartIndex,
      },
      zoom: {
        pan: {
          enabled: true,
          mode: 'x',
        },
        zoom: {
          wheel: { enabled: true },
          pinch: { enabled: true },
          mode: 'x',
        },
      },
    },
    interaction: {
      mode: 'index',
      intersect: false,
    },
    onHover(event, elements) {
      const idx = elements?.[0]?.index;
      state.hoveredChartIndex = Number.isInteger(idx) ? idx : null;
      const bar = Number.isInteger(idx) ? ordered[idx] : null;
      if (chartOhlc) {
        if (bar) {
          chartOhlc.textContent = `O:${formatPrice(bar.open)} H:${formatPrice(bar.high)} L:${formatPrice(bar.low)} C:${formatPrice(bar.close)}`;
        } else {
          chartOhlc.textContent = 'O:- H:- L:- C:-';
        }
      }
      if (priceChart) priceChart.draw();
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
  state.latestBars = bars;
  renderChart(symbol, bars);
}

function renderQuote(result, requestedSymbol) {
  const body = result.body || {};
  const quote = body.quote || {};
  const errorMessage = getErrorMessage(result, 'Quote request failed');

  displayError(quoteError, errorMessage);

  if (!result.ok) {
    quoteSymbol.textContent = requestedSymbol;
    quoteLast.textContent = '-';
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
  pulseValue(quoteLast);
}

function renderSignal(result) {
  const body = result.body || {};
  const errorMessage = getErrorMessage(result, 'Signal request failed');

  displayError(signalError, errorMessage);

  if (!result.ok) {
    signalScore.textContent = '-';
    signalTrend.textContent = 'trend: -';
    signalMomentum.textContent = 'momentum: -';
    resetClass(signalTrend, 'badge', 'neutral');
    resetClass(signalMomentum, 'badge', 'neutral');
    signalConfidence.textContent = '0%';
    signalConfidenceBar.style.width = '0%';
    signalConfidenceBar.style.setProperty('--pct', '0');
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
  signalConfidenceBar.style.setProperty('--pct', String(Math.round(conf * 100)));

  signalDebug.textContent = asJson(body.debug || {});
  pulseValue(signalScore);
  pulseValue(signalConfidence);
}

// Trade support

function formatPrice(value) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  return Number(value).toFixed(2);
}

function formatPct(value) {
  if (value == null || Number.isNaN(Number(value))) return '--';
  const num = Number(value);
  const sign = num > 0 ? '+' : '';
  return `${sign}${num.toFixed(2)}%`;
}

function formatScore(value) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  return String(Math.round(Number(value)));
}

function entryZoneText(zone) {
  if (!zone || typeof zone !== 'object') return '-';
  const low = zone.low != null ? formatPrice(zone.low) : '-';
  const high = zone.high != null ? formatPrice(zone.high) : '-';
  const type = zone.type ? String(zone.type) : '';
  return type ? `${low} to ${high} (${type})` : `${low} to ${high}`;
}

function renderTrade(result) {
  if (!tradeAction && !tradeRaw && !tradeError) {
    return;
  }

  const errorMessage = getErrorMessage(result, 'Trade request failed');
  displayError(tradeError, errorMessage);

  if (!result.ok) {
    if (tradeAction) tradeAction.textContent = '-';
    if (tradeConfidence) tradeConfidence.textContent = '-';
    if (tradeTimeframe) tradeTimeframe.textContent = '-';
    if (tradeEntryZone) tradeEntryZone.textContent = '-';
    if (tradeTarget) tradeTarget.textContent = '-';
    if (tradeStop) tradeStop.textContent = '-';
    if (tradeTrail) tradeTrail.textContent = '-';
    if (tradeReasons) tradeReasons.textContent = '[]';
    if (tradeRaw) tradeRaw.textContent = asJson(result);
    const explainEl = document.getElementById('tradeExplanationDynamic');
    if (explainEl) explainEl.innerHTML = '';
    state.latestTrade = null;
    if (tradeAction) tradeAction.className = '';
    if (state.latestBars.length) {
      renderChart(getSymbol(), state.latestBars);
    }
    return;
  }

  const body = result.body || {};
  const trade = body.trade || body || {};

  if (tradeAction) tradeAction.textContent = trade.action || '-';
  if (tradeAction) {
    const action = String(trade.action || '').toUpperCase();
    tradeAction.className = action === 'BUY' ? 'trade-action-buy' : action === 'SELL' ? 'trade-action-sell' : 'trade-action-hold';
  }
  if (tradeConfidence) {
    const conf = trade.confidence != null ? Number(trade.confidence) : null;
    tradeConfidence.textContent = conf == null || Number.isNaN(conf) ? '-' : `${Math.round(conf * 100)}%`;
  }
  if (tradeTimeframe) tradeTimeframe.textContent = trade.timeframe || body.interval || '-';

  if (tradeEntryZone) tradeEntryZone.textContent = entryZoneText(trade.entry_zone);
  if (tradeTarget) tradeTarget.textContent = trade.target_sell_price != null ? formatPrice(trade.target_sell_price) : '-';
  if (tradeStop) tradeStop.textContent = trade.stop_loss_price != null ? formatPrice(trade.stop_loss_price) : '-';
  if (tradeTrail) tradeTrail.textContent = trade.trailing_stop_price != null ? formatPrice(trade.trailing_stop_price) : '-';

  if (tradeReasons) {
    const reasons = Array.isArray(trade.reasons) ? trade.reasons : [];
    tradeReasons.textContent = asJson(reasons);
  }

  if (tradeRaw) tradeRaw.textContent = asJson(result);

  const explainEl = document.getElementById('tradeExplanationDynamic');
  if (explainEl) {
    const explanation = trade.explanation && typeof trade.explanation === 'object' ? trade.explanation : null;
    if (explanation) {
      const calc = explanation.calc && typeof explanation.calc === 'object' ? explanation.calc : {};
      const entryAnchor = calc.entry_anchor != null ? formatPrice(calc.entry_anchor) : '-';
      const stopText = calc.stop != null ? formatPrice(calc.stop) : '-';
      const riskText = calc.risk_per_share != null ? formatPrice(calc.risk_per_share) : '-';
      const rrText = calc.risk_reward_ratio != null ? String(calc.risk_reward_ratio) : '-';
      const targetText = calc.target != null ? formatPrice(calc.target) : '-';
      const atrText = calc.atr14 != null ? formatPrice(calc.atr14) : 'null';
      const notes = Array.isArray(explanation.notes) ? explanation.notes : [];
      const notesHtml = notes.length
        ? `<ul>${notes.map((n) => `<li>${String(n)}</li>`).join('')}</ul>`
        : '';

      explainEl.innerHTML = `
        <p><strong>Why this action:</strong> ${String(explanation.action_why || '-')}</p>
        <p><strong>Why this target:</strong> ${String(explanation.target_why || '-')}</p>
        <p><strong>Why this stop:</strong> ${String(explanation.stop_why || '-')}</p>
        <p><code>Entry(anchor): ${entryAnchor}, Stop: ${stopText}, Risk: ${riskText}, RR: ${rrText}, Target: ${targetText}, ATR14: ${atrText}</code></p>
        ${notesHtml}
      `;
    } else {
      explainEl.innerHTML = '';
    }
  }

  state.latestTrade = trade;
  if (state.latestBars.length) {
    renderChart(getSymbol(), state.latestBars);
  }
  pulseValue(tradeAction);
  pulseValue(tradeConfidence);
}

async function fetchTrade(symbol, interval, outputsize) {
  const url = `/signal/trade?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&outputsize=${encodeURIComponent(String(outputsize))}`;
  return fetchJson(url);
}

async function loadTradeForCurrentSymbol() {
  const symbol = getSymbol();
  const interval = getInterval();
  const outputsize = getOutputsize();

  renderTrade({ ok: false, status: 0, body: { error: '' } });
  displayError(tradeError, 'Loading...');

  const result = await fetchTrade(symbol, interval, outputsize);
  renderTrade(result);
}

function toFiniteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function computeRsi(values, period = 14) {
  if (!Array.isArray(values) || values.length < period + 1) return null;
  let gains = 0;
  let losses = 0;
  for (let i = values.length - period; i < values.length; i += 1) {
    const prev = Number(values[i - 1]);
    const cur = Number(values[i]);
    if (!Number.isFinite(prev) || !Number.isFinite(cur)) continue;
    const diff = cur - prev;
    if (diff >= 0) gains += diff;
    else losses += Math.abs(diff);
  }
  if (losses === 0) return 100;
  const rs = gains / losses;
  return 100 - (100 / (1 + rs));
}

function computeTradeAction(closes) {
  if (closes.length < 20) return 'HOLD';
  const ma10 = computeMovingAverage(closes, 10).at(-1);
  const ma20 = computeMovingAverage(closes, 20).at(-1);
  const rsi14 = computeRsi(closes, 14);
  const last = closes.at(-1);
  if ([ma10, ma20, rsi14, last].some((v) => !Number.isFinite(v))) return 'HOLD';
  if (last > ma10 && ma10 > ma20 && rsi14 < 70) return 'BUY';
  if (last < ma10 && ma10 < ma20 && rsi14 > 30) return 'SELL';
  return 'HOLD';
}

function midpoint(zone) {
  if (!zone || typeof zone !== 'object') return null;
  const low = toFiniteNumber(zone.low);
  const high = toFiniteNumber(zone.high);
  if (low == null || high == null) return null;
  return (low + high) / 2;
}

function setBacktestResults(payload = null) {
  if (!payload) {
    btWinRate.textContent = '-';
    btTrades.textContent = '-';
    btReturn.textContent = '-';
    btMaxDd.textContent = '-';
    return;
  }
  btWinRate.textContent = `${payload.winRate.toFixed(1)}%`;
  btTrades.textContent = String(payload.totalTrades);
  btReturn.textContent = `${payload.cumulativeReturnPct.toFixed(2)}%`;
  btMaxDd.textContent = `${payload.maxDrawdownPct.toFixed(2)}%`;
}

async function runBacktestForCurrentSymbol() {
  const symbol = getSymbol();
  const interval = getInterval();
  const outputsize = getOutputsize();

  if (backtestBtn) {
    backtestBtn.disabled = true;
    backtestBtn.textContent = 'Running...';
  }
  setBacktestResults(null);

  try {
    const [barsResult, tradeResult] = await Promise.all([
      fetchJson(`/market/bars?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&outputsize=${encodeURIComponent(String(outputsize))}`),
      fetchTrade(symbol, interval, outputsize),
    ]);

    const bars = (barsResult.body?.bars || []).filter((b) => toFiniteNumber(b?.close) != null);
    if (!barsResult.ok || bars.length < 25) {
      throw new Error(getErrorMessage(barsResult, 'Not enough bars to backtest'));
    }

    const ordered = sortBarsAscending(bars);
    const closes = ordered.map((b) => Number(b.close));

    const trade = tradeResult.body?.trade || tradeResult.body || {};
    const entryBase = midpoint(trade.entry_zone);
    const targetBase = toFiniteNumber(trade.target_sell_price);
    const stopBase = toFiniteNumber(trade.stop_loss_price);
    const trailBase = toFiniteNumber(trade.trailing_stop_price);
    const targetRatio = entryBase && targetBase ? targetBase / entryBase : 1.03;
    const stopRatio = entryBase && stopBase ? stopBase / entryBase : 0.98;
    const trailRatio = entryBase && trailBase ? trailBase / entryBase : null;

    let inPosition = false;
    let entry = 0;
    let peak = 1;
    let equity = 1;
    let maxDrawdown = 0;
    let trades = 0;
    let wins = 0;

    for (let i = 20; i < ordered.length; i += 1) {
      const bar = ordered[i];
      const close = Number(bar.close);
      if (!Number.isFinite(close)) continue;

      if (!inPosition) {
        const action = computeTradeAction(closes.slice(0, i + 1));
        if (action === 'BUY') {
          entry = entryBase && Number.isFinite(entryBase) ? entryBase : close;
          inPosition = true;
        }
      } else {
        const high = toFiniteNumber(bar.high) ?? close;
        const low = toFiniteNumber(bar.low) ?? close;
        const target = entry * targetRatio;
        let stop = entry * stopRatio;
        if (trailRatio) {
          stop = Math.max(stop, close * trailRatio);
        }

        let exit = null;
        if (high >= target) exit = target;
        else if (low <= stop) exit = stop;
        else if (computeTradeAction(closes.slice(0, i + 1)) === 'SELL') exit = close;

        if (exit != null) {
          const ret = (exit - entry) / entry;
          equity *= 1 + ret;
          trades += 1;
          if (ret > 0) wins += 1;
          inPosition = false;
          peak = Math.max(peak, equity);
          const dd = ((peak - equity) / peak) * 100;
          maxDrawdown = Math.max(maxDrawdown, dd);
        }
      }
    }

    const cumulativeReturnPct = (equity - 1) * 100;
    const winRate = trades ? (wins / trades) * 100 : 0;
    setBacktestResults({
      winRate,
      totalTrades: trades,
      cumulativeReturnPct,
      maxDrawdownPct: maxDrawdown,
    });
  } catch (error) {
    setBacktestResults(null);
    displayError(tradeError, error?.message || 'Backtest failed');
  } finally {
    if (backtestBtn) {
      backtestBtn.disabled = false;
      backtestBtn.textContent = 'Backtest Strategy';
    }
  }
}

function sortByScoreDesc(a, b) {
  const as = a.signal.score == null ? Number.NEGATIVE_INFINITY : Number(a.signal.score);
  const bs = b.signal.score == null ? Number.NEGATIVE_INFINITY : Number(b.signal.score);
  if (bs !== as) return bs - as;
  return a.symbol.localeCompare(b.symbol);
}

function compareNullableNumberDesc(a, b) {
  const aNum = Number(a);
  const bNum = Number(b);
  const aMissing = !Number.isFinite(aNum);
  const bMissing = !Number.isFinite(bNum);

  if (aMissing && bMissing) return 0;
  if (aMissing) return 1;
  if (bMissing) return -1;
  return bNum - aNum;
}

function sortWatchlistRows(rows) {
  const mode = state.watchlistSort;
  const sorted = [...rows];

  if (mode === 'price') {
    sorted.sort((a, b) => {
      const diff = compareNullableNumberDesc(a.quote.last, b.quote.last);
      if (diff !== 0) return diff;
      return a.symbol.localeCompare(b.symbol);
    });
    return sorted;
  }

  if (mode === 'score') {
    sorted.sort((a, b) => {
      const diff = compareNullableNumberDesc(a.signal.score, b.signal.score);
      if (diff !== 0) return diff;
      return a.symbol.localeCompare(b.symbol);
    });
    return sorted;
  }

  sorted.sort((a, b) => a.symbol.localeCompare(b.symbol));
  return sorted;
}

function getPanelRows(symbols, panelName) {
  return symbols.map((symbol) => {
    const cached = dataCache.get(symbol);
    const quote = getQuoteView(symbol, cached?.quoteResult);
    const signal = getSignalView(cached?.signalResult);
    const isSelected = state.selectedSymbol === symbol;
    const isExpanded = state.expandedByPanel[panelName] === symbol;
    const isLoading = state.loadingByPanel[panelName]?.has(symbol) || false;
    const hasData = Boolean(cached);
    return { symbol, quote, signal, isSelected, isExpanded, isLoading, hasData };
  });
}

function renderCardDetails(row) {
  const debug = row.signal.debug || {};

  return `
    <div class="expand-content">
      <div class="expand-grid">
        <div>
          <div class="label">Quote summary</div>
          <div class="expand-line">last: ${formatPrice(row.quote.last)}</div>
          <div class="expand-line">ts_event: ${row.quote.ts || '-'}</div>
          <div class="expand-line">provider: ${row.quote.provider || '-'}</div>
          <div class="expand-line">error: ${row.quote.error || '-'}</div>
        </div>
        <div>
          <div class="label">Signal summary</div>
          <div class="expand-line">score: ${formatScore(row.signal.score)}</div>
          <div class="expand-line">confidence: ${Math.round((row.signal.confidence || 0) * 100)}%</div>
          <div class="expand-line">trend: ${row.signal.trend || '-'}</div>
          <div class="expand-line">momentum: ${row.signal.momentum || '-'}</div>
          <div class="expand-line">error: ${row.signal.error || '-'}</div>
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

function sentimentChipText(row) {
  const trend = row.hasData ? row.signal.trend : '...';
  const momentum = row.hasData ? row.signal.momentum : '...';
  return { trend, momentum };
}

function renderSymbolList(container, rows, panelName, options = {}) {
  const showQty = Boolean(options.showQty);
  const qtyBySymbol = options.qtyBySymbol || {};
  const plPctBySymbol = options.plPctBySymbol || {};

  if (!rows.length) {
    container.innerHTML = '<div class="empty">No symbols.</div>';
    return;
  }

  container.innerHTML = rows
    .map((row) => {
      const trendClass = sentimentClass(row.signal.trend);
      const momentumClass = sentimentClass(row.signal.momentum);
      const loadingClass = row.isLoading ? 'loading' : '';
      const priceText = row.hasData ? `$${formatPrice(row.quote.last)}` : '...';
      const scoreText = row.hasData ? `score ${formatScore(row.signal.score)}` : 'score ...';
      const { trendText, momentumText } = (() => {
        const t = sentimentChipText(row);
        return { trendText: t.trend, momentumText: t.momentum };
      })();

      const qty = showQty ? `<span class="pill">qty ${qtyBySymbol[row.symbol] ?? 0}</span>` : '';
      const pl = showQty
        ? `<span class="pill muted-pill">P/L ${formatPct(plPctBySymbol[row.symbol])}</span>`
        : '';
      const hasErr = row.hasData && (row.quote.error || row.signal.error);
      const errBadge = hasErr ? '<span class="badge bear">ERR</span>' : '';

      return `
      <article class="symbol-card ${row.isSelected ? 'selected' : ''} ${loadingClass}" data-panel="${panelName}" data-symbol="${row.symbol}">
        <button type="button" class="symbol-main" data-action="select" data-panel="${panelName}" data-symbol="${row.symbol}">
          <span class="sym">${row.symbol}</span>
          <span class="metric ${row.hasData ? '' : 'skeleton-chip'}">${priceText}</span>
          <span class="metric ${row.hasData ? '' : 'skeleton-chip'}">${scoreText}</span>
          <span class="badge ${row.hasData ? trendClass : 'neutral'} ${row.hasData ? '' : 'skeleton-chip'}">${trendText}</span>
          <span class="badge ${row.hasData ? momentumClass : 'neutral'} ${row.hasData ? '' : 'skeleton-chip'}">${momentumText}</span>
          ${errBadge}
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
  if (scannerToggleBtn) {
    scannerToggleBtn.textContent = 'Refresh';
  }
  const mode = state.scannerMode === 'watch' ? 'watch' : 'buy';
  const rows = state.scannerRows.filter((row) => (mode === 'watch' ? row.action === 'HOLD' && row.nearEntry : row.action === 'BUY'));
  renderScannerRows(rows);
}

function renderWatchlist() {
  const rows = sortWatchlistRows(getPanelRows(state.watchlist, 'watchlist'));
  renderSymbolList(watchlistList, rows, 'watchlist');
  warmSymbols(state.watchlist, 'watchlist');
}

function buildPortfolioDerived() {
  const bySymbol = new Map();

  state.portfolio.forEach((entry) => {
    const symbol = normalizeSymbol(entry.symbol);
    if (!symbol) return;

    const qty = Number(entry.qty);
    const avg = Number(entry.avg_cost);

    const current = bySymbol.get(symbol) || { qty: 0, costValue: 0 };
    const nextQty = current.qty + (Number.isFinite(qty) ? qty : 0);
    const nextCost = current.costValue +
      (Number.isFinite(qty) && Number.isFinite(avg) ? qty * avg : 0);

    bySymbol.set(symbol, { qty: nextQty, costValue: nextCost });
  });

  const symbols = [...bySymbol.keys()].sort((a, b) => a.localeCompare(b));
  const qtyBySymbol = {};
  const avgCostBySymbol = {};

  symbols.forEach((symbol) => {
    const rollup = bySymbol.get(symbol);
    qtyBySymbol[symbol] = rollup.qty;
    avgCostBySymbol[symbol] = rollup.qty > 0 ? rollup.costValue / rollup.qty : null;
  });

  return { symbols, qtyBySymbol, avgCostBySymbol };
}

function renderPortfolio() {
  const { symbols, qtyBySymbol, avgCostBySymbol } = buildPortfolioDerived();
  const rows = getPanelRows(symbols, 'portfolio').sort((a, b) => a.symbol.localeCompare(b.symbol));

  const plPctBySymbol = Object.fromEntries(
    rows.map((row) => {
      const avgCost = avgCostBySymbol[row.symbol];
      const last = Number(row.quote.last);
      if (!Number.isFinite(avgCost) || avgCost <= 0 || !Number.isFinite(last)) {
        return [row.symbol, null];
      }
      return [row.symbol, ((last - avgCost) / avgCost) * 100];
    })
  );

  renderSymbolList(portfolioList, rows, 'portfolio', { showQty: true, qtyBySymbol, plPctBySymbol });
  warmSymbols(symbols, 'portfolio');
}

function renderMonitor() {
  renderMonitorPanel();
}

function renderPanels() {
  renderScanner();
  renderWatchlist();
  renderMonitor();
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

  try {
    await loadBarsChart(normalized);
  } catch {
    renderChart(normalized, []);
  }

  renderPanels();
}

async function safeSelectSymbol(symbol, options = {}) {
  try {
    await selectSymbol(symbol, options);
  } catch (error) {
    const message = error?.message || 'Failed to load symbol data';
    renderQuote({ ok: false, status: 0, body: { error: message } }, normalizeSymbol(symbol));
    renderSignal({ ok: false, status: 0, body: { error: message } });
    renderPanels();
  }
}

function addPortfolioEntry() {
  const symbol = normalizeSymbol(window.prompt('Portfolio symbol (e.g. AAPL):', ''));
  if (!symbol) return;

  const qtyInput = window.prompt('Quantity:', '1');
  const qty = Number(qtyInput);
  if (!Number.isFinite(qty) || qty <= 0) {
    window.alert('Quantity must be a positive number.');
    return;
  }

  const avgInput = window.prompt('Average cost per share (optional):', '');
  let avgCost = null;
  if (avgInput != null && avgInput.trim() !== '') {
    const parsed = Number(avgInput);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      window.alert('Average cost must be a positive number if provided.');
      return;
    }
    avgCost = parsed;
  }

  state.portfolio.push({ symbol, qty, avg_cost: avgCost });
  savePortfolio();
  renderPortfolio();
}

if (scannerToggleBtn) {
  scannerToggleBtn.addEventListener('click', () => {
    refreshScannerData();
  });
}

if (watchlistSort) {
  watchlistSort.addEventListener('change', () => {
    state.watchlistSort = watchlistSort.value;
    renderWatchlist();
  });
}

if (watchlistAddBtn) {
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
}

if (monitorRefreshBtn) {
  monitorRefreshBtn.addEventListener('click', () => {
    refreshMonitorQuotes({ force: true });
  });
}

if (portfolioAddBtn) {
  portfolioAddBtn.addEventListener('click', addPortfolioEntry);
}

if (watchlistInput) {
  watchlistInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      if (watchlistAddBtn) watchlistAddBtn.click();
    }
  });
}

document.addEventListener('click', (event) => {
  const monitorAddTarget = event.target.closest('[data-action="monitor-add"]');
  if (monitorAddTarget) {
    const symbol = normalizeSymbol(monitorAddTarget.dataset.symbol);
    const entry = Number(monitorAddTarget.dataset.entry);
    addMonitorItem(symbol, entry);
    return;
  }

  const editAmountTarget = event.target.closest('[data-action="monitor-edit-amount"]');
  if (editAmountTarget) {
    editMonitorAmount(String(editAmountTarget.dataset.id || ''));
    return;
  }

  const editEntryTarget = event.target.closest('[data-action="monitor-edit-entry"]');
  if (editEntryTarget) {
    editMonitorEntry(String(editEntryTarget.dataset.id || ''));
    return;
  }

  const removeTarget = event.target.closest('[data-action="monitor-remove"]');
  if (removeTarget) {
    removeMonitorItem(String(removeTarget.dataset.id || ''));
    return;
  }

  const target = event.target.closest('[data-action="select"]');
  if (!target) return;

  const panel = target.dataset.panel;
  const symbol = normalizeSymbol(target.dataset.symbol);
  if (!panel || !symbol) return;

  state.expandedByPanel[panel] = state.expandedByPanel[panel] === symbol ? null : symbol;
  safeSelectSymbol(symbol);
});

quoteBtn.addEventListener('click', async () => {
  const symbol = getSymbol();
  await safeSelectSymbol(symbol, { force: true });
});

signalBtn.addEventListener('click', async () => {
  const symbol = getSymbol();
  await safeSelectSymbol(symbol, { force: true });
});

if (tradeBtn) {
  tradeBtn.addEventListener('click', async () => {
    await loadTradeForCurrentSymbol();
  });
}

if (addMonitorBtn) {
  addMonitorBtn.addEventListener('click', () => {
    addMonitorFromCurrentTrade();
  });
}

if (backtestBtn) {
  backtestBtn.addEventListener('click', async () => {
    await runBacktestForCurrentSymbol();
  });
}

window.addEventListener('DOMContentLoaded', async () => {
  const initial = getSymbol();
  state.selectedSymbol = initial;
  initScannerModeToggle();
  renderPanels();
  await refreshScannerData();
  window.setInterval(() => {
    refreshScannerData();
  }, 60000);
  await refreshMonitorQuotes();
  window.setInterval(() => {
    refreshMonitorQuotes();
  }, 60000);
  await safeSelectSymbol(initial);

  // Optional: clear trade card on load so it doesn't show stale content
  if (tradeRaw) tradeRaw.textContent = '{}';
  if (tradeReasons) tradeReasons.textContent = '[]';
  setBacktestResults(null);
});

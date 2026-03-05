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
const scannerSegment = document.getElementById('scannerSegment');
const scannerStatusRow = document.getElementById('scannerStatusRow');
const scannerStatusPill = document.getElementById('scannerStatusPill');
const scannerProgressTrack = document.getElementById('scannerProgressTrack');
const scannerProgressFill = document.getElementById('scannerProgressFill');
const scannerStatusMeta = document.getElementById('scannerStatusMeta');
const scannerStatusRefreshBtn = document.getElementById('scannerStatusRefreshBtn');
const watchlistInput = document.getElementById('watchlistInput');
const watchlistAddBtn = document.getElementById('watchlistAddBtn');
const watchlistSort = document.getElementById('watchlistSort');
const watchlistList = document.getElementById('watchlistList');
const monitorRefreshBtn = document.getElementById('monitorRefreshBtn');
const monitorTotals = document.getElementById('monitorTotals');
const monitorList = document.getElementById('monitorList');
const monitorModal = document.getElementById('monitorModal');
const monitorModalSymbol = document.getElementById('monitorModalSymbol');
const monitorModalAmount = document.getElementById('monitorModalAmount');
const monitorModalPrice = document.getElementById('monitorModalPrice');
const monitorModalZoneLow = document.getElementById('monitorModalZoneLow');
const monitorModalZoneHigh = document.getElementById('monitorModalZoneHigh');
const monitorModalSubmit = document.getElementById('monitorModalSubmit');
const monitorModalCancel = document.getElementById('monitorModalCancel');
const monitorModalError = document.getElementById('monitorModalError');
const monitorModalSuccess = document.getElementById('monitorModalSuccess');
const scannerSourcesModal = document.getElementById('scannerSourcesModal');
const scannerSourcesTitle = document.getElementById('scannerSourcesTitle');
const scannerSourcesTotals = document.getElementById('scannerSourcesTotals');
const scannerSourcesList = document.getElementById('scannerSourcesList');
const scannerSourcesError = document.getElementById('scannerSourcesError');
const scannerSourcesCloseBtn = document.getElementById('scannerSourcesCloseBtn');
const portfolioAddBtn = document.getElementById('portfolioAddBtn');
const portfolioList = document.getElementById('portfolioList');
const paperRefreshBtn = document.getElementById('paperRefreshBtn');
const paperSummary = document.getElementById('paperSummary');
const paperPositionsBody = document.getElementById('paperPositionsBody');
const paperClosedBody = document.getElementById('paperClosedBody');
const paperRunsBody = document.getElementById('paperRunsBody');
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
const buyNowBtn = document.getElementById('buyNowBtn');
const btWinRate = document.getElementById('btWinRate');
const btTrades = document.getElementById('btTrades');
const btReturn = document.getElementById('btReturn');
const btMaxDd = document.getElementById('btMaxDd');

const paperOrderModal = document.getElementById('paperOrderModal');
const paperOrderModalTitle = document.getElementById('paperOrderModalTitle');
const paperOrderModalSymbol = document.getElementById('paperOrderModalSymbol');
const paperOrderModalAmount = document.getElementById('paperOrderModalAmount');
const paperOrderModalStrategy = document.getElementById('paperOrderModalStrategy');
const paperOrderModalTactic = document.getElementById('paperOrderModalTactic');
const paperOrderModalSubmit = document.getElementById('paperOrderModalSubmit');
const paperOrderModalCancel = document.getElementById('paperOrderModalCancel');
const paperOrderModalError = document.getElementById('paperOrderModalError');
const paperOrderModalSuccess = document.getElementById('paperOrderModalSuccess');

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
let openModalCount = 0;
const PAPER_DEFAULT_AMOUNT = 1000;
const paperBuyInFlightBySymbol = new Map();
let paperOrderDraft = null;

const state = {
  scannerExpanded: false,
  scannerAgent: 'overall',
  scannerMarket: 'ALL',
  scannerSegment: 'small',
  scannerRows: [],
  selectedSymbol: 'AAPL',
  watchlist: loadWatchlist(),
  watchlistSort: 'symbol',
  monitorRows: [],
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
  monitorDraft: null,
  monitorModalSaving: false,
  scannerSources: null,
  scannerSourcesLoading: false,
  scannerSourcesSymbol: null,
  scannerSourcesChannel: null,
  scannerSourcesTimeframe: '1day',
  scannerSourcesByKey: {},
  scannerRuntimeStatus: { state: 'idle', run_id: null, error: null },
  scannerRuntimeProgress: null,
  scannerRuntimeStale: false,
  scannerRuntimeLastRunAt: null,
  scannerPollTimer: null,
  paperStatus: null,
  paperPositions: [],
  paperRecentOrders: [],
  paperStrategies: [],
  paperLastAmountUsd: PAPER_DEFAULT_AMOUNT,
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
  return Number.isFinite(num) && num > 0 ? `$${formatPrice(num)}` : '—';
}

function resolveScannerSymbol(item, fallbackKey = '') {
  const raw = item?.symbol
    || item?.ticker
    || item?.instrument
    || item?.instrument_id
    || item?.data?.symbol
    || item?.quote?.symbol
    || item?.payload?.symbol
    || fallbackKey
    || '';
  const sym = String(raw).trim().toUpperCase();
  return sym || 'UNKNOWN';
}

function resolveScannerPrice(item) {
  const candidates = [
    item?.price,
    item?.last,
    item?.last_close,
    item?.quote?.last,
    item?.payload?.quote?.last,
  ];
  for (const c of candidates) {
    if (c == null || c === '') continue;
    const n = Number(c);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

function resolveScannerScore(item) {
  const candidates = [
    item?.score,
    item?.signal_score,
    item?.signal?.score,
    item?.payload?.score,
    item?.payload?.signal?.score,
  ];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n)) return Math.round(n);
  }
  const conf = Number(item?.confidence);
  const action = String(item?.action || '').toUpperCase();
  if (Number.isFinite(conf)) {
    let derived = Math.round(conf * 100);
    if (action === 'BUY') derived += 10;
    if (action === 'SELL') derived -= 10;
    return derived;
  }
  return null;
}

function resolveScannerTarget(item) {
  const candidates = [
    item?.trade?.target_sell_price,
    item?.target,
    item?.target_price,
    item?.payload?.trade?.target_sell_price,
  ];
  for (const c of candidates) {
    if (c == null || c === '') continue;
    const n = Number(c);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

function resolveScannerLevelValue(...values) {
  for (const v of values) {
    if (v == null || v === '') continue;
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

function scannerSourcesCacheKey(symbol, channel, timeframe = '1day') {
  const sym = normalizeSymbol(symbol);
  const ch = String(channel || 'overall').trim().toLowerCase();
  const tf = String(timeframe || '1day').trim().toLowerCase();
  return `${sym}:${ch}:${tf}`;
}

function normaliseSourcesPayload(rawPayload) {
  const payload = rawPayload && typeof rawPayload === 'object' ? rawPayload : {};
  const rawSources = payload.connectors
    || payload.sources
    || payload.source_breakdown
    || payload?.payload?.sources
    || payload?.payload?.source_breakdown
    || payload?.explanation?.sources
    || payload?.debug?.sources
    || null;
  const rows = [];

  if (Array.isArray(rawSources)) {
    rawSources.forEach((entry) => {
      if (!entry || typeof entry !== 'object') return;
      const source = String(entry.source || entry.name || entry.id || entry.key || '').trim();
      if (!source) return;
      const posts = Number(entry.count ?? entry.posts ?? entry.mentions ?? 0);
      const positive = Number(entry.pos ?? entry.positive ?? 0);
      const negative = Number(entry.neg ?? entry.negative ?? 0);
      rows.push({
        source,
        posts: Number.isFinite(posts) ? posts : 0,
        positive: Number.isFinite(positive) ? positive : 0,
        negative: Number.isFinite(negative) ? negative : 0,
        neutral: Number.isFinite(Number(entry.neutral))
          ? Number(entry.neutral)
          : Math.max(0, (Number.isFinite(posts) ? posts : 0) - (Number.isFinite(positive) ? positive : 0) - (Number.isFinite(negative) ? negative : 0)),
        status: String(entry.status || entry.origin || 'auto'),
        enabled: entry.enabled != null ? Boolean(entry.enabled) : null,
      });
    });
  } else if (rawSources && typeof rawSources === 'object') {
    Object.keys(rawSources).forEach((key) => {
      const val = rawSources[key];
      if (val && typeof val === 'object' && !Array.isArray(val)) {
        const posts = Number(val.count ?? val.posts ?? val.mentions ?? 0);
        const positive = Number(val.pos ?? val.positive ?? 0);
        const negative = Number(val.neg ?? val.negative ?? 0);
        rows.push({
          source: key,
          posts: Number.isFinite(posts) ? posts : 0,
          positive: Number.isFinite(positive) ? positive : 0,
          negative: Number.isFinite(negative) ? negative : 0,
          neutral: Number.isFinite(Number(val.neutral))
            ? Number(val.neutral)
            : Math.max(0, (Number.isFinite(posts) ? posts : 0) - (Number.isFinite(positive) ? positive : 0) - (Number.isFinite(negative) ? negative : 0)),
          status: String(val.status || val.origin || 'auto'),
          enabled: val.enabled != null ? Boolean(val.enabled) : null,
        });
      } else {
        const posts = Number(val);
        rows.push({
          source: key,
          posts: Number.isFinite(posts) ? posts : 0,
          positive: 0,
          negative: 0,
          neutral: Number.isFinite(posts) ? posts : 0,
          status: 'auto',
          enabled: null,
        });
      }
    });
  }

  rows.sort((a, b) => (Number(b.posts) || 0) - (Number(a.posts) || 0));
  return rows;
}

function initScannerModeToggle() {
  const root = document.querySelector('.scanner-toggle');
  if (!root) return;

  root.addEventListener('click', (e) => {
    const btn = e.target && e.target.closest ? e.target.closest('button[data-agent]') : null;
    if (!btn) return;
    const agent = String(btn.getAttribute('data-agent') || 'overall').toLowerCase();
    state.scannerAgent = ['overall', 'institution', 'news', 'social'].includes(agent) ? agent : 'overall';
    root.querySelectorAll('button[data-agent]').forEach((b) => b.classList.toggle('is-active', b === btn));
    refreshScannerData();
  });
}

function parseScannerSegmentValue(rawValue) {
  const raw = String(rawValue || 'ALL:small');
  const [marketRaw, segmentRaw] = raw.split(':');
  const market = String(marketRaw || 'US').trim().toUpperCase();
  const segment = String(segmentRaw || 'large').trim().toLowerCase();
  return {
    market: ['US', 'AU', 'ALL'].includes(market) ? market : 'ALL',
    segment: ['large', 'mid', 'small'].includes(segment) ? segment : 'small',
  };
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

function scannerParams({ refresh = false } = {}) {
  return {
    tab: state.scannerAgent || 'overall',
    market: state.scannerMarket || 'ALL',
    segment: state.scannerSegment || 'small',
    interval: '1day',
    bars: 60,
    limit: 20,
    refresh: Boolean(refresh),
  };
}

function scheduleScannerStatusPoll(nextMs) {
  if (state.scannerPollTimer) {
    window.clearTimeout(state.scannerPollTimer);
    state.scannerPollTimer = null;
  }
  state.scannerPollTimer = window.setTimeout(() => {
    fetchScannerStatus().catch(() => {});
  }, Math.max(1000, Number(nextMs) || 10000));
}

function renderScannerRuntimeStatus() {
  if (!scannerStatusRow || !scannerStatusPill || !scannerProgressTrack || !scannerProgressFill || !scannerStatusMeta) return;
  const st = state.scannerRuntimeStatus || { state: 'idle' };
  const mode = String(st.state || 'idle').toLowerCase();
  const progress = state.scannerRuntimeProgress && typeof state.scannerRuntimeProgress === 'object'
    ? state.scannerRuntimeProgress
    : null;
  const pct = Number(progress?.pct);
  const pctSafe = Number.isFinite(pct) ? Math.max(0, Math.min(100, pct)) : null;
  const stage = String(progress?.stage || '').trim();
  const eta = Number(progress?.eta_s);
  let pillClass = 'pill';
  if (mode === 'running') pillClass = 'pill bull';
  else if (mode === 'queued') pillClass = 'pill';
  else if (mode === 'error') pillClass = 'pill bear';
  else if (state.scannerRuntimeStale) pillClass = 'pill muted-pill';
  scannerStatusPill.className = pillClass;
  scannerStatusPill.textContent = mode === 'running'
    ? 'Scanning'
    : mode === 'queued'
      ? 'Queued'
      : mode === 'error'
        ? 'Error'
        : (state.scannerRuntimeStale ? 'Stale' : 'Idle');
  scannerProgressTrack.hidden = !(mode === 'running' || mode === 'queued');
  if (mode === 'running' || mode === 'queued') {
    if (pctSafe == null) {
      scannerProgressFill.style.width = '35%';
      scannerProgressFill.style.opacity = '0.6';
    } else {
      scannerProgressFill.style.width = `${pctSafe}%`;
      scannerProgressFill.style.opacity = '1';
    }
  } else {
    scannerProgressFill.style.width = '0%';
  }
  const lastUpdated = state.scannerRuntimeLastRunAt
    ? String(state.scannerRuntimeLastRunAt).replace('T', ' ').slice(0, 19)
    : 'n/a';
  const etaText = Number.isFinite(eta) ? ` ETA ${eta}s` : '';
  const stageText = stage ? `${stage}${etaText}` : '';
  const errText = st.error ? ` • ${st.error}` : '';
  scannerStatusMeta.textContent = `${stageText ? `${stageText} • ` : ''}Updated ${lastUpdated}${errText}`;
}

async function fetchScannerStatus() {
  const params = scannerParams();
  const q = `tab=${encodeURIComponent(params.tab)}&market=${encodeURIComponent(params.market)}&segment=${encodeURIComponent(params.segment)}&interval=${encodeURIComponent(params.interval)}&bars=${encodeURIComponent(params.bars)}&limit=${encodeURIComponent(params.limit)}`;
  const result = await fetchJson(`/scanner/status?${q}`);
  if (result.ok) {
    const body = result.body || {};
    state.scannerRuntimeStatus = body.state || { state: 'idle', run_id: null, error: null };
    state.scannerRuntimeProgress = body.progress || null;
    state.scannerRuntimeStale = Boolean(body.stale);
    state.scannerRuntimeLastRunAt = body.last_run_at || null;
    const rows = body.result?.segments?.[state.scannerSegment];
    state.scannerRows = Array.isArray(rows)
      ? rows.filter((row) => String(row?.action || '').toUpperCase() === 'BUY')
      : [];
    setSectionLoading('scanner', false);
  } else {
    state.scannerRuntimeStatus = { state: 'error', run_id: null, error: getErrorMessage(result, 'status failed') };
  }
  renderScannerRuntimeStatus();
  renderScanner();
  const mode = String(state.scannerRuntimeStatus?.state || 'idle').toLowerCase();
  scheduleScannerStatusPoll(mode === 'running' || mode === 'queued' ? 2000 : 10000);
}

async function queueScannerRun(refresh = false) {
  const params = scannerParams({ refresh });
  setSectionLoading('scanner', true);
  await fetchJson('/scanner/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  });
  await fetchScannerStatus();
}

async function refreshScannerData() {
  await queueScannerRun(false);
}

async function refreshScannerDataLive() {
  await queueScannerRun(true);
}

function renderScannerRows(rows) {
  if (!scannerList) return;
  if (!rows.length) {
    scannerList.innerHTML = '<div class="scanner-empty">No BUY opportunities found for this segment right now.</div>';
    return;
  }

  scannerList.innerHTML = rows.map((row) => {
    const symbol = resolveScannerSymbol(row, row?.key || row?.id);
    if (symbol === 'UNKNOWN') {
      console.warn('Scanner row missing symbol', row);
    }
    const action = String(row.action || '').toUpperCase();
    const isNoData = action === 'NO_DATA';
    const actionPillClass = action === 'BUY' ? 'bull' : action === 'SELL' ? 'bear' : 'neutral';
    const provider = row.provider || '-';
    const timeframe = row.timeframe || '1day';
    const rrText = row.rr != null ? `RR ${Number(row.rr).toFixed(2)}` : null;
    const confValue = Number(row.confidence);
    const confPct = Number.isFinite(confValue) ? Math.max(0, Math.min(100, Math.round(confValue * 100))) : 0;
    const scoreValue = resolveScannerScore(row);
    const entryLow = resolveScannerLevelValue(row.entry_low, row.entryLow, row?.entry_zone?.low, row?.trade?.entry_zone?.low);
    const entryHigh = resolveScannerLevelValue(row.entry_high, row.entryHigh, row?.entry_zone?.high, row?.trade?.entry_zone?.high);
    const target = resolveScannerTarget(row);
    const stop = resolveScannerLevelValue(row.stop, row?.trade?.stop_loss_price);
    const trail = resolveScannerLevelValue(row.trail, row?.trade?.trailing_stop_price);
    const name = typeof row.name === 'string'
      ? row.name.trim()
      : (typeof row.company_name === 'string' ? row.company_name.trim() : '');
    const nearEntry = Array.isArray(row.tags) && row.tags.includes('Near Entry');
    const sourceSummary = row.source_summary && typeof row.source_summary === 'object' ? row.source_summary : null;
    const sourceSummaryText = sourceSummary
      ? `Posts ${Number(sourceSummary.posts || 0)} • Mentions ${Number(sourceSummary.mentions || 0)}`
      : null;
    const resolvedPrice = resolveScannerPrice(row);
    const scoreBadge = renderScoreBadge(scoreValue, {
      small: true,
      symbol,
      confidence: row.confidence,
      scoreComponents: row.score_components,
      evidence: row.evidence || row.source_summary,
    });
    const entryText = entryLow != null && entryHigh != null
      ? `${formatPrice(entryLow)}-${formatPrice(entryHigh)}`
      : '—';
    const reasons = (Array.isArray(row.reasons) ? row.reasons : []).map((r) => String(r || '').trim()).filter(Boolean).slice(0, 2);

    return `
    <article class="scan-card ${state.selectedSymbol === symbol ? 'selected' : ''}" data-panel="scanner" data-symbol="${symbol}">
      <button type="button" class="scan-main" data-action="select" data-panel="scanner" data-symbol="${symbol}">
        <div class="scan-head">
          <div class="scan-id">
            <div class="scan-symbol-wrap">
              <div class="scan-symbol">${symbol}</div>
              ${scoreBadge}
            </div>
            ${name ? `<div class="scan-name" title="${name}">${name}</div>` : ''}
          </div>
          <div class="scan-price">${scannerPriceText(resolvedPrice)}</div>
        </div>
          <div class="scan-subline">${provider} • ${timeframe}</div>
        <div class="scan-badges">
          <span class="pill pill-action ${actionPillClass}">${action || 'HOLD'}</span>
          ${!isNoData ? `<span class="scan-inline-confidence" role="img" aria-label="Confidence: ${confPct}%" title="Confidence: ${confPct}%"><span class="scan-confidence-bar"><span class="scan-confidence-fill" style="width:${confPct}%;"></span></span></span>` : ''}
          ${rrText ? `<span class="pill">${rrText}</span>` : ''}
          ${nearEntry ? '<span class="pill pill-muted">Near Entry</span>' : ''}
        </div>
        <div class="scan-levels">
          <div class="kv"><span>Entry</span><strong>${entryText}</strong></div>
          <div class="kv"><span>Target</span><strong>${target != null ? formatPrice(target) : '—'}</strong></div>
          <div class="kv"><span>Stop</span><strong>${stop != null ? formatPrice(stop) : '—'}</strong></div>
          <div class="kv"><span>Trail</span><strong>${trail != null ? formatPrice(trail) : '—'}</strong></div>
        </div>
        ${reasons.length
          ? `<div class="scan-reasons">${reasons.map((r) => `<div class="reason">• ${r}</div>`).join('')}</div>`
          : (row.explanation_short
              ? `<div class="scan-reasons"><div class="reason">• ${String(row.explanation_short)}</div></div>`
              : (row.snapshot ? `<div class="scan-reasons"><div class="reason">• ${String(row.snapshot)}</div></div>` : '')
            )
        }
        ${sourceSummaryText ? `<div class="scan-subline">${sourceSummaryText}</div>` : ''}
      </button>
      <div class="scan-actions">
        <button type="button" class="button-ghost scanner-monitor-btn scan-buy-btn" data-action="scanner-buy-now" aria-label="Buy ${symbol}" data-symbol="${symbol}" data-price="${resolvedPrice != null ? Number(resolvedPrice) : ''}">BUY NOW</button>
        <button type="button" class="button-ghost scanner-monitor-btn" data-action="scanner-watch" data-symbol="${symbol}" data-entry-low="${entryLow != null ? Number(entryLow) : ''}" data-entry-high="${entryHigh != null ? Number(entryHigh) : ''}">WATCH</button>
        <button type="button" class="button-ghost scanner-monitor-btn" data-action="scanner-monitor" data-symbol="${symbol}" data-price="${resolvedPrice != null ? Number(resolvedPrice) : ''}" data-entry-low="${entryLow != null ? Number(entryLow) : ''}" data-entry-high="${entryHigh != null ? Number(entryHigh) : ''}">MONITOR</button>
        <button type="button" class="button-ghost scanner-monitor-btn" data-action="scanner-sources" data-symbol="${symbol}">SOURCES</button>
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

function monitorDaysHeld(createdAt) {
  const ts = Date.parse(createdAt || '');
  if (Number.isNaN(ts)) return 0;
  return Math.max(0, Math.floor((Date.now() - ts) / 86400000));
}

function renderMonitorPanel() {
  if (!monitorList || !monitorTotals) return;
  if (!state.monitorRows.length) {
    monitorTotals.innerHTML = '<div class="monitor-total-item">No monitor outcomes yet.</div>';
    monitorList.innerHTML = '<div class="empty">Create one from scanner or trade panel.</div>';
    return;
  }

  const rows = state.monitorRows.map((row) => ({ ...row }));

  const totalAmount = rows.reduce((sum, row) => sum + (Number(row.buy_amount) || 0), 0);
  const totalPnl = rows.reduce((sum, row) => {
    const entryRef = row.buy_price != null
      ? Number(row.buy_price)
      : (row.buy_zone_low != null && row.buy_zone_high != null ? (Number(row.buy_zone_low) + Number(row.buy_zone_high)) / 2 : null);
    const lastPrice = Number(row.last_price);
    const amount = Number(row.buy_amount);
    if (!Number.isFinite(entryRef) || entryRef <= 0 || !Number.isFinite(lastPrice) || !Number.isFinite(amount)) return sum;
    const shares = amount / entryRef;
    return sum + ((lastPrice - entryRef) * shares);
  }, 0);
  const totalPnlPct = totalAmount > 0 ? (totalPnl / totalAmount) * 100 : 0;

  monitorTotals.innerHTML = `
    <div class="monitor-total-item"><span>Total Amount</span><strong>$${formatPrice(totalAmount)}</strong></div>
    <div class="monitor-total-item"><span>Total P/L $</span><strong class="${totalPnl >= 0 ? 'up' : 'down'}">$${formatPrice(totalPnl)}</strong></div>
    <div class="monitor-total-item"><span>Total P/L %</span><strong class="${totalPnlPct >= 0 ? 'up' : 'down'}">${formatPct(totalPnlPct)}</strong></div>
  `;

  monitorList.innerHTML = rows.map((row) => {
    const symbol = normalizeSymbol(row.symbol);
    const cached = symbol ? dataCache.get(symbol) : null;
    const cachedSignal = cached ? getSignalView(cached.signalResult) : null;
    const monitorScore = resolveScannerLevelValue(
      row.score,
      row.signal_score,
      row.scanner_score,
      cachedSignal?.score,
    );
    return `
    <article class="monitor-row">
      <div class="monitor-main">
        <div class="monitor-top">
          <span class="monitor-symbol-wrap">
            <span class="monitor-symbol">${row.symbol}</span>
            ${renderScoreBadge(monitorScore, {
              small: true,
              symbol: symbol || row.symbol,
              confidence: resolveScannerLevelValue(row.confidence, row.signal_confidence, cachedSignal?.confidence),
              scoreComponents: row.score_components || cachedSignal?.score_components,
              evidence: row.evidence || cachedSignal?.evidence,
            })}
          </span>
          <span class="monitor-price">${row.last_price != null ? `$${formatPrice(row.last_price)}` : '-'}</span>
        </div>
        <div class="monitor-meta">
          <span>Amount $${formatPrice(row.buy_amount)}</span>
          <span>Entry ${row.buy_price != null ? `$${formatPrice(row.buy_price)}` : (row.buy_zone_low != null && row.buy_zone_high != null ? `${formatPrice(row.buy_zone_low)}-${formatPrice(row.buy_zone_high)}` : '-')}</span>
          <span>P/L <strong class="${row.pnl_pct != null && Number(row.pnl_pct) >= 0 ? 'up' : 'down'}">${row.pnl_pct != null ? formatPct(row.pnl_pct) : '--'}</strong></span>
          <span>Max Up <strong class="${row.max_up_pct != null && Number(row.max_up_pct) >= 0 ? 'up' : 'down'}">${row.max_up_pct != null ? formatPct(row.max_up_pct) : '--'}</strong></span>
          <span>Max Down <strong class="${row.max_down_pct != null && Number(row.max_down_pct) >= 0 ? 'up' : 'down'}">${row.max_down_pct != null ? formatPct(row.max_down_pct) : '--'}</strong></span>
          <span>Days ${monitorDaysHeld(row.created_at)}</span>
          <span>Status ${row.status || 'open'}</span>
          <span>Checked ${row.last_checked_at ? String(row.last_checked_at).slice(0, 19).replace('T', ' ') : '-'}</span>
        </div>
      </div>
      <div class="monitor-actions">
        <button type="button" class="button-ghost" data-action="monitor-buy" data-symbol="${symbol}">Buy</button>
        ${String(row.status || 'open') === 'open' ? `<button type="button" class="button-ghost" data-action="monitor-close" data-id="${row.id}">Close</button>` : ''}
      </div>
    </article>
  `;
  }).join('');
}

async function loadMonitorList() {
  setSectionLoading('monitor', true);
  const result = await fetchJson('/monitor/list?status=open');
  state.monitorRows = result.ok && Array.isArray(result.body?.rows) ? result.body.rows : [];
  setSectionLoading('monitor', false);
  renderMonitorPanel();
}

function openMonitorModal(draft) {
  if (!monitorModal) return;
  state.monitorDraft = draft || null;
  state.monitorModalSaving = false;
  if (monitorModalSymbol) monitorModalSymbol.value = normalizeSymbol(draft?.symbol || getSymbol());
  if (monitorModalAmount) monitorModalAmount.value = String(draft?.amount || 1000);
  let defaultPrice = draft?.buy_price;
  if (defaultPrice == null) {
    const quoteText = String(quoteLast?.textContent || '').replace(/[^0-9.-]/g, '');
    const parsed = Number(quoteText);
    if (Number.isFinite(parsed) && parsed > 0) defaultPrice = parsed;
  }
  if (monitorModalPrice) monitorModalPrice.value = defaultPrice != null ? String(defaultPrice) : '';
  if (monitorModalZoneLow) monitorModalZoneLow.value = draft?.buy_zone_low != null ? String(draft.buy_zone_low) : '';
  if (monitorModalZoneHigh) monitorModalZoneHigh.value = draft?.buy_zone_high != null ? String(draft.buy_zone_high) : '';
  if (monitorModalError) {
    monitorModalError.textContent = '';
    monitorModalError.hidden = true;
  }
  if (monitorModalSuccess) {
    monitorModalSuccess.textContent = '';
    monitorModalSuccess.hidden = true;
  }
  if (monitorModalSubmit) {
    monitorModalSubmit.disabled = false;
    monitorModalSubmit.textContent = 'Save';
  }
  openModalCount += 1;
  document.body.style.overflow = 'hidden';
  monitorModal.hidden = false;
}

function closeMonitorModal() {
  if (monitorModal) monitorModal.hidden = true;
  openModalCount = Math.max(0, openModalCount - 1);
  document.body.style.overflow = openModalCount > 0 ? 'hidden' : '';
  if (monitorModalError) {
    monitorModalError.textContent = '';
    monitorModalError.hidden = true;
  }
  if (monitorModalSuccess) {
    monitorModalSuccess.textContent = '';
    monitorModalSuccess.hidden = true;
  }
  if (monitorModalSubmit) {
    monitorModalSubmit.disabled = false;
    monitorModalSubmit.textContent = 'Save';
  }
  state.monitorModalSaving = false;
  state.monitorDraft = null;
}

async function submitMonitorModal() {
  if (state.monitorModalSaving) return;
  const symbol = normalizeSymbol(monitorModalSymbol?.value || '');
  const buyAmount = Number(monitorModalAmount?.value);
  const buyPrice = monitorModalPrice && monitorModalPrice.value !== '' ? Number(monitorModalPrice.value) : null;
  const buyZoneLow = monitorModalZoneLow && monitorModalZoneLow.value !== '' ? Number(monitorModalZoneLow.value) : null;
  const buyZoneHigh = monitorModalZoneHigh && monitorModalZoneHigh.value !== '' ? Number(monitorModalZoneHigh.value) : null;

  const setModalError = (msg) => {
    if (monitorModalError) {
      monitorModalError.textContent = msg;
      monitorModalError.hidden = false;
    }
  };

  if (monitorModalError) {
    monitorModalError.textContent = '';
    monitorModalError.hidden = true;
  }
  if (monitorModalSuccess) {
    monitorModalSuccess.textContent = '';
    monitorModalSuccess.hidden = true;
  }

  if (!symbol || !Number.isFinite(buyAmount) || buyAmount <= 0) {
    setModalError('Symbol and buy amount (> 0) are required.');
    return;
  }
  if (buyPrice != null && (!Number.isFinite(buyPrice) || buyPrice <= 0)) {
    setModalError('Buy price must be > 0 when provided.');
    return;
  }
  const zoneLowProvided = buyZoneLow != null;
  const zoneHighProvided = buyZoneHigh != null;
  if (zoneLowProvided !== zoneHighProvided) {
    setModalError('Provide both buy zone low and high, or leave both blank.');
    return;
  }
  if (zoneLowProvided && zoneHighProvided && buyZoneLow > buyZoneHigh) {
    setModalError('Buy zone low must be less than or equal to buy zone high.');
    return;
  }

  const payload = { symbol, buy_amount: buyAmount };
  if (Number.isFinite(buyPrice)) payload.buy_price = buyPrice;
  if (Number.isFinite(buyZoneLow)) payload.buy_zone_low = buyZoneLow;
  if (Number.isFinite(buyZoneHigh)) payload.buy_zone_high = buyZoneHigh;

  state.monitorModalSaving = true;
  if (monitorModalSubmit) {
    monitorModalSubmit.disabled = true;
    monitorModalSubmit.textContent = 'Saving...';
  }

  try {
    const controller = new AbortController();
    let timedOut = false;
    const timeoutId = window.setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, 10000);
    const result = await fetchJson('/monitor/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    window.clearTimeout(timeoutId);
    if (timedOut) {
      setModalError('Request timed out after 10s. Please try again.');
      return;
    }
    if (!result.ok) {
      setModalError(getErrorMessage(result, 'Failed to create monitor position'));
      return;
    }
    if (monitorModalSuccess) {
      monitorModalSuccess.textContent = 'Saved';
      monitorModalSuccess.hidden = false;
    }
    closeMonitorModal();
    await loadMonitorList();
  } catch (error) {
    const msg = error?.name === 'AbortError'
      ? 'Request timed out after 10s. Please try again.'
      : (error?.message || 'Failed to create monitor position');
    setModalError(msg);
  } finally {
    state.monitorModalSaving = false;
    if (monitorModalSubmit) {
      monitorModalSubmit.disabled = false;
      monitorModalSubmit.textContent = 'Save';
    }
  }
}

async function refreshMonitor() {
  await fetchJson('/monitor/refresh', { method: 'POST' });
  await loadMonitorList();
}

async function refreshPaperTrading() {
  const [statusResult, positionsResult, ordersResult] = await Promise.all([
    fetchJson('/paper/status'),
    fetchJson('/paper/positions'),
    fetchJson('/paper/orders'),
  ]);
  state.paperStatus = statusResult.ok ? (statusResult.body || {}) : {};
  state.paperStrategies = Array.isArray(state.paperStatus?.strategies) ? state.paperStatus.strategies : [];
  state.paperPositions = positionsResult.ok && Array.isArray(positionsResult.body?.rows)
    ? positionsResult.body.rows
    : [];
  state.paperRecentOrders = ordersResult.ok && Array.isArray(ordersResult.body?.rows)
    ? ordersResult.body.rows
    : [];
  renderPaperTrading();
}

function addMonitorFromCurrentTrade() {
  const symbol = getSymbol();
  const tradePrice = Number(state.latestTrade?.last_close);
  const entryLow = Number(state.latestTrade?.entry_zone?.low);
  const entryHigh = Number(state.latestTrade?.entry_zone?.high);
  const draft = { symbol, amount: 1000 };
  if (Number.isFinite(tradePrice)) draft.buy_price = tradePrice;
  if (Number.isFinite(entryLow)) draft.buy_zone_low = entryLow;
  if (Number.isFinite(entryHigh)) draft.buy_zone_high = entryHigh;
  openMonitorModal(draft);
}

function renderScannerSourcesModal() {
  if (!scannerSourcesList || !scannerSourcesTotals) return;
  const activeKey = scannerSourcesCacheKey(
    state.scannerSourcesSymbol,
    state.scannerSourcesChannel || state.scannerAgent || 'overall',
    state.scannerSourcesTimeframe || '1day'
  );
  const payload = state.scannerSourcesByKey[activeKey] || state.scannerSources;
  const sources = normaliseSourcesPayload(payload);
  const totals = payload?.totals || {};
  const symbol = state.scannerSourcesSymbol || '-';
  const scannerType = state.scannerSourcesChannel || state.scannerAgent || 'overall';

  if (scannerSourcesTitle) {
    scannerSourcesTitle.textContent = `Sources • ${symbol} (${scannerType})`;
  }
  if (!sources.length) {
    if (scannerSourcesTotals) {
      scannerSourcesTotals.innerHTML = '<span class="pill">No source breakdown available for this item yet</span>';
    }
    scannerSourcesList.innerHTML = `
      <div class="empty">No source breakdown available for this item yet.</div>
      <details class="details">
        <summary>Raw JSON</summary>
        <pre>${asJson(payload || {})}</pre>
      </details>
    `;
    return;
  }

  const totalsPosts = sources.reduce((acc, src) => acc + (Number(src.posts) || 0), 0);
  const totalsPos = sources.reduce((acc, src) => acc + (Number(src.positive) || 0), 0);
  const totalsNeg = sources.reduce((acc, src) => acc + (Number(src.negative) || 0), 0);
  const totalsNet = totalsPos - totalsNeg;

  if (scannerSourcesTotals) {
    scannerSourcesTotals.innerHTML = `
      <span class="pill">Posts ${totalsPosts}</span>
      <span class="pill">Positive ${totalsPos}</span>
      <span class="pill">Negative ${totalsNeg}</span>
      <span class="pill">Net ${totalsNet >= 0 ? '+' : ''}${totalsNet}</span>
      ${totals?.mentions != null ? `<span class="pill">Mentions ${totals.mentions}</span>` : ''}
    `;
  }

  const tableRows = sources.map((src) => {
    const pos = Number(src.positive) || 0;
    const neg = Number(src.negative) || 0;
    const neu = Number(src.neutral) || 0;
    const net = pos - neg;
    const statusText = `${src.status || 'auto'}${src.enabled === null ? '' : (src.enabled ? ' • enabled' : ' • disabled')}`;
    return `
      <tr>
        <td>${src.source}</td>
        <td>${Number(src.posts) || 0}</td>
        <td>${pos}</td>
        <td>${neg}</td>
        <td>${neu}</td>
        <td>${net >= 0 ? '+' : ''}${net}</td>
        <td>${statusText}</td>
      </tr>
    `;
  }).join('');
  scannerSourcesList.innerHTML = `
    <div class="table-wrap source-table-wrap">
      <table class="source-table">
        <thead>
          <tr><th>Source</th><th>Posts</th><th>Positive</th><th>Negative</th><th>Neutral</th><th>Net</th><th>Status</th></tr>
        </thead>
        <tbody>${tableRows}</tbody>
        <tfoot>
          <tr><th>Totals</th><th>${totalsPosts}</th><th>${totalsPos}</th><th>${totalsNeg}</th><th>${Math.max(0, totalsPosts - totalsPos - totalsNeg)}</th><th>${totalsNet >= 0 ? '+' : ''}${totalsNet}</th><th>-</th></tr>
        </tfoot>
      </table>
    </div>
  `;
}

function openScannerSourcesModal(symbol) {
  if (!scannerSourcesModal) return;
  state.scannerSourcesSymbol = normalizeSymbol(symbol);
  state.scannerSourcesChannel = String(state.scannerAgent || 'overall').toLowerCase();
  state.scannerSourcesTimeframe = getInterval();
  state.scannerSources = null;
  state.scannerSourcesLoading = false;
  if (scannerSourcesError) {
    scannerSourcesError.textContent = '';
    scannerSourcesError.hidden = true;
  }
  if (scannerSourcesList) {
    scannerSourcesList.innerHTML = '<div class="empty">Loading sources...</div>';
  }
  openModalCount += 1;
  document.body.style.overflow = 'hidden';
  scannerSourcesModal.hidden = false;
  fetchScannerSources();
}

function closeScannerSourcesModal() {
  if (!scannerSourcesModal) return;
  scannerSourcesModal.hidden = true;
  openModalCount = Math.max(0, openModalCount - 1);
  document.body.style.overflow = openModalCount > 0 ? 'hidden' : '';
  state.scannerSourcesLoading = false;
}

async function fetchScannerSources() {
  const symbol = state.scannerSourcesSymbol;
  if (!symbol || state.scannerSourcesLoading) return;
  const channel = state.scannerSourcesChannel || state.scannerAgent || 'overall';
  const timeframe = state.scannerSourcesTimeframe || getInterval();
  const key = scannerSourcesCacheKey(symbol, channel, timeframe);
  if (state.scannerSourcesByKey[key]) {
    state.scannerSources = state.scannerSourcesByKey[key];
    renderScannerSourcesModal();
    return;
  }
  state.scannerSourcesLoading = true;
  try {
    const url = `/scanner/sources?symbol=${encodeURIComponent(symbol)}&channel=${encodeURIComponent(channel)}&timeframe=${encodeURIComponent(timeframe)}`;
    const result = await fetchJson(url);
    if (!result.ok) {
      if (scannerSourcesError) {
        scannerSourcesError.textContent = getErrorMessage(result, 'Failed to load sources');
        scannerSourcesError.hidden = false;
      }
      state.scannerSources = { sources: [], totals: {} };
      renderScannerSourcesModal();
      return;
    }
    state.scannerSources = result.body || { sources: [], totals: {} };
    state.scannerSourcesByKey[key] = state.scannerSources;
    renderScannerSourcesModal();
  } catch (error) {
    if (scannerSourcesError) {
      scannerSourcesError.textContent = error?.message || 'Failed to load sources';
      scannerSourcesError.hidden = false;
    }
  } finally {
    state.scannerSourcesLoading = false;
  }
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

async function fetchJson(url, options = undefined) {
  try {
    const response = await fetch(url, options);
    const text = await response.text();
    const contentType = String(response.headers.get('content-type') || '').toLowerCase();
    const isJson = contentType.includes('application/json') || contentType.includes('+json');
    let data = {};
    if (text) {
      if (isJson) {
        try {
          data = JSON.parse(text);
        } catch {
          data = { error: 'Paper trade API returned non-JSON. Check server logs.', detail: 'Failed to parse JSON body' };
        }
      } else {
        data = {
          error: 'Paper trade API returned non-JSON. Check server logs.',
          detail: `HTTP ${response.status} ${text.slice(0, 200)}`,
        };
      }
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
    score_components: body.score_components && typeof body.score_components === 'object' ? body.score_components : null,
    evidence: body.evidence && typeof body.evidence === 'object' ? body.evidence : null,
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

function clampScoreForBadge(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.max(-100, Math.min(100, num));
}

function scoreHueForBadge(score) {
  const clamped = clampScoreForBadge(score);
  if (clamped == null) return 0;
  if (clamped >= 0) {
    return Math.round(45 + (clamped * 0.75));
  }
  return Math.max(0, Math.min(25, Math.round(25 + (clamped * 0.25))));
}

function scoreRingStrokeForBadge(score) {
  const clamped = clampScoreForBadge(score);
  if (clamped == null || (clamped >= -10 && clamped <= 10)) {
    return 'hsl(0 0% 55%)';
  }
  const hue = scoreHueForBadge(clamped);
  return `hsl(${hue} 70% 45%)`;
}

function formatScoreBadgeText(rawScore) {
  if (rawScore == null) return '-';
  const original = String(rawScore).trim();
  const clamped = clampScoreForBadge(rawScore);
  if (clamped == null) return '-';
  const rounded = String(Math.round(Math.abs(clamped)));
  if (original.startsWith('+')) return `+${rounded}`;
  if (original.startsWith('-') || clamped < 0) return `-${rounded}`;
  return String(Math.round(clamped));
}

function renderScoreBadge(rawScore, options = {}) {
  const {
    small = false,
    loading = false,
    symbol = '',
    confidence = null,
    scoreComponents = null,
    evidence = null,
  } = options;
  const clamped = clampScoreForBadge(rawScore);
  const hue = scoreHueForBadge(rawScore);
  const scoreText = formatScoreBadgeText(rawScore);
  const ariaValue = clamped == null ? 'unavailable' : String(Math.round(clamped));
  const strength = clamped == null ? 0 : Math.abs(clamped) / 100;
  const radius = 15;
  const circumference = 2 * Math.PI * radius;
  const dashOffset = circumference * (1 - strength);
  const ringStroke = scoreRingStrokeForBadge(clamped);
  const normalized = normalizeScoreComponents(scoreComponents);
  const componentsEncoded = normalized
    ? encodeURIComponent(JSON.stringify(normalized))
    : '';
  const evidenceEncoded = evidence && typeof evidence === 'object'
    ? encodeURIComponent(JSON.stringify(evidence))
    : '';
  const confNum = Number(confidence);
  const className = ['score-badge', small ? 'score-badge--sm' : '', loading ? 'skeleton-chip' : '']
    .filter(Boolean)
    .join(' ');

  const badgeClasses = className;
  return `<span class="${badgeClasses}"
      style="--sb-h:${hue}; --sb-s:70%; --sb-l:40%; --sb-circ:${circumference}; --sb-offset:${dashOffset}; --sb-ring-colour:${ringStroke};"
      data-score="${ariaValue}"
      data-confidence="${Number.isFinite(confNum) ? String(confNum) : ''}"
      data-components="${componentsEncoded}"
      data-evidence="${evidenceEncoded}"
      aria-label="Score ${ariaValue}"
      title="Signal strength score"
      tabindex="0">
      <svg class="score-badge-svg" viewBox="0 0 36 36" aria-hidden="true" focusable="false">
        <circle class="score-ring-bg" cx="18" cy="18" r="${radius}"></circle>
        <circle class="score-ring-fill" cx="18" cy="18" r="${radius}"></circle>
      </svg>
      <span class="score-badge-value">${scoreText}</span>
    </span>`;
}

let scoreTooltipEl = null;
let scoreTooltipPinned = false;
const scoreBadgesLegendOrder = ["technical", "institution", "news", "social"];

function scoreMeaningBucket(score) {
  if (score >= 80) return 'Multiple indicators aligned. High probability upward momentum.';
  if (score >= 60) return 'Positive indicators present. Momentum building.';
  if (score >= 40) return 'Early signal forming but confirmation required.';
  if (score >= 20) return 'Insufficient strength for trade.';
  if (score >= -20) return 'No directional advantage.';
  if (score >= -60) return 'Indicators suggesting downside risk.';
  return 'Clear negative trend across indicators.';
}

function scoreDirectionText(score) {
  if (score == null) return 'Neutral';
  if (score > 0) return 'Bullish';
  if (score < 0) return 'Bearish';
  return 'Neutral';
}

function decodeDataJson(raw) {
  if (!raw || typeof raw !== 'string') return null;
  try {
    const decoded = decodeURIComponent(raw);
    const parsed = JSON.parse(decoded);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

function normalizeScoreComponents(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const technical = Math.max(0, Number(raw.technical) || 0);
  const institution = Math.max(0, Number(raw.institution) || 0);
  const news = Math.max(0, Number(raw.news) || 0);
  const social = Math.max(0, Number(raw.social) || 0);
  const total = technical + institution + news + social;
  if (!(total > 0)) return null;
  return {
    technical: technical / total,
    institution: institution / total,
    news: news / total,
    social: social / total,
  };
}

function buildScoreSegmentMarkup({ components, radius, circumference, hue }) {
  const segOrder = scoreBadgesLegendOrder;
  const lightnessByKey = {
    technical: 38,
    institution: 42,
    news: 46,
    social: 50,
  };
  const gap = 1.4;
  let offset = 0;
  return segOrder.map((key) => {
    const share = Number(components[key]) || 0;
    if (!(share > 0)) return '';
    const segmentLength = Math.max(0, (circumference * share) - gap);
    const dashArray = `${segmentLength} ${Math.max(0, circumference - segmentLength)}`;
    const dashOffset = -offset;
    offset += (circumference * share);
    return `<circle class="score-ring-segment score-ring-segment--${key}" cx="17" cy="17" r="${radius}" stroke="hsl(${hue} 70% ${lightnessByKey[key]}%)" stroke-dasharray="${dashArray}" stroke-dashoffset="${dashOffset}"></circle>`;
  }).join('');
}

function scoreBreakdownRows(components) {
  if (!components) return '';
  const labels = {
    technical: 'Technical',
    institution: 'Institution',
    news: 'News',
    social: 'Social',
  };
  const rows = scoreBadgesLegendOrder.map((key) => {
    const value = Math.round((Number(components[key]) || 0) * 100);
    return `<div><strong>${labels[key]}:</strong> ${value}%</div>`;
  }).join('');
  return `<div class="score-tooltip-breakdown"><div class="score-tooltip-subtitle">Breakdown</div>${rows}</div>`;
}

function ensureScoreTooltip() {
  if (scoreTooltipEl) return scoreTooltipEl;
  scoreTooltipEl = document.createElement('div');
  scoreTooltipEl.className = 'score-badge-tooltip';
  scoreTooltipEl.setAttribute('role', 'tooltip');
  scoreTooltipEl.hidden = true;
  document.body.appendChild(scoreTooltipEl);
  return scoreTooltipEl;
}

function scoreTooltipHtml(rawScore) {
  const score = clampScoreForBadge(rawScore);
  const scoreText = score == null ? 'unavailable' : String(Math.round(score));
  const direction = scoreDirectionText(score);
  const meaning = score == null ? 'No directional advantage.' : scoreMeaningBucket(score);
  const directionGlyph = score == null ? '→' : (score >= 0 ? '↑' : '↓');
  return `
    <div class="score-tooltip-title">Signal Strength</div>
    <div class="score-tooltip-score">${directionGlyph} Score: ${scoreText} (${direction})</div>
    <div class="score-tooltip-meaning">${meaning}</div>
    <div class="score-tooltip-ranges">
      <div><strong>80-100</strong> Strong BUY signal</div>
      <div><strong>60-79</strong> BUY bias</div>
      <div><strong>40-59</strong> Candidate opportunity</div>
      <div><strong>20-39</strong> Weak signal</div>
      <div><strong>-20-19</strong> Neutral</div>
      <div><strong>-21--60</strong> Bearish bias</div>
      <div><strong>-61--100</strong> Strong SELL signal</div>
    </div>
    <div class="score-tooltip-note">
      Score is calculated from combined signals including:
      <span>• price momentum</span>
      <span>• institutional activity</span>
      <span>• social sentiment</span>
      <span>• news signals</span>
      <span>• technical indicators</span>
    </div>
    <div class="score-tooltip-tail">The higher the score, the stronger the opportunity. Negative scores indicate downside risk.</div>
  `;
}

function scoreTooltipHtmlFromBadge(target) {
  const components = normalizeScoreComponents(decodeDataJson(target?.dataset?.components || ''));
  const evidence = decodeDataJson(target?.dataset?.evidence || '');
  const base = scoreTooltipHtml(target?.dataset?.score);
  const breakdown = scoreBreakdownRows(components);
  let evidenceBlock = '';
  if (evidence && typeof evidence === 'object') {
    const posts = Number(evidence.posts);
    const net = Number(evidence.net);
    if (Number.isFinite(posts) || Number.isFinite(net)) {
      evidenceBlock = `<div class="score-tooltip-evidence">${Number.isFinite(posts) ? `Posts: ${posts}` : ''}${Number.isFinite(posts) && Number.isFinite(net) ? ', ' : ''}${Number.isFinite(net) ? `Net: ${net >= 0 ? '+' : ''}${net}` : ''}</div>`;
    }
  }
  return `
    ${base}
    ${breakdown}
    ${evidenceBlock}
  `;
}

function placeScoreTooltip(target) {
  const tip = ensureScoreTooltip();
  const rect = target.getBoundingClientRect();
  const width = tip.offsetWidth || 320;
  const top = Math.max(8, rect.top - 12);
  const left = Math.min(window.innerWidth - width - 8, Math.max(8, rect.left + rect.width / 2 - width / 2));
  tip.style.top = `${top}px`;
  tip.style.left = `${left}px`;
  tip.style.transform = 'translateY(-100%)';
}

function showScoreTooltip(target, pinned) {
  if (!target) return;
  const tip = ensureScoreTooltip();
  tip.innerHTML = scoreTooltipHtmlFromBadge(target);
  tip.hidden = false;
  scoreTooltipPinned = Boolean(pinned);
  placeScoreTooltip(target);
}

function hideScoreTooltip(force = false) {
  if (!scoreTooltipEl) return;
  if (!force && scoreTooltipPinned) return;
  scoreTooltipEl.hidden = true;
  scoreTooltipPinned = false;
}

function setInlineButtonFeedback(button, message, isError) {
  if (!button || !button.parentElement) return;
  const host = button.parentElement;
  let el = host.querySelector('.inline-buy-feedback');
  if (!el) {
    el = document.createElement('span');
    el.className = 'inline-buy-feedback';
    el.style.fontSize = '0.72rem';
    el.style.marginLeft = '6px';
    host.appendChild(el);
  }
  el.textContent = message;
  el.style.color = isError ? '#b91c1c' : '#166534';
  window.setTimeout(() => {
    if (el && el.parentElement && el.textContent === message) {
      el.textContent = '';
    }
  }, 2800);
}

function inferPaperStrategyKey(source) {
  const src = String(source || '').toLowerCase();
  if (src.includes('scanner-social')) return 'scanner_social_v1';
  if (src.includes('scanner-news')) return 'scanner_news_v1';
  if (src.includes('scanner-institution')) return 'scanner_institution_v1';
  if (src.includes('scanner')) return 'scanner_overall_v1';
  return 'manual';
}

function paperStrategyOptionsHtml(selected) {
  const rows = Array.isArray(state.paperStrategies) && state.paperStrategies.length
    ? state.paperStrategies
    : [
      { key: 'manual', label: 'Manual' },
      { key: 'scanner_overall_v1', label: 'Scanner Overall v1' },
      { key: 'scanner_social_v1', label: 'Scanner Social v1' },
      { key: 'scanner_news_v1', label: 'Scanner News v1' },
      { key: 'scanner_institution_v1', label: 'Scanner Institution v1' },
    ];
  return rows
    .map((row) => {
      const key = String(row.key || row.strategy_key || 'manual');
      const label = String(row.label || row.name || key);
      return `<option value="${key}" ${key === selected ? 'selected' : ''}>${label}</option>`;
    })
    .join('');
}

function openPaperOrderModal({ symbol, defaultAmount, strategyKey, tacticLabel, source, button } = {}) {
  if (!paperOrderModal) return;
  const symbolNorm = normalizeSymbol(symbol || getSymbol());
  if (!symbolNorm) return;

  const amountNum = Number(defaultAmount);
  const amount = Number.isFinite(amountNum) && amountNum > 0
    ? amountNum
    : Number(state.paperLastAmountUsd || PAPER_DEFAULT_AMOUNT);
  const chosenStrategy = String(strategyKey || inferPaperStrategyKey(source) || 'manual');
  const tactic = String(tacticLabel || '').trim();
  paperOrderDraft = {
    side: 'BUY',
    symbol: symbolNorm,
    source: source || 'ui',
    button: button || null,
  };
  if (paperOrderModalTitle) paperOrderModalTitle.textContent = `Paper Buy • ${symbolNorm}`;
  if (paperOrderModalSymbol) paperOrderModalSymbol.value = symbolNorm;
  if (paperOrderModalAmount) paperOrderModalAmount.value = String(Math.max(1, Math.round(amount)));
  if (paperOrderModalStrategy) paperOrderModalStrategy.innerHTML = paperStrategyOptionsHtml(chosenStrategy);
  if (paperOrderModalTactic) paperOrderModalTactic.value = tactic;
  if (paperOrderModalError) {
    paperOrderModalError.textContent = '';
    paperOrderModalError.hidden = true;
  }
  if (paperOrderModalSuccess) {
    paperOrderModalSuccess.textContent = '';
    paperOrderModalSuccess.hidden = true;
  }
  if (paperOrderModalSubmit) {
    paperOrderModalSubmit.disabled = false;
    paperOrderModalSubmit.textContent = 'Place Paper Buy';
  }
  openModalCount += 1;
  document.body.style.overflow = 'hidden';
  paperOrderModal.hidden = false;
}

function closePaperOrderModal() {
  if (!paperOrderModal) return;
  paperOrderModal.hidden = true;
  openModalCount = Math.max(0, openModalCount - 1);
  document.body.style.overflow = openModalCount > 0 ? 'hidden' : '';
  paperOrderDraft = null;
}

async function submitPaperOrderModal() {
  if (!paperOrderDraft || !paperOrderModalSymbol) return;
  const symbolNorm = normalizeSymbol(paperOrderModalSymbol.value);
  const amount = Number(paperOrderModalAmount?.value);
  const strategyKey = String(paperOrderModalStrategy?.value || 'manual').trim() || 'manual';
  const tacticLabel = String(paperOrderModalTactic?.value || '').trim() || null;
  const source = String(paperOrderDraft.source || 'ui');
  const draftButton = paperOrderDraft.button || null;

  if (!symbolNorm || !Number.isFinite(amount) || amount <= 0) {
    if (paperOrderModalError) {
      paperOrderModalError.textContent = 'Symbol and buy amount (> 0) are required.';
      paperOrderModalError.hidden = false;
    }
    return;
  }

  if (paperBuyInFlightBySymbol.get(symbolNorm)) return;
  paperBuyInFlightBySymbol.set(symbolNorm, true);
  state.paperLastAmountUsd = amount;

  if (paperOrderModalError) {
    paperOrderModalError.textContent = '';
    paperOrderModalError.hidden = true;
  }
  if (paperOrderModalSuccess) {
    paperOrderModalSuccess.textContent = '';
    paperOrderModalSuccess.hidden = true;
  }
  if (paperOrderModalSubmit) {
    paperOrderModalSubmit.disabled = true;
    paperOrderModalSubmit.textContent = 'Placing...';
  }
  if (draftButton) {
    draftButton.disabled = true;
    draftButton.dataset.prevText = draftButton.textContent || 'BUY NOW';
    draftButton.textContent = 'Buying...';
  }

  try {
    const res = await fetchJson('/paper/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        symbol: symbolNorm,
        side: 'BUY',
        amount_usd: amount,
        strategy_key: strategyKey,
        tactic_label: tacticLabel,
        source,
      }),
    });
    if (!res.ok || !res.body?.ok) {
      const msg = getErrorMessage(res, 'Failed to place paper BUY order');
      if (paperOrderModalError) {
        paperOrderModalError.textContent = msg;
        paperOrderModalError.hidden = false;
      }
      if (draftButton) {
        setInlineButtonFeedback(draftButton, msg, true);
      }
      return;
    }

    await refreshPaperTrading();
    if (paperOrderModalSuccess) {
      const fill = Number(res.body?.order?.fill_price);
      paperOrderModalSuccess.textContent = Number.isFinite(fill)
        ? `Paper BUY placed: ${symbolNorm} $${formatPrice(amount)} @ $${formatPrice(fill)}`
        : `Paper BUY placed: ${symbolNorm} $${formatPrice(amount)}`;
      paperOrderModalSuccess.hidden = false;
    }
    if (draftButton) {
      draftButton.textContent = 'Added';
      setInlineButtonFeedback(draftButton, 'Added to Paper Trading', false);
      window.setTimeout(() => {
        if (draftButton.dataset.prevText) {
          draftButton.textContent = draftButton.dataset.prevText;
        }
      }, 1200);
    }
    window.setTimeout(() => {
      closePaperOrderModal();
    }, 450);
  } finally {
    paperBuyInFlightBySymbol.delete(symbolNorm);
    if (paperOrderModalSubmit) {
      paperOrderModalSubmit.disabled = false;
      paperOrderModalSubmit.textContent = 'Place Paper Buy';
    }
    if (draftButton) {
      draftButton.disabled = false;
      if (draftButton.dataset.prevText && draftButton.textContent === 'Buying...') {
        draftButton.textContent = draftButton.dataset.prevText;
      }
    }
  }
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

function renderCardDetails(row, panelName = '') {
  const debug = row.signal.debug || {};
  const watchlistActions = panelName === 'watchlist'
    ? `
      <div class="watchlist-expand-actions">
        <button type="button" class="button-ghost watchlist-buy-btn" data-action="watchlist-buy" data-symbol="${row.symbol}">Buy</button>
      </div>
    `
    : '';

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
      ${watchlistActions}
    </div>
  `;
}

function sentimentChipText(row) {
  const trend = row.hasData ? row.signal.trend : '...';
  const momentum = row.hasData ? row.signal.momentum : '...';
  return { trend, momentum };
}

function trendStateForWatchlist(row) {
  const trendRaw = String(row?.signal?.trend || '').toLowerCase();
  const momentumRaw = String(row?.signal?.momentum || '').toLowerCase();
  const scoreNum = Number(row?.signal?.score);

  let stateLabel = 'neutral';
  if (trendRaw.includes('bull')) {
    stateLabel = 'bullish';
  } else if (trendRaw.includes('bear')) {
    stateLabel = 'bearish';
  } else if (Number.isFinite(scoreNum)) {
    if (scoreNum >= 20) stateLabel = 'bullish';
    else if (scoreNum <= -20) stateLabel = 'bearish';
    else stateLabel = 'neutral';
  }

  if (
    stateLabel === 'neutral'
    && momentumRaw.includes('negative')
    && Number.isFinite(scoreNum)
    && scoreNum > -20
    && scoreNum < 20
  ) {
    stateLabel = 'caution';
  }
  return stateLabel;
}

function trendTooltipForState(stateLabel) {
  if (stateLabel === 'bullish') return 'Bullish';
  if (stateLabel === 'bearish') return 'Bearish';
  if (stateLabel === 'caution') return 'Caution';
  return 'Neutral';
}

function renderWatchlistTrendIcon(row, loading) {
  const stateLabel = trendStateForWatchlist(row);
  const tip = trendTooltipForState(stateLabel);
  const classes = [
    'trend-icon',
    `trend-icon--${stateLabel}`,
    'trend-icon--watchlist-flat',
    loading ? 'skeleton-chip' : '',
  ].filter(Boolean).join(' ');
  return `
    <span
      class="${classes}"
      role="img"
      aria-label="${tip}"
      title="${tip}"
    >
      <span class="trend-icon-glyph" aria-hidden="true"></span>
    </span>
  `;
}

function indicatorPillClass(kind, stateLabel) {
  const safeKind = kind === 'momentum' ? 'momentum' : 'trend';
  let tone = 'neutral';
  if (stateLabel === 'bullish' || stateLabel === 'up') tone = 'bull';
  else if (stateLabel === 'bearish' || stateLabel === 'down') tone = 'bear';
  else if (stateLabel === 'caution' || stateLabel === 'volatile') tone = 'warn';
  return `indicator-pill indicator-pill--${safeKind} indicator-pill--${tone}`;
}

function momentumStateForRow(row) {
  const momentumRaw = String(row?.signal?.momentum || '').toLowerCase();
  if (momentumRaw.includes('volatile') || momentumRaw.includes('unstable')) return 'volatile';
  if (
    momentumRaw.includes('bull')
    || momentumRaw.includes('positive')
    || momentumRaw.includes('up')
    || momentumRaw.includes('increasing')
  ) {
    return 'up';
  }
  if (
    momentumRaw.includes('bear')
    || momentumRaw.includes('negative')
    || momentumRaw.includes('down')
    || momentumRaw.includes('weak')
  ) {
    return 'down';
  }
  return 'neutral';
}

function momentumTooltipForState(stateLabel) {
  if (stateLabel === 'up') return 'Momentum increasing';
  if (stateLabel === 'down') return 'Momentum weakening';
  if (stateLabel === 'volatile') return 'Momentum unstable';
  return 'Momentum neutral';
}

function renderMomentumIcon(row, loading) {
  return renderMomentumIconWithOptions(row, loading, { watchlistFlat: false });
}

function renderMomentumIconWithOptions(row, loading, opts = {}) {
  const stateLabel = momentumStateForRow(row);
  const tip = momentumTooltipForState(stateLabel);
  const classes = [
    'momentum-icon',
    `momentum-icon--${stateLabel}`,
    opts.watchlistFlat ? 'momentum-icon--watchlist-flat' : '',
    loading ? 'skeleton-chip' : '',
  ].filter(Boolean).join(' ');
  return `
    <span
      class="${classes}"
      role="img"
      aria-label="${tip}"
      title="${tip}"
    >
      <span class="momentum-icon-glyph" aria-hidden="true"></span>
    </span>
  `;
}

function renderWatchlistIndicatorGroup(row, loading) {
  const labels = sentimentChipText(row);
  const trendRaw = String(labels.trend || '').trim();
  const momentumRaw = String(labels.momentum || '').trim();
  const trendLower = trendRaw.toLowerCase();
  const momentumLower = momentumRaw.toLowerCase();
  const trendTone = trendLower.includes('bull')
    ? 'green-text'
    : trendLower.includes('bear')
      ? 'red-text'
      : 'grey-text';
  const momentumTone = momentumLower.includes('positive') || momentumLower.includes('bull')
    ? 'green-text'
    : momentumLower.includes('negative') || momentumLower.includes('bear')
      ? 'red-text'
      : 'grey-text';
  const trendValue = trendRaw ? trendRaw.charAt(0).toUpperCase() + trendRaw.slice(1) : 'Neutral';
  const momentumValue = momentumRaw ? momentumRaw.charAt(0).toUpperCase() + momentumRaw.slice(1) : 'Neutral';
  return `
    <span class="watchlist-indicator-pack">
      <span class="watchlist-indicator-head">
        <span class="watchlist-indicator-label">Trend: <span class="${trendTone}">${trendValue}</span></span>
        <span class="watchlist-indicator-label">Momentum: <span class="${momentumTone}">${momentumValue}</span></span>
      </span>
    </span>
  `;
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
      const scoreBadge = renderScoreBadge(row.signal.score, {
        small: true,
        loading: !row.hasData,
        symbol: row.symbol,
        confidence: row.signal.confidence,
        scoreComponents: row.signal.score_components,
        evidence: row.signal.evidence,
      });
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
      const trendElement = panelName === 'watchlist'
        ? renderWatchlistTrendIcon(row, !row.hasData)
        : `<span class="badge ${row.hasData ? trendClass : 'neutral'} ${row.hasData ? '' : 'skeleton-chip'}">${trendText}</span>`;
      const momentumElement = renderMomentumIcon(row, !row.hasData);
      const watchlistIndicatorGroup = panelName === 'watchlist'
        ? renderWatchlistIndicatorGroup(row, !row.hasData)
        : '';

      return `
      <article class="symbol-card ${row.isSelected ? 'selected' : ''} ${loadingClass}" data-panel="${panelName}" data-symbol="${row.symbol}">
        <button type="button" class="symbol-main" data-action="select" data-panel="${panelName}" data-symbol="${row.symbol}">
          <span class="sym-with-score">
            <span class="sym">${row.symbol}</span>
            ${scoreBadge}
          </span>
          <span class="metric ${row.hasData ? '' : 'skeleton-chip'}">${priceText}</span>
          ${panelName === 'watchlist' ? watchlistIndicatorGroup : trendElement}
          ${panelName === 'watchlist' ? '' : momentumElement}
          ${errBadge}
          ${qty}
          ${pl}
        </button>
        ${row.isExpanded ? renderCardDetails(row, panelName) : ''}
      </article>
      `;
    })
    .join('');
}

function renderScanner() {
  if (scannerToggleBtn) {
    scannerToggleBtn.textContent = 'Refresh';
  }
  renderScannerRuntimeStatus();
  const rows = state.scannerRows.filter((row) => row && row.ok !== false);
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
  const symbols = state.monitorRows.map((row) => normalizeSymbol(row.symbol)).filter(Boolean);
  warmSymbols(symbols, 'monitor');
}

function renderPaperTrading() {
  if (!paperSummary || !paperPositionsBody || !paperClosedBody || !paperRunsBody) return;
  const status = state.paperStatus || {};
  const totals = status.totals || {};
  const leaderboard = Array.isArray(status.leaderboard) ? status.leaderboard : [];
  const positions = Array.isArray(state.paperPositions) ? state.paperPositions : [];
  const orders = Array.isArray(state.paperRecentOrders) ? state.paperRecentOrders : [];

  paperSummary.innerHTML = `
    <div class="monitor-total-item"><span>Open Positions</span><strong>${positions.length}</strong></div>
    <div class="monitor-total-item"><span>Open Orders</span><strong>${Number(status.open_orders_count || 0)}</strong></div>
    <div class="monitor-total-item"><span>Net P/L</span><strong class="${Number(totals.net_pnl || 0) >= 0 ? 'up' : 'down'}">$${formatPrice(Number(totals.net_pnl || 0))}</strong></div>
  `;

  paperPositionsBody.innerHTML = positions.length
    ? positions.map((row) => `
      <tr>
        <td>${row.symbol || '-'}</td>
        <td>${row.qty != null ? formatPrice(row.qty) : '-'}</td>
        <td>${row.avg_price != null ? `$${formatPrice(row.avg_price)}` : '-'}</td>
        <td>${row.last_price != null ? `$${formatPrice(row.last_price)}` : '-'}</td>
        <td class="${Number(row.unrealised_pnl || 0) >= 0 ? 'up' : 'down'}">$${formatPrice(Number(row.unrealised_pnl || 0))}</td>
        <td class="${Number(row.realised_pnl || 0) >= 0 ? 'up' : 'down'}">$${formatPrice(Number(row.realised_pnl || 0))}</td>
        <td>${row.tactic_id || '-'}</td>
      </tr>
    `).join('')
    : '<tr><td colspan="7" class="empty">No paper positions.</td></tr>';

  paperClosedBody.innerHTML = orders.length
    ? orders.slice(0, 20).map((row) => `
      <tr>
        <td>${row.symbol || '-'}</td>
        <td>${row.side || '-'}</td>
        <td>$${formatPrice(Number(row.amount_usd || row.notional || 0))}</td>
        <td>${row.status || '-'}</td>
        <td>${row.created_at ? String(row.created_at).slice(0, 19).replace('T', ' ') : '-'}</td>
      </tr>
    `).join('')
    : '<tr><td colspan="5" class="empty">No paper orders.</td></tr>';

  paperRunsBody.innerHTML = leaderboard.length
    ? leaderboard.slice(0, 10).map((row) => `
      <tr>
        <td>${row.tactic_id || '-'}</td>
        <td>${Number(row.wins || 0)}</td>
        <td>${Number(row.losses || 0)}</td>
        <td>${formatPct(Number(row.win_rate || 0) * 100)}</td>
        <td class="${Number(row.net_pnl || 0) >= 0 ? 'up' : 'down'}">$${formatPrice(Number(row.net_pnl || 0))}</td>
      </tr>
    `).join('')
    : '<tr><td colspan="5" class="empty">No runs yet.</td></tr>';
}

function renderPanels() {
  renderScanner();
  renderWatchlist();
  renderMonitor();
  renderPortfolio();
  renderPaperTrading();
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
    refreshScannerDataLive();
  });
}

if (scannerStatusRefreshBtn) {
  scannerStatusRefreshBtn.addEventListener('click', () => {
    refreshScannerDataLive();
  });
}

if (scannerSegment) {
  scannerSegment.addEventListener('change', () => {
    const parsed = parseScannerSegmentValue(scannerSegment.value);
    state.scannerMarket = parsed.market;
    state.scannerSegment = parsed.segment;
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
    refreshMonitor();
  });
}

if (paperRefreshBtn) {
  paperRefreshBtn.addEventListener('click', () => {
    refreshPaperTrading();
  });
}

if (monitorModalCancel) {
  monitorModalCancel.addEventListener('click', () => {
    closeMonitorModal();
  });
}

if (monitorModalSubmit) {
  monitorModalSubmit.addEventListener('click', async () => {
    await submitMonitorModal();
  });
}

if (monitorModal) {
  monitorModal.addEventListener('click', (event) => {
    if (event.target === monitorModal) {
      closeMonitorModal();
    }
  });
}

if (paperOrderModalCancel) {
  paperOrderModalCancel.addEventListener('click', () => {
    closePaperOrderModal();
  });
}

if (paperOrderModalSubmit) {
  paperOrderModalSubmit.addEventListener('click', async () => {
    await submitPaperOrderModal();
  });
}

if (paperOrderModal) {
  paperOrderModal.addEventListener('click', (event) => {
    if (event.target === paperOrderModal) {
      closePaperOrderModal();
    }
  });
}

if (scannerSourcesCloseBtn) {
  scannerSourcesCloseBtn.addEventListener('click', () => {
    closeScannerSourcesModal();
  });
}

if (scannerSourcesModal) {
  scannerSourcesModal.addEventListener('click', (event) => {
    if (event.target === scannerSourcesModal) {
      closeScannerSourcesModal();
    }
  });
}

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && monitorModal && !monitorModal.hidden) {
    closeMonitorModal();
    return;
  }
  if (event.key === 'Escape' && scannerSourcesModal && !scannerSourcesModal.hidden) {
    closeScannerSourcesModal();
    return;
  }
  if (event.key === 'Escape' && paperOrderModal && !paperOrderModal.hidden) {
    closePaperOrderModal();
    return;
  }
  if (event.key === 'Escape') {
    hideScoreTooltip(true);
    return;
  }
  const activeScoreBadge = document.activeElement && document.activeElement.classList?.contains('score-badge')
    ? document.activeElement
    : null;
  if (activeScoreBadge && (event.key === 'Enter' || event.key === ' ')) {
    event.preventDefault();
    if (!scoreTooltipEl || scoreTooltipEl.hidden || !scoreTooltipPinned) {
      showScoreTooltip(activeScoreBadge, true);
    } else {
      hideScoreTooltip(true);
    }
  }
});

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

document.addEventListener('mouseover', (event) => {
  const badge = event.target.closest('.score-badge');
  if (badge) {
    showScoreTooltip(badge, false);
    return;
  }
  hideScoreTooltip();
});

document.addEventListener('focusin', (event) => {
  const badge = event.target.closest('.score-badge');
  if (badge) {
    showScoreTooltip(badge, true);
  }
});

document.addEventListener('focusout', (event) => {
  const badge = event.target.closest('.score-badge');
  if (badge) {
    hideScoreTooltip(true);
  }
});

document.addEventListener('click', (event) => {
  const scoreBadge = event.target.closest('.score-badge');
  if (scoreBadge) {
    event.preventDefault();
    event.stopPropagation();
    if (!scoreTooltipEl || scoreTooltipEl.hidden || !scoreTooltipPinned) {
      showScoreTooltip(scoreBadge, true);
    } else {
      hideScoreTooltip(true);
    }
    return;
  }
  hideScoreTooltip(true);

  const watchlistBuy = event.target.closest('[data-action="watchlist-buy"]');
  if (watchlistBuy) {
    const symbol = normalizeSymbol(watchlistBuy.dataset.symbol || getSymbol());
    openPaperOrderModal({
      symbol,
      source: 'watchlist',
      strategyKey: 'manual',
      button: watchlistBuy,
      defaultAmount: state.paperLastAmountUsd || PAPER_DEFAULT_AMOUNT,
    });
    return;
  }

  const scannerBuyNow = event.target.closest('[data-action="scanner-buy-now"]');
  if (scannerBuyNow) {
    const symbol = normalizeSymbol(scannerBuyNow.dataset.symbol);
    const strategyFromTab = inferPaperStrategyKey(`scanner-${state.scannerAgent || 'overall'}`);
    openPaperOrderModal({
      symbol,
      source: `scanner-${state.scannerAgent || 'overall'}`,
      strategyKey: strategyFromTab,
      tacticLabel: `Scanner ${String(state.scannerAgent || 'overall')}`,
      button: scannerBuyNow,
      defaultAmount: state.paperLastAmountUsd || PAPER_DEFAULT_AMOUNT,
    });
    return;
  }

  const genericBuy = event.target.closest('[data-action="buy"], [data-action="paper-buy"], [data-action="buy-now"]');
  if (genericBuy) {
    const symbol = normalizeSymbol(genericBuy.dataset.symbol || getSymbol());
    openPaperOrderModal({
      symbol,
      source: 'detail',
      strategyKey: 'manual',
      button: genericBuy,
      defaultAmount: state.paperLastAmountUsd || PAPER_DEFAULT_AMOUNT,
    });
    return;
  }

  const monitorBuy = event.target.closest('[data-action="monitor-buy"]');
  if (monitorBuy) {
    const symbol = normalizeSymbol(monitorBuy.dataset.symbol || getSymbol());
    openPaperOrderModal({
      symbol,
      source: 'monitor',
      strategyKey: 'manual',
      button: monitorBuy,
      defaultAmount: state.paperLastAmountUsd || PAPER_DEFAULT_AMOUNT,
    });
    return;
  }

  const scannerWatch = event.target.closest('[data-action="scanner-watch"]');
  if (scannerWatch) {
    const symbol = normalizeSymbol(scannerWatch.dataset.symbol);
    const low = Number(scannerWatch.dataset.entryLow);
    const high = Number(scannerWatch.dataset.entryHigh);
    openMonitorModal({
      symbol,
      amount: 1000,
      buy_zone_low: Number.isFinite(low) ? low : undefined,
      buy_zone_high: Number.isFinite(high) ? high : undefined,
    });
    return;
  }

  const scannerMonitor = event.target.closest('[data-action="scanner-monitor"]');
  if (scannerMonitor) {
    const symbol = normalizeSymbol(scannerMonitor.dataset.symbol);
    const price = Number(scannerMonitor.dataset.price);
    const low = Number(scannerMonitor.dataset.entryLow);
    const high = Number(scannerMonitor.dataset.entryHigh);
    openMonitorModal({
      symbol,
      amount: 1000,
      buy_price: Number.isFinite(price) ? price : undefined,
      buy_zone_low: Number.isFinite(low) ? low : undefined,
      buy_zone_high: Number.isFinite(high) ? high : undefined,
    });
    return;
  }

  const scannerSources = event.target.closest('[data-action="scanner-sources"]');
  if (scannerSources) {
    const symbol = normalizeSymbol(scannerSources.dataset.symbol);
    if (symbol) {
      openScannerSourcesModal(symbol);
    }
    return;
  }

  const closeTarget = event.target.closest('[data-action="monitor-close"]');
  if (closeTarget) {
    const id = Number(closeTarget.dataset.id);
    if (Number.isFinite(id)) {
      fetchJson('/monitor/close', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, status: 'closed_manual' }),
      }).then(() => refreshMonitor());
    }
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
  if (scannerSegment) {
    const parsed = parseScannerSegmentValue(scannerSegment.value);
    state.scannerMarket = parsed.market;
    state.scannerSegment = parsed.segment;
  }
  initScannerModeToggle();
  renderPanels();
  await refreshScannerDataLive();
  await refreshMonitor();
  window.setInterval(() => {
    refreshMonitor();
  }, 60000);
  await refreshPaperTrading();
  window.setInterval(() => {
    refreshPaperTrading();
  }, 60000);
  await safeSelectSymbol(initial);

  // Optional: clear trade card on load so it doesn't show stale content
  if (tradeRaw) tradeRaw.textContent = '{}';
  if (tradeReasons) tradeReasons.textContent = '[]';
  setBacktestResults(null);
});

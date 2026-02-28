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
});

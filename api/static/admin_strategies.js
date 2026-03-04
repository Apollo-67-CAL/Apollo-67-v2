const el = (id) => document.getElementById(id);

const strategyLibrary = el('strategyLibrary');
const strategyPreset = el('strategyPreset');
const strategyName = el('strategyName');
const strategyInstruction = el('strategyInstruction');
const createStrategyBtn = el('createStrategyBtn');
const strategyCreateOutput = el('strategyCreateOutput');

const monitorStrategyFilter = el('monitorStrategyFilter');
const monitorSymbol = el('monitorSymbol');
const monitorBuyAmount = el('monitorBuyAmount');
const monitorQuantity = el('monitorQuantity');
const createMonitorBtn = el('createMonitorBtn');
const refreshMonitorsBtn = el('refreshMonitorsBtn');
const monitorSummary = el('monitorSummary');
const monitorsBody = el('monitorsBody');

const backtestStrategy = el('backtestStrategy');
const backtestSymbol = el('backtestSymbol');
const backtestInterval = el('backtestInterval');
const backtestLookback = el('backtestLookback');
const runBacktestBtn = el('runBacktestBtn');
const backtestMetrics = el('backtestMetrics');
const backtestChart = el('backtestChart');

let strategies = [];
let monitors = [];

async function fetchJson(url, options) {
  try {
    const res = await fetch(url, options || {});
    const text = await res.text();
    let body = {};
    try { body = text ? JSON.parse(text) : {}; } catch { body = {}; }
    return { ok: res.ok, body };
  } catch (error) {
    return { ok: false, body: { error: error?.message || 'Network error' } };
  }
}

function esc(value) {
  return String(value == null ? '' : value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function fillStrategySelects(items) {
  const options = ['<option value="">None</option>'].concat(items.map((s) => `<option value="${esc(s.id)}">${esc(s.name)}</option>`));
  monitorStrategyFilter.innerHTML = options.join('');
  backtestStrategy.innerHTML = items.map((s) => `<option value="${esc(s.id)}">${esc(s.name)}</option>`).join('');
  strategyPreset.innerHTML = items.map((s) => `<option value="${esc(s.id)}">${esc(s.name)} (${esc(s.group)})</option>`).join('');
}

function renderLibrary(items) {
  strategyLibrary.innerHTML = items.map((s) => `
    <article class="strategy-card">
      <h3>${esc(s.name)}</h3>
      <div class="muted">${esc(s.group)}</div>
      <p>${esc(s.description)}</p>
      <div class="muted">Signals: ${(s.signals_used || []).map(esc).join(', ')}</div>
      <ul>${(s.rules_summary || []).map((r) => `<li>${esc(r)}</li>`).join('')}</ul>
    </article>
  `).join('');
}

async function loadLibrary() {
  const result = await fetchJson('/admin/strategies/library');
  const items = result.ok && result.body?.ok ? (result.body.data || []) : [];
  renderLibrary(items);
  fillStrategySelects(items);
}

async function loadStrategies() {
  const result = await fetchJson('/admin/strategies');
  strategies = result.ok && result.body?.ok ? (result.body.data || []) : [];
}

function monitorStrategyName(id) {
  if (!id) return 'Unassigned';
  const found = strategies.find((s) => s.id === id);
  return found ? found.name : id;
}

function renderMonitors(summaryByStrategy) {
  monitorsBody.innerHTML = monitors.map((m) => `
    <tr>
      <td>${esc(m.id)}</td>
      <td>${esc(monitorStrategyName(m.strategy_id))}</td>
      <td>${esc(m.symbol)}</td>
      <td>${Number(m.entry_price || 0).toFixed(2)}</td>
      <td>${Number(m.quantity || 0).toFixed(4)}</td>
      <td>${m.last_price != null ? Number(m.last_price).toFixed(2) : '-'}</td>
      <td>${m.pnl != null ? Number(m.pnl).toFixed(2) : '-'}</td>
      <td>${m.pnl_pct != null ? Number(m.pnl_pct).toFixed(2) + '%' : '-'}</td>
      <td>${esc((m.updated_at || '').replace('T', ' ').slice(0, 19))}</td>
      <td><button type="button" class="button button-secondary" data-refresh-monitor="${esc(m.id)}">Refresh</button></td>
    </tr>
  `).join('');

  const lines = Object.keys(summaryByStrategy || {}).map((sid) => {
    const s = summaryByStrategy[sid] || {};
    return `${monitorStrategyName(sid)}: positions ${s.positions || 0}, total pnl ${(s.total_pnl || 0).toFixed ? s.total_pnl.toFixed(2) : s.total_pnl}, win rate ${(s.win_rate || 0).toFixed ? s.win_rate.toFixed(2) : s.win_rate}%`;
  });
  monitorSummary.textContent = lines.length ? lines.join(' | ') : 'No monitor data yet.';
}

async function loadMonitors() {
  const strategyId = monitorStrategyFilter.value || '';
  const query = strategyId ? `?strategy_id=${encodeURIComponent(strategyId)}` : '';
  const result = await fetchJson(`/admin/monitors${query}`);
  monitors = result.ok && result.body?.ok ? (result.body.data || []) : [];
  renderMonitors((result.body && result.body.summary_by_strategy) || {});
}

function renderEquityCurve(curve) {
  if (!backtestChart) return;
  const points = Array.isArray(curve) ? curve : [];
  if (!points.length) {
    backtestChart.innerHTML = '';
    return;
  }
  const values = points.map((p) => Number(p.equity || 0));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const width = 600;
  const height = 160;
  const pad = 12;
  const span = Math.max(1e-9, max - min);

  const path = points.map((p, i) => {
    const x = pad + (i / Math.max(1, points.length - 1)) * (width - pad * 2);
    const y = height - pad - ((Number(p.equity || 0) - min) / span) * (height - pad * 2);
    return `${i === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(' ');

  backtestChart.innerHTML = `
    <rect x="0" y="0" width="${width}" height="${height}" fill="#fff" stroke="#dbe3ee" />
    <path d="${path}" fill="none" stroke="#0071e3" stroke-width="2" />
  `;
}

createStrategyBtn.addEventListener('click', async () => {
  const payload = {
    preset_id: strategyPreset.value,
    name: strategyName.value || undefined,
    instruction: strategyInstruction.value || '',
  };
  const result = await fetchJson('/admin/strategies', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  strategyCreateOutput.textContent = JSON.stringify(result.body || {}, null, 2);
  await loadStrategies();
  await loadMonitors();
});

createMonitorBtn.addEventListener('click', async () => {
  const payload = {
    strategy_id: monitorStrategyFilter.value || null,
    symbol: (monitorSymbol.value || '').trim().toUpperCase(),
    buy_amount: Number(monitorBuyAmount.value || 0),
    quantity: monitorQuantity.value ? Number(monitorQuantity.value) : null,
  };
  const result = await fetchJson('/admin/monitors', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  strategyCreateOutput.textContent = JSON.stringify(result.body || {}, null, 2);
  await loadMonitors();
});

refreshMonitorsBtn.addEventListener('click', loadMonitors);
monitorStrategyFilter.addEventListener('change', loadMonitors);

document.addEventListener('click', async (event) => {
  const btn = event.target.closest('[data-refresh-monitor]');
  if (!btn) return;
  const id = btn.getAttribute('data-refresh-monitor');
  await fetchJson(`/admin/monitors/${encodeURIComponent(id)}/refresh`, { method: 'POST' });
  await loadMonitors();
});

runBacktestBtn.addEventListener('click', async () => {
  const strategyId = backtestStrategy.value;
  const symbol = (backtestSymbol.value || '').trim().toUpperCase();
  const interval = backtestInterval.value;
  const lookback = Number(backtestLookback.value || 500);
  const url = `/backtest/run?symbol=${encodeURIComponent(symbol)}&strategy_id=${encodeURIComponent(strategyId)}&interval=${encodeURIComponent(interval)}&lookback=${encodeURIComponent(String(lookback))}`;
  const result = await fetchJson(url);
  if (!result.ok || !result.body?.ok) {
    backtestMetrics.textContent = result.body?.error || 'Backtest failed';
    renderEquityCurve([]);
    return;
  }
  const m = result.body.metrics || {};
  backtestMetrics.textContent = `Return ${Number(m.total_return_pct || 0).toFixed(2)}%, Max DD ${Number(m.max_drawdown_pct || 0).toFixed(2)}%, Trades ${m.trades_count || 0}, Win rate ${Number(m.win_rate || 0).toFixed(2)}%`;
  renderEquityCurve(m.equity_curve || []);
});

window.addEventListener('DOMContentLoaded', async () => {
  await loadLibrary();
  await loadStrategies();
  await loadMonitors();
});

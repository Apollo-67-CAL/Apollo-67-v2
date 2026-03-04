const DEFAULT_STATE = {
  sentiment: {
    overall: { weight: 50, influence: 'medium' },
    institution: { weight: 50, influence: 'medium' },
    news: { weight: 50, influence: 'medium' },
    social: { weight: 50, influence: 'medium' },
  },
  active_tactic_version: 'none',
  updated_at: null,
};

const sentimentScopes = ['overall', 'institution', 'news', 'social'];
const previewParams = [
  'rsi_weight',
  'sma_weight',
  'atr_multiplier',
  'stop_sensitivity',
  'trade_threshold',
  'risk_multiplier',
  'timeframe_bias',
];

const state = {
  adminState: JSON.parse(JSON.stringify(DEFAULT_STATE)),
  generatedProfile: null,
  generatedVersion: null,
  saveTimer: null,
};

const $ = (id) => document.getElementById(id);

const saveToast = $('saveToast');
const activateToast = $('activateToast');
const sentimentSummary = $('sentimentSummary');
const saveStateBtn = $('saveStateBtn');
const tacticPreset = $('tacticPreset');
const tacticInstruction = $('tacticInstruction');
const generateBtn = $('generateBtn');
const activateBtn = $('activateBtn');
const previewBody = $('previewBody');
const activeProfileJson = $('activeProfileJson');

function showToast(el, text) {
  if (!el) return;
  el.textContent = text;
  el.hidden = false;
  window.setTimeout(() => {
    el.hidden = true;
  }, 1400);
}

function nowVersion() {
  return String(Date.now());
}

function getSentimentControls(scope) {
  return {
    weight: $(`sentiment-${scope}-weight`),
    weightNum: $(`sentiment-${scope}-weight-num`),
    influence: $(`sentiment-${scope}-influence`),
  };
}

function clampWeight(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return 50;
  return Math.max(0, Math.min(100, Math.round(n)));
}

function safeInfluence(raw) {
  const v = String(raw || '').toLowerCase();
  if (v === 'low' || v === 'medium' || v === 'high') return v;
  return 'medium';
}

function buildStateFromControls() {
  const sentiment = {};
  sentimentScopes.forEach((scope) => {
    const c = getSentimentControls(scope);
    sentiment[scope] = {
      weight: clampWeight(c.weightNum.value),
      influence: safeInfluence(c.influence.value),
    };
  });
  return {
    sentiment,
    active_tactic_version: state.adminState.active_tactic_version || 'none',
    updated_at: new Date().toISOString(),
  };
}

function applyStateToControls(nextState) {
  const merged = JSON.parse(JSON.stringify(DEFAULT_STATE));
  const incoming = nextState && typeof nextState === 'object' ? nextState : {};
  const incomingSentiment = incoming.sentiment && typeof incoming.sentiment === 'object' ? incoming.sentiment : {};

  sentimentScopes.forEach((scope) => {
    const src = incomingSentiment[scope] && typeof incomingSentiment[scope] === 'object'
      ? incomingSentiment[scope]
      : merged.sentiment[scope];
    merged.sentiment[scope] = {
      weight: clampWeight(src.weight),
      influence: safeInfluence(src.influence),
    };

    const c = getSentimentControls(scope);
    c.weight.value = String(merged.sentiment[scope].weight);
    c.weightNum.value = String(merged.sentiment[scope].weight);
    c.influence.value = merged.sentiment[scope].influence;
  });

  merged.active_tactic_version = String(incoming.active_tactic_version || 'none');
  merged.updated_at = incoming.updated_at || null;
  state.adminState = merged;
  renderSentimentSummary();
}

function renderSentimentSummary() {
  const s = state.adminState.sentiment;
  sentimentSummary.textContent =
    `Current sentiment configuration: overall ${s.overall.weight}% (${s.overall.influence}), ` +
    `institution ${s.institution.weight}% (${s.institution.influence}), ` +
    `news ${s.news.weight}% (${s.news.influence}), ` +
    `social ${s.social.weight}% (${s.social.influence}).`;
}

async function fetchJson(url, options = {}) {
  try {
    const res = await fetch(url, options);
    const text = await res.text();
    let body = {};
    try {
      body = text ? JSON.parse(text) : {};
    } catch {
      body = { ok: false, error: 'Invalid JSON response' };
    }
    return { ok: res.ok, status: res.status, body };
  } catch (error) {
    return { ok: false, status: 0, body: { ok: false, error: error?.message || 'Network error' } };
  }
}

async function loadAdminState() {
  const result = await fetchJson('/admin/state');
  if (!result.ok || !result.body?.ok) {
    applyStateToControls(DEFAULT_STATE);
    return;
  }
  applyStateToControls(result.body.data || DEFAULT_STATE);
}

async function saveAdminState() {
  const toSave = buildStateFromControls();
  const result = await fetchJson('/admin/state', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ state: toSave }),
  });

  if (!result.ok || !result.body?.ok) return;
  applyStateToControls(result.body.data || toSave);
  showToast(saveToast, 'Saved');
}

function debounceSave() {
  if (state.saveTimer) window.clearTimeout(state.saveTimer);
  state.saveTimer = window.setTimeout(() => {
    saveAdminState();
  }, 400);
}

function wireSentimentControls() {
  sentimentScopes.forEach((scope) => {
    const c = getSentimentControls(scope);
    c.weight.addEventListener('input', () => {
      c.weightNum.value = c.weight.value;
      state.adminState = buildStateFromControls();
      renderSentimentSummary();
      debounceSave();
    });

    c.weightNum.addEventListener('input', () => {
      c.weight.value = String(clampWeight(c.weightNum.value));
      state.adminState = buildStateFromControls();
      renderSentimentSummary();
      debounceSave();
    });

    c.influence.addEventListener('change', () => {
      state.adminState = buildStateFromControls();
      renderSentimentSummary();
      debounceSave();
    });
  });

  saveStateBtn.addEventListener('click', saveAdminState);
}

function effectClass(effect) {
  const e = String(effect || '').toLowerCase();
  if (e.includes('increase')) return 'effect-up';
  if (e.includes('decrease')) return 'effect-down';
  return 'effect-flat';
}

function renderPreview(preview) {
  if (!Array.isArray(preview) || preview.length === 0) {
    previewBody.innerHTML = '';
    return;
  }

  previewBody.innerHTML = preview
    .map((row) => {
      const effect = String(row.effect || 'unchanged');
      return `
        <tr>
          <td>${row.parameter}</td>
          <td>${row.current}</td>
          <td>${row.proposed}</td>
          <td class="${effectClass(effect)}">${effect}</td>
        </tr>
      `;
    })
    .join('');
}

async function generateProfile() {
  const payload = {
    preset: tacticPreset.value,
    instruction: tacticInstruction.value || '',
  };

  const result = await fetchJson('/admin/tactic/generate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!result.ok || !result.body?.ok) {
    previewBody.innerHTML = '';
    return;
  }

  state.generatedProfile = result.body.profile || null;
  state.generatedVersion = nowVersion();
  renderPreview(result.body.preview || []);
}

async function activateProfile() {
  if (!state.generatedProfile) {
    await generateProfile();
    if (!state.generatedProfile) return;
  }

  const result = await fetchJson('/admin/tactic/activate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      dataset_version: state.generatedVersion || nowVersion(),
      profile: state.generatedProfile,
    }),
  });

  if (!result.ok || !result.body?.ok) return;

  const activeVersion = String(result.body.data?.dataset_version || 'none');
  state.adminState.active_tactic_version = activeVersion;
  await saveAdminState();
  await loadActiveProfile();
  showToast(activateToast, 'Activated');
}

async function loadActiveProfile() {
  const result = await fetchJson('/admin/tactic/active');
  if (!result.ok || !result.body?.ok || !result.body.data) {
    activeProfileJson.textContent = JSON.stringify({ active_tactic_version: 'none' }, null, 2);
    return;
  }

  const data = result.body.data;
  activeProfileJson.textContent = JSON.stringify(
    {
      dataset_version: data.dataset_version,
      profile: data.profile,
      created_at: data.created_at,
    },
    null,
    2
  );
}

function wireActions() {
  generateBtn.addEventListener('click', generateProfile);
  activateBtn.addEventListener('click', activateProfile);
}

window.addEventListener('DOMContentLoaded', async () => {
  wireSentimentControls();
  wireActions();
  await loadAdminState();
  await generateProfile();
  await loadActiveProfile();
});

/* ═══════════════════════════════════════════════════════════════════════════
   ArbScanner — Frontend Application Logic
   ═══════════════════════════════════════════════════════════════════════════ */

const API_BASE = "/api";

// ─── State ────────────────────────────────────────────────────────────────────
const state = {
  opportunities: [],
  filteredOpps: [],
  config: {
    odds_api_key: "",
    oddspapi_key: "",
    refresh_interval: 60,
    min_arb_threshold: 0,
    notify_above_pct: 2.0,
    sound_alerts: false,
    default_bankroll: 100,
  },
  meta: null,
  isLoading: false,
  expandedRow: null,
  countdownSeconds: 60,
  countdownInterval: null,
  scanInterval: null,
  autoScanEnabled: false,
  sortColumn: "net",
  sortDirection: "desc",
  previousIds: new Set(),
  sessionBest: 0,
  sessionCount: 0,
  _scanCycle: 0,           // tracks quick/full scan alternation
};

// Quick scan runs every QUICK_SCAN_INTERVAL seconds, fetching only
// prediction market prices against cached sportsbook data.
// Every FULL_SCAN_EVERY-th cycle triggers a full scan that refreshes
// sportsbook odds too.  This gives ~10s arb detection latency
// while only hitting the Odds API every ~40s.
const QUICK_SCAN_INTERVAL = 10;
const FULL_SCAN_EVERY = 4;  // full scan every 4th cycle (~40s)

// ─── Utility Functions ────────────────────────────────────────────────────────

function formatOdds(american) {
  if (!american || american === 0) return "--";
  return american > 0 ? `+${american}` : `${american}`;
}

function formatProb(prob) {
  if (prob === null || prob === undefined) return "--";
  return (prob * 100).toFixed(1) + "%";
}

function formatPct(pct) {
  if (pct === null || pct === undefined) return "--";
  return pct.toFixed(2) + "%";
}

function formatMoney(amount) {
  if (amount === null || amount === undefined) return "--";
  return "$" + parseFloat(amount).toFixed(2);
}

function formatLiquidity(val) {
  if (!val) return "--";
  val = parseFloat(val);
  if (val >= 1000000) return "$" + (val / 1000000).toFixed(1) + "M";
  if (val >= 1000) return "$" + (val / 1000).toFixed(0) + "K";
  return "$" + val.toFixed(0);
}

function formatOddsAge(isoStr) {
  if (!isoStr) return null;
  try {
    const age = (Date.now() - new Date(isoStr).getTime()) / 1000;
    if (age < 0) return null;
    if (age < 60) return { text: `${Math.round(age)}s ago`, stale: false };
    if (age < 3600) return { text: `${Math.round(age / 60)}m ago`, stale: age > 300 };
    return { text: `${Math.round(age / 3600)}h ago`, stale: true };
  } catch { return null; }
}

function platformClass(name) {
  if (!name) return "";
  const n = name.toLowerCase();
  if (n.includes("polymarket")) return "polymarket";
  if (n.includes("kalshi")) return "kalshi";
  if (n.includes("draftkings") || n.includes("draft")) return "draftkings";
  if (n.includes("fanduel")) return "fanduel";
  if (n.includes("fanatics")) return "fanatics";
  if (n.includes("betrivers") || n.includes("rivers")) return "betrivers";
  if (n.includes("pinnacle")) return "pinnacle";
  if (n.includes("betmgm") || n.includes("mgm")) return "betmgm";
  if (n.includes("espnbet") || n.includes("espn")) return "espnbet";
  if (n.includes("hardrock") || n.includes("hard rock")) return "hardrock";
  if (n.includes("lowvig")) return "lowvig";
  if (n.includes("novig")) return "novig";
  if (n.includes("betonline")) return "betonline";
  if (n.includes("mybookie")) return "mybookie";
  if (n.includes("betus")) return "betus";
  if (n.includes("ballybet") || n.includes("bally")) return "ballybet";
  if (n.includes("betparx") || n.includes("parx")) return "betparx";
  if (n.includes("bovada")) return "bovada";
  if (n.includes("william hill") || n.includes("caesars")) return "caesars";
  return "";
}

function sportClass(sport) {
  if (!sport) return "sport-default";
  const s = sport.toUpperCase();
  if (s.includes("NBA")) return "sport-nba";
  if (s.includes("NFL")) return "sport-nfl";
  if (s.includes("MLB")) return "sport-mlb";
  if (s.includes("NHL")) return "sport-nhl";
  if (s.includes("SOCCER") || s.includes("MLS") || s.includes("EPL")) return "sport-soccer";
  if (s.includes("MMA") || s.includes("UFC")) return "sport-mma";
  return "sport-default";
}

function profitClass(pct) {
  if (pct >= 3) return "positive";
  if (pct >= 1) return "marginal";
  return "negative";
}

function tierClass(pct) {
  if (pct >= 3) return "tier-strong";
  if (pct >= 1) return "tier-marginal";
  return "tier-weak";
}

function riskIcon(risk) {
  if (risk === "low") return '<span class="risk-icon risk-low" title="Low risk">●</span>';
  if (risk === "medium") return '<span class="risk-icon risk-medium" title="Medium risk — check resolution criteria">▲</span>';
  return '<span class="risk-icon risk-high" title="High risk — markets may not match">⚠</span>';
}

function truncate(str, max) {
  if (!str) return "";
  return str.length > max ? str.substring(0, max) + "…" : str;
}

function escapeHtml(s) {
  if (!s) return "";
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

// ─── API Calls ────────────────────────────────────────────────────────────────

async function fetchScan(mode = "full") {
  const params = new URLSearchParams();
  // API key stays server-side only (env var or DB) — never sent from browser
  const minPct = parseFloat(document.getElementById("minProfitSlider").value) || 0;
  if (minPct > 0) params.set("min_pct", minPct.toString());
  if (mode === "quick") params.set("mode", "quick");

  const url = `${API_BASE}/scan?${params.toString()}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const data = await resp.json();
  if (!data.opportunities) throw new Error("Invalid response format");
  return data;
}

async function fetchDetail(opp, bankroll) {
  const params = new URLSearchParams({
    platform_a: opp.platform_a.name.toLowerCase(),
    platform_b: opp.platform_b.name.toLowerCase(),
    market_id_a: opp.platform_a.market_id || "",
    market_id_b: opp.platform_b.market_id || "",
    prob_a: opp.platform_a.implied_prob.toString(),
    prob_b: opp.platform_b.implied_prob.toString(),
    bankroll: bankroll.toString(),
    fee_a: (opp.platform_a.fee_pct / 100).toString(),
    fee_b: (opp.platform_b.fee_pct / 100).toString(),
  });

  const url = `${API_BASE}/detail?${params.toString()}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Detail fetch failed: ${resp.status}`);
  return resp.json();
}

async function loadConfig() {
  // Try localStorage first (persists across page reloads on deployed site)
  try {
    const saved = localStorage.getItem("arbscanner_config");
    if (saved) Object.assign(state.config, JSON.parse(saved));
  } catch (e) { /* corrupt localStorage */ }

  // Then try backend API (may override localStorage)
  try {
    const url = `${API_BASE}/config`;
    const resp = await fetch(url);
    if (resp.ok) {
      const text = await resp.text();
      try {
        const data = JSON.parse(text);
        if (data.config) {
          Object.assign(state.config, data.config);
        }
      } catch (parseErr) {
        // Invalid JSON, use defaults
      }
    }
  } catch (e) {
    // Backend unavailable, localStorage config already loaded
  }
}

async function saveConfig(configData) {
  // Always persist to localStorage (survives Vercel ephemeral storage)
  try {
    localStorage.setItem("arbscanner_config", JSON.stringify(configData));
  } catch (e) { /* quota */ }

  try {
    const url = `${API_BASE}/config`;
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(configData),
    });
  } catch (e) {
    console.error("Failed to save config:", e);
  }
}

// ─── Filter Persistence ───────────────────────────────────────────────────────

const FILTER_STORAGE_KEY = "arbscanner_filters";

function saveFiltersToStorage() {
  try {
    const filters = {};
    document.querySelectorAll(".sidebar input[type='checkbox']").forEach(cb => {
      filters[cb.id] = cb.checked;
    });
    filters._minProfitSlider = document.getElementById("minProfitSlider").value;
    filters._sortSelect = document.getElementById("sortSelect").value;
    localStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify(filters));
  } catch (e) { /* quota */ }
}

function restoreFiltersFromStorage() {
  try {
    const saved = localStorage.getItem(FILTER_STORAGE_KEY);
    if (!saved) return;
    const filters = JSON.parse(saved);
    for (const [id, checked] of Object.entries(filters)) {
      if (id.startsWith("_")) continue;
      const el = document.getElementById(id);
      if (el && el.type === "checkbox") el.checked = checked;
    }
    if (filters._minProfitSlider !== undefined) {
      const slider = document.getElementById("minProfitSlider");
      slider.value = filters._minProfitSlider;
      document.getElementById("minProfitValue").textContent = slider.value + "%";
    }
    if (filters._sortSelect !== undefined) {
      document.getElementById("sortSelect").value = filters._sortSelect;
    }
  } catch (e) { /* corrupt */ }
}

// ─── Filtering ────────────────────────────────────────────────────────────────

function getActiveFilters() {
  const sports = [];
  if (document.getElementById("fNBA").checked) sports.push("NBA");
  if (document.getElementById("fNFL").checked) sports.push("NFL");
  if (document.getElementById("fMLB").checked) sports.push("MLB");
  if (document.getElementById("fNHL").checked) sports.push("NHL");
  if (document.getElementById("fSoccer").checked) sports.push("SOCCER");
  if (document.getElementById("fMMA").checked) sports.push("MMA");

  const platforms = [];
  if (document.getElementById("fPolymarket").checked) platforms.push("polymarket");
  if (document.getElementById("fKalshi").checked) platforms.push("kalshi");
  if (document.getElementById("fDraftKings").checked) platforms.push("draftkings");
  if (document.getElementById("fFanDuel").checked) platforms.push("fanduel");
  if (document.getElementById("fBetRivers").checked) platforms.push("betrivers");
  if (document.getElementById("fPinnacle").checked) platforms.push("pinnacle");
  if (document.getElementById("fBetMGM").checked) platforms.push("betmgm");
  if (document.getElementById("fFanatics").checked) platforms.push("fanatics");
  if (document.getElementById("fESPNBET").checked) platforms.push("espnbet");
  if (document.getElementById("fHardRock").checked) platforms.push("hardrockbet");
  if (document.getElementById("fLowVig").checked) platforms.push("lowvig");
  if (document.getElementById("fNovig").checked) platforms.push("novig");
  if (document.getElementById("fBetOnline").checked) platforms.push("betonline");
  if (document.getElementById("fMyBookie").checked) platforms.push("mybookie");
  if (document.getElementById("fBetUS").checked) platforms.push("betus");
  if (document.getElementById("fBallyBet").checked) platforms.push("ballybet");
  if (document.getElementById("fBetParx").checked) platforms.push("betparx");

  const marketTypes = [];
  if (document.getElementById("fMoneyline").checked) marketTypes.push("h2h");
  if (document.getElementById("fSpreads").checked) marketTypes.push("spreads");
  if (document.getElementById("fTotals").checked) marketTypes.push("totals");
  if (document.getElementById("fProps").checked) marketTypes.push("player_points", "player_rebounds", "player_assists", "player_threes");

  const showArbs = document.getElementById("fShowArbs").checked;
  const showEV = document.getElementById("fShowEV").checked;
  const includeLive = document.getElementById("fIncludeLive").checked;
  const minProfit = parseFloat(document.getElementById("minProfitSlider").value) || 0;

  return { sports, platforms, marketTypes, showArbs, showEV, includeLive, minProfit };
}

function applyFilters() {
  const filters = getActiveFilters();
  let opps = state.opportunities;

  opps = opps.filter(o => {
    // Type filter
    const oppType = o.type || "arb";
    if (oppType === "arb" && !filters.showArbs) return false;
    if (oppType === "ev" && !filters.showEV) return false;

    // Sport filter
    if (filters.sports.length > 0) {
      const s = (o.sport || "").toUpperCase();
      if (!filters.sports.some(f => s.includes(f))) return false;
    }

    // Platform filter
    const pA = (o.platform_a.name || "").toLowerCase();
    const pB = (o.platform_b.name || "").toLowerCase();
    if (filters.platforms.length > 0) {
      const matchA = filters.platforms.some(f => pA.includes(f));
      const matchB = filters.platforms.some(f => pB.includes(f));
      if (!matchA && !matchB) return false;
    }

    // Market type filter
    if (filters.marketTypes.length > 0 && o.market_type) {
      if (!filters.marketTypes.includes(o.market_type) &&
          !(o.is_prop && filters.marketTypes.some(m => m.startsWith("player"))) &&
          !(o.market_type === "binary" && filters.marketTypes.includes("h2h"))) {
        return false;
      }
    }

    // Live filter
    if (!filters.includeLive && o.is_live) return false;

    // Min edge filter — applies to both arb net% and EV%
    const edgePct = oppType === "ev" ? (o.ev_pct || 0) : o.net_arb_pct;
    if (edgePct < filters.minProfit) return false;

    return true;
  });

  // Sort — arbs always rank above +EV, then within each group by selected metric
  const sortBy = document.getElementById("sortSelect").value;
  opps.sort((a, b) => {
    // Arbs first (type "arb" before "ev")
    if (a.type !== b.type) {
      if (a.type === "arb") return -1;
      if (b.type === "arb") return 1;
    }

    let va, vb;
    switch (sortBy) {
      case "edge":
        // Arbs by net%, +EV by Kelly fraction (bankroll-optimal ranking)
        va = a.type === "ev" ? (a.kelly_fraction || 0) : (a.net_arb_pct || 0);
        vb = b.type === "ev" ? (b.kelly_fraction || 0) : (b.net_arb_pct || 0);
        break;
      case "eqs":
        // Edge Quality Score: arbs first (by net%), +EV by composite quality
        va = a.type === "ev" ? (a.edge_quality_score || 0) : (a.net_arb_pct || 0);
        vb = b.type === "ev" ? (b.edge_quality_score || 0) : (b.net_arb_pct || 0);
        break;
      case "ev_raw":
        va = a.type === "ev" ? (a.ev_pct || 0) : (a.net_arb_pct || 0);
        vb = b.type === "ev" ? (b.ev_pct || 0) : (b.net_arb_pct || 0);
        break;
      case "gross_pct": va = a.gross_arb_pct; vb = b.gross_arb_pct; break;
      case "time":
        va = a.commence_time ? new Date(a.commence_time).getTime() : Infinity;
        vb = b.commence_time ? new Date(b.commence_time).getTime() : Infinity;
        return va - vb; // Ascending for time
      case "liquidity": va = a.liquidity || 0; vb = b.liquidity || 0; break;
      case "confidence": va = a.match_confidence || 0; vb = b.match_confidence || 0; break;
      default:
        va = a.type === "ev" ? (a.kelly_fraction || 0) : (a.net_arb_pct || 0);
        vb = b.type === "ev" ? (b.kelly_fraction || 0) : (b.net_arb_pct || 0);
    }
    return vb - va; // Descending default
  });

  // Column sort override
  if (state.sortColumn) {
    opps = sortByColumn(opps, state.sortColumn, state.sortDirection);
  }

  state.filteredOpps = opps;
  renderTable();
  updateStats();
}

function sortByColumn(opps, col, dir) {
  return [...opps].sort((a, b) => {
    let va, vb;
    switch (col) {
      case "sport": va = a.sport; vb = b.sport; break;
      case "event": va = a.event; vb = b.event; break;
      case "time":
        va = a.commence_time ? new Date(a.commence_time).getTime() : Infinity;
        vb = b.commence_time ? new Date(b.commence_time).getTime() : Infinity;
        break;
      case "platform_a": va = a.platform_a.name; vb = b.platform_a.name; break;
      case "platform_b": va = a.platform_b.name; vb = b.platform_b.name; break;
      case "gross": va = a.gross_arb_pct; vb = b.gross_arb_pct; break;
      case "net":
        va = (a.type === "ev") ? (a.kelly_fraction || 0) : (a.net_arb_pct || 0);
        vb = (b.type === "ev") ? (b.kelly_fraction || 0) : (b.net_arb_pct || 0);
        break;
      default: return 0;
    }
    if (typeof va === "string") {
      return dir === "asc" ? va.localeCompare(vb) : vb.localeCompare(va);
    }
    return dir === "asc" ? va - vb : vb - va;
  });
}

// ─── Rendering ────────────────────────────────────────────────────────────────

function renderTable() {
  const tbody = document.getElementById("arbTableBody");
  const emptyState = document.getElementById("emptyState");
  const tableCount = document.getElementById("tableCount");
  const opps = state.filteredOpps;

  tableCount.textContent = `${opps.length} result${opps.length !== 1 ? "s" : ""}`;

  if (opps.length === 0) {
    tbody.innerHTML = "";
    emptyState.style.display = "flex";
    return;
  }

  emptyState.style.display = "none";
  const newIds = new Set(opps.map(o => o.id));

  let html = "";
  for (const opp of opps) {
    const isNew = !state.previousIds.has(opp.id);
    const isEV = opp.type === "ev";
    const edgePct = isEV ? (opp.ev_pct || 0) : opp.net_arb_pct;
    const tier = isEV ? "tier-ev" : tierClass(opp.net_arb_pct);
    const rowClass = `opp-row ${tier} ${isNew ? "new-row-enter" : ""}`;

    const typeBadge = isEV
      ? '<span class="type-badge ev">+EV</span>'
      : '<span class="type-badge arb">ARB</span>';

    const timeCell = opp.is_live
      ? '<span class="live-badge"><span class="live-pulse"></span>LIVE</span>'
      : escapeHtml(opp.time_display || "--");

    const oddsA = opp.platform_a.implied_prob
      ? `${formatProb(opp.platform_a.implied_prob)}<br><span style="color:var(--text-dim);font-size:0.6rem">${formatOdds(opp.platform_a.american_odds)}</span>`
      : formatOdds(opp.platform_a.american_odds);

    const oddsB = opp.platform_b.implied_prob
      ? `${formatProb(opp.platform_b.implied_prob)}<br><span style="color:var(--text-dim);font-size:0.6rem">${formatOdds(opp.platform_b.american_odds)}</span>`
      : formatOdds(opp.platform_b.american_odds);

    let edgeLabel = isEV ? `+${formatPct(edgePct)} EV` : formatPct(edgePct);
    if (isEV && opp.consensus_prob) {
      const winPct = Math.round(opp.consensus_prob * 100);
      const winClass = winPct > 50 ? "high" : winPct >= 25 ? "mid" : "low";
      edgeLabel += `<span class="win-rate ${winClass}">Win ~${winPct}%</span>`;
    }
    const edgeClass = isEV ? "profit-cell" : `profit-cell ${profitClass(edgePct)}`;
    const edgeColor = isEV ? 'style="color:var(--blue);text-shadow:0 0 12px rgba(77,166,255,0.2)"' : "";

    html += `
      <tr class="${rowClass}" data-id="${opp.id}" onclick="toggleDetail('${opp.id}')">
        <td>${typeBadge}</td>
        <td><span class="sport-tag ${sportClass(opp.sport)}">${escapeHtml(opp.sport)}</span></td>
        <td title="${escapeHtml(opp.event_detail || opp.event)}" style="white-space:normal;line-height:1.3">${escapeHtml(truncate(opp.event, 35))}</td>
        <td>${timeCell}</td>
        <td><span class="platform-name ${platformClass(opp.platform_a.name)}">${escapeHtml(opp.platform_a.name)}</span></td>
        <td>${escapeHtml(opp.platform_a.side)}</td>
        <td class="odds-cell">${oddsA}</td>
        <td>${isEV
          ? `<span class="platform-name ${platformClass(opp.platform_b.name)}" style="opacity:0.6">${escapeHtml(opp.platform_b.name)}</span><div class="ref-label">SHARP LINE</div>`
          : `<span class="platform-name ${platformClass(opp.platform_b.name)}">${escapeHtml(opp.platform_b.name)}</span>`}</td>
        <td${isEV ? ' style="opacity:0.5"' : ''}>${escapeHtml(opp.platform_b.side)}</td>
        <td class="odds-cell"${isEV ? ' style="opacity:0.5"' : ''}>${oddsB}</td>
        <td class="${edgeClass}" ${edgeColor}>${edgeLabel}</td>
        <td>
          <div class="tooltip-wrapper">
            ${riskIcon(opp.resolution_risk)}
            <div class="tooltip-text">${escapeHtml(opp.risk_note || "No additional risk notes")}</div>
          </div>
        </td>
        <td>
          <button class="track-btn" onclick="event.stopPropagation(); trackBet('${opp.id}')" title="Track this bet">+</button>
          <button class="copy-btn" onclick="event.stopPropagation(); copyOpp('${opp.id}')" title="Copy details">⧉</button>
        </td>
      </tr>
      <tr class="detail-panel" id="detail-${opp.id}">
        <td colspan="13">
          <div class="detail-content" id="detail-content-${opp.id}">
            ${isEV ? renderEVDetail(opp) : renderArbDetail(opp)}
          </div>
        </td>
      </tr>
    `;
  }

  tbody.innerHTML = html;
  state.previousIds = newIds;
}

function renderStakes(opp, bankroll) {
  bankroll = parseFloat(bankroll) || 100;
  const pa = opp.platform_a.implied_prob;
  const pb = opp.platform_b.implied_prob;
  const is3Way = opp.n_sides === 3 && opp.platform_c;
  const pc = is3Way ? opp.platform_c.implied_prob : 0;

  if (!pa || !pb || pa <= 0 || pb <= 0) {
    return '<div style="color:var(--text-dim);font-size:0.72rem">Insufficient data for stake calculation</div>';
  }

  const stakeA = (bankroll * pa).toFixed(2);
  const stakeB = (bankroll * pb).toFixed(2);
  let totalStaked, profit, roi;

  let html = `
    <div class="stake-line"><span class="label">Stake on ${escapeHtml(opp.platform_a.name)} (${escapeHtml(opp.platform_a.side)}):</span><span class="value">${formatMoney(stakeA)}</span></div>
    <div class="stake-line"><span class="label">Stake on ${escapeHtml(opp.platform_b.name)} (${escapeHtml(opp.platform_b.side)}):</span><span class="value">${formatMoney(stakeB)}</span></div>
  `;

  if (is3Way && pc > 0) {
    const stakeC = (bankroll * pc).toFixed(2);
    totalStaked = (parseFloat(stakeA) + parseFloat(stakeB) + parseFloat(stakeC)).toFixed(2);
    profit = (bankroll - parseFloat(totalStaked)).toFixed(2);
    roi = ((profit / totalStaked) * 100).toFixed(2);
    html += `<div class="stake-line"><span class="label">Stake on ${escapeHtml(opp.platform_c.name)} (${escapeHtml(opp.platform_c.side)}):</span><span class="value">${formatMoney(stakeC)}</span></div>`;
  } else {
    totalStaked = (parseFloat(stakeA) + parseFloat(stakeB)).toFixed(2);
    profit = (bankroll - parseFloat(totalStaked)).toFixed(2);
    roi = ((profit / totalStaked) * 100).toFixed(2);
  }

  html += `
    <div class="stake-line total"><span class="label">Total staked:</span><span class="value">${formatMoney(totalStaked)}</span></div>
    <div class="stake-line"><span class="label">Total return (any outcome):</span><span class="value">${formatMoney(bankroll)}</span></div>
    <div class="stake-line"><span class="label">Your ${formatMoney(totalStaked)} wagered returns ${formatMoney(bankroll)} whichever side wins</span></div>
    <div class="stake-line"><span class="label">Guaranteed profit:</span><span class="value green">${formatMoney(profit)} (${roi}%)</span></div>
  `;
  return html;
}

function computeRiskScore(opp) {
  // Multi-factor risk score (0-100, lower = less risky)
  let score = 30; // base
  const ev = opp.ev_pct || 0;
  if (ev > 15) score += Math.min(20, (ev - 15) * 2);          // large edges = stale risk
  if (ev > 25) score += 15;
  const nBooks = opp.n_books || 1;
  score -= Math.min(15, nBooks * 3);                            // more books = safer
  const spread = opp.consensus_spread || 0;
  score += Math.min(15, spread * 100);                          // high disagreement = risky
  if (opp.match_confidence < 0.8) score += 10;                  // low match confidence
  if (opp.is_live) score += 10;                                 // live = faster movement
  if (nBooks <= 1) score += 10;                                 // single source = uncertain
  return Math.max(0, Math.min(100, Math.round(score)));
}

function riskScoreLabel(score) {
  if (score <= 25) return { text: "Low", color: "var(--green)" };
  if (score <= 50) return { text: "Medium", color: "var(--yellow)" };
  if (score <= 75) return { text: "High", color: "var(--orange, #ff9900)" };
  return { text: "Very High", color: "var(--red, #ff4444)" };
}

function renderEdgeMetrics(opp) {
  const nBooks = opp.n_books || 0;
  const spread = opp.consensus_spread || 0;
  const growthRate = opp.growth_rate || 0;
  const betsToDouble = opp.bets_to_double || 0;
  const eqs = opp.edge_quality_score || 0;
  const sourceBooks = opp.source_books || [];
  const overround = opp.overround || 0;
  const riskScore = computeRiskScore(opp);
  const risk = riskScoreLabel(riskScore);

  // 2A: Book agreement
  const agreementLabel = spread < 0.02 ? "High" : spread < 0.05 ? "Medium" : "Low";
  const agreementColor = spread < 0.02 ? "var(--green)" : spread < 0.05 ? "var(--yellow)" : "var(--red, #ff4444)";

  let html = '<div style="margin-top:10px;font-size:0.65rem;line-height:1.8;color:var(--text-dim)">';

  // 2A: Book Consensus Spread
  if (nBooks > 0) {
    html += `<div><strong>Book agreement:</strong> <span style="color:${agreementColor};font-weight:600">${agreementLabel}</span> (${nBooks} book${nBooks !== 1 ? 's' : ''}, spread: ${(spread * 100).toFixed(1)}%)</div>`;
  }

  // 2B: Fair Value Source Attribution
  if (sourceBooks.length > 0) {
    const bookList = sourceBooks.slice(0, 4).map(b => escapeHtml(b)).join(', ');
    const vigRemoved = overround > 1 ? ((overround - 1) * 100).toFixed(1) + '% vig removed' : '';
    html += `<div><strong>Fair value source:</strong> ${bookList}${vigRemoved ? ' (' + vigRemoved + ')' : ''}</div>`;
  }

  // 2C: Enhanced Risk Classification
  html += `<div><strong>Risk score:</strong> <span style="color:${risk.color};font-weight:600">${riskScore}/100 (${risk.text})</span></div>`;

  // 2D: Expected Growth Rate
  if (growthRate > 0) {
    html += `<div><strong>Growth per bet:</strong> +${(growthRate * 100).toFixed(3)}%`;
    if (betsToDouble > 0 && betsToDouble < 100000) {
      html += ` (~${Math.round(betsToDouble)} similar bets to double bankroll)`;
    }
    html += '</div>';
  }

  // EQS
  if (eqs > 0) {
    html += `<div><strong>Edge Quality Score:</strong> ${(eqs * 10000).toFixed(1)}</div>`;
  }

  html += '</div>';
  return html;
}

function renderArbDetail(opp) {
  const bankroll = state.config.default_bankroll || 100;
  const stakeA = (bankroll * opp.platform_a.implied_prob).toFixed(2);
  const stakeB = (bankroll * opp.platform_b.implied_prob).toFixed(2);
  const is3Way = opp.n_sides === 3 && opp.platform_c;
  const stakeC = is3Way ? (bankroll * opp.platform_c.implied_prob).toFixed(2) : "0";
  const totalStaked = is3Way
    ? (parseFloat(stakeA) + parseFloat(stakeB) + parseFloat(stakeC))
    : (parseFloat(stakeA) + parseFloat(stakeB));
  const profit = bankroll - totalStaked;
  const sidesLabel = is3Way ? "all three outcomes across bookmakers" : "both sides across two platforms";

  let stepsHtml = `
          <div class="action-step">
            <span class="step-num">1</span>
            <div class="step-body">
              <div class="step-title">Bet <strong style="color:var(--green)">${formatMoney(stakeA)}</strong> on ${escapeHtml(opp.platform_a.name)}</div>
              <div class="step-detail">${escapeHtml(opp.platform_a.side)} @ ${formatProb(opp.platform_a.implied_prob)} (${formatOdds(opp.platform_a.american_odds)})</div>
              ${opp.platform_a.url ? `<a class="step-link" href="${opp.platform_a.url}" target="_blank" rel="noopener noreferrer">Open ${escapeHtml(opp.platform_a.name)} &rarr;</a>` : ""}
            </div>
          </div>
          <div class="action-step">
            <span class="step-num">2</span>
            <div class="step-body">
              <div class="step-title">Bet <strong style="color:var(--green)">${formatMoney(stakeB)}</strong> on ${escapeHtml(opp.platform_b.name)}</div>
              <div class="step-detail">${escapeHtml(opp.platform_b.side)} @ ${formatProb(opp.platform_b.implied_prob)} (${formatOdds(opp.platform_b.american_odds)})</div>
              ${opp.platform_b.url ? `<a class="step-link" href="${opp.platform_b.url}" target="_blank" rel="noopener noreferrer">Open ${escapeHtml(opp.platform_b.name)} &rarr;</a>` : ""}
            </div>
          </div>`;

  if (is3Way) {
    stepsHtml += `
          <div class="action-step">
            <span class="step-num">3</span>
            <div class="step-body">
              <div class="step-title">Bet <strong style="color:var(--green)">${formatMoney(stakeC)}</strong> on ${escapeHtml(opp.platform_c.name)}</div>
              <div class="step-detail">${escapeHtml(opp.platform_c.side)} @ ${formatProb(opp.platform_c.implied_prob)} (${formatOdds(opp.platform_c.american_odds)})</div>
            </div>
          </div>`;
  }

  const collectStepNum = is3Way ? 4 : 3;
  stepsHtml += `
          <div class="action-step">
            <span class="step-num">${collectStepNum}</span>
            <div class="step-body">
              <div class="step-title">Collect <strong style="color:var(--green)">${formatMoney(profit)}</strong> guaranteed profit</div>
              <div class="step-detail">Total staked: ${formatMoney(totalStaked)} &rarr; Total return: ${formatMoney(bankroll)} (any outcome)</div>
              <div class="step-detail" style="margin-top:4px;color:var(--text-dim)">Your ${formatMoney(totalStaked)} wagered returns ${formatMoney(bankroll)} whichever side wins</div>
            </div>
          </div>`;

  return `
    <div class="detail-explainer arb-explainer">
      <div class="explainer-label">GUARANTEED ARBITRAGE${is3Way ? " (3-WAY)" : ""}</div>
      <div class="explainer-text">
        Bet ${sidesLabel}. No matter who wins, you profit <strong style="color:var(--green)">${formatPct(opp.net_arb_pct)}</strong> after fees.
      </div>
    </div>
    <div class="detail-grid">
      <div class="detail-section">
        <h4>What To Do</h4>
        <div class="action-steps">
          ${stepsHtml}
        </div>
        <div class="stake-input-row" style="margin-top:14px">
          <label>Bankroll: $</label>
          <input type="number" value="${bankroll}" min="1" onchange="recalcStakes('${opp.id}', this.value)">
        </div>
        <div class="stake-result" id="stakes-${opp.id}">
          ${renderStakes(opp, bankroll)}
        </div>
      </div>
      <div class="detail-section">
        <h4>Fee Breakdown</h4>
        <table class="fee-table">
          <tr><td>${escapeHtml(opp.platform_a.name)} fee</td><td>${opp.platform_a.fee_pct.toFixed(1)}%</td></tr>
          <tr><td>${escapeHtml(opp.platform_b.name)} fee</td><td>${opp.platform_b.fee_pct.toFixed(1)}%</td></tr>
          ${is3Way ? `<tr><td>${escapeHtml(opp.platform_c.name)} fee</td><td>${opp.platform_c.fee_pct.toFixed(1)}%</td></tr>` : ""}
          <tr><td>Gross arb</td><td style="color:var(--green)">${formatPct(opp.gross_arb_pct)}</td></tr>
          <tr><td>Net arb (after fees)</td><td style="color:${opp.net_arb_pct >= 1 ? 'var(--green)' : 'var(--yellow)'}">${formatPct(opp.net_arb_pct)}</td></tr>
        </table>
        <div style="margin-top:10px;font-size:0.65rem;color:var(--text-dim)">
          <strong>Match confidence:</strong> ${(opp.match_confidence * 100).toFixed(0)}%<br>
          <strong>Liquidity:</strong> ${formatLiquidity(opp.liquidity)}<br>
          <strong>Volume:</strong> ${formatLiquidity(opp.volume)}
        </div>
      </div>
      <div class="detail-section">
        <h4>How Arbs Work</h4>
        <div style="font-size:0.72rem;color:var(--text-secondary);line-height:1.8">
          ${is3Way
            ? `<p>Bookmakers disagree on the odds for a 3-way market (home win, draw, away win). By betting <strong>all three outcomes</strong> across different books, you lock in a profit no matter the result.</p>`
            : `<p>Two platforms disagree on the odds. By betting <strong>both sides</strong>, you lock in a profit no matter what happens.</p>`
          }
          <p style="margin-top:8px;color:var(--text-dim)">
            <strong>Event:</strong> ${escapeHtml(opp.event_detail || opp.event)}<br>
            <strong>Type:</strong> ${escapeHtml(opp.market_type)}${is3Way ? " (3-way)" : ""}<br>
            ${opp.commence_time ? `<strong>Start:</strong> ${new Date(opp.commence_time).toLocaleString()}` : ""}
          </p>
        </div>
      </div>
    </div>
  `;
}

function renderEVDetail(opp) {
  const bankroll = state.config.default_bankroll || 100;
  const kelly = computeKelly(opp);
  const useAdaptive = kelly.adaptive > 0;
  const suggestedFraction = useAdaptive ? kelly.adaptive : (kelly.half > 0 ? kelly.half : 0.05);
  const suggestedStake = formatMoney(suggestedFraction * bankroll);
  const suggestedPct = (suggestedFraction * 100).toFixed(1) + "%";
  const kellyLabel = useAdaptive ? "Adaptive Kelly" : "Half Kelly";
  return `
    <div class="detail-explainer ev-explainer">
      <div class="explainer-label">POSITIVE EXPECTED VALUE</div>
      <div class="explainer-text">
        The price on <strong>${escapeHtml(opp.platform_a.name)}</strong> is cheaper than the true fair probability.
        You're paying <strong>${formatProb(opp.platform_a.implied_prob)}</strong> for something worth <strong style="color:var(--blue)">${formatProb(opp.consensus_prob)}</strong>.
        You win this bet approximately <strong>${Math.round((opp.consensus_prob || 0) * 100)}%</strong> of the time.
        This is <strong>not a guaranteed profit</strong> — you can lose any single bet — but repeating +EV bets is profitable long-term.
      </div>
    </div>
    <div class="detail-grid">
      <div class="detail-section">
        <h4>What To Do</h4>
        <div class="action-steps">
          <div class="action-step">
            <span class="step-num">1</span>
            <div class="step-body">
              <div class="step-title">Go to <strong>${escapeHtml(opp.platform_a.name)}</strong></div>
              <div class="step-detail">Find: ${escapeHtml(opp.event_detail || opp.event)}</div>
              ${opp.platform_a.url ? `<a class="step-link" href="${opp.platform_a.url}" target="_blank" rel="noopener noreferrer">Open ${escapeHtml(opp.platform_a.name)} &rarr;</a>` : ""}
            </div>
          </div>
          <div class="action-step">
            <span class="step-num">2</span>
            <div class="step-body">
              <div class="step-title">Buy <strong style="color:var(--blue)">${escapeHtml(opp.platform_a.side)}</strong></div>
              <div class="step-detail">At ${formatProb(opp.platform_a.implied_prob)} or better (${formatOdds(opp.platform_a.american_odds)} American)</div>
            </div>
          </div>
          <div class="action-step">
            <span class="step-num">3</span>
            <div class="step-body">
              <div class="step-title">Stake <strong style="color:var(--blue)">${suggestedStake}</strong> (${suggestedPct} of bankroll)</div>
              <div class="step-detail">${kellyLabel} — balances growth with risk. Adjust bankroll below.</div>
            </div>
          </div>
        </div>
        <div class="ev-warning">
          Not guaranteed — you can lose this bet. The edge means you profit over many bets, not every bet.
        </div>
      </div>
      <div class="detail-section">
        <h4>Why This Has Edge</h4>
        <table class="fee-table">
          <tr><td>You pay (cost)</td><td style="color:var(--text-primary)">${(opp.platform_a.implied_prob * 100).toFixed(2)}%</td></tr>
          <tr><td>True fair value</td><td style="color:var(--blue)">${(opp.consensus_prob * 100).toFixed(2)}%</td></tr>
          <tr><td>Platform fee</td><td>${opp.platform_a.fee_pct.toFixed(1)}%</td></tr>
          <tr><td>Your edge</td><td style="color:var(--blue);font-weight:800">+${formatPct(opp.ev_pct)}</td></tr>
        </table>
        ${renderEdgeMetrics(opp)}
        <div class="ref-explainer">
          <strong>Why is ${escapeHtml(opp.platform_b.name)} shown?</strong>
          ${escapeHtml(opp.platform_b.name)} is <em>not</em> a bet you place — it's the reference line used to estimate the true fair probability.
          The fair value (${formatProb(opp.consensus_prob)}) is derived from ${opp.n_books ? opp.n_books + ' sharp sportsbook lines' : 'sharp sportsbook lines'} with the bookmaker's margin (${opp.overround ? ((opp.overround - 1) * 100).toFixed(1) + '% vig' : 'vig'}) removed.
          You only bet on <strong>${escapeHtml(opp.platform_a.name)}</strong>.
        </div>
        ${(() => {
          const age = formatOddsAge(opp.sb_last_update);
          if (!age) return '';
          return `<div class="odds-freshness ${age.stale ? 'stale' : 'fresh'}">
            <span class="freshness-dot"></span>
            Sportsbook odds updated <strong>${age.text}</strong>${age.stale ? ' — edge may be based on stale line' : ''}
          </div>`;
        })()}
        <div style="margin-top:8px;font-size:0.65rem;color:var(--text-dim);line-height:1.6">
          A +${formatPct(opp.ev_pct)} edge means for every $100 wagered, you expect ~$${(opp.ev_pct).toFixed(2)} profit on average.
        </div>
        <details class="ev-formula-details">
          <summary>How is +${formatPct(opp.ev_pct)} calculated?</summary>
          <div class="ev-formula-body">
            <p>Edge is <strong>return on investment</strong>, not just the probability gap.</p>
            <div class="ev-formula-block">
              <span class="formula-label">Formula</span>
              Edge&nbsp;=&nbsp;(&thinsp;fair&thinsp;÷&thinsp;cost&thinsp;−&thinsp;1&thinsp;)&thinsp;×&thinsp;100
            </div>
            <div class="ev-formula-block">
              <span class="formula-label">Your numbers</span>
              Edge&nbsp;=&nbsp;(&thinsp;${(opp.consensus_prob * 100).toFixed(2)}%&thinsp;÷&thinsp;${(opp.platform_a.implied_prob * 100).toFixed(2)}%&thinsp;−&thinsp;1&thinsp;)&thinsp;×&thinsp;100&nbsp;=&nbsp;<strong style="color:var(--blue)">+${formatPct(opp.ev_pct)}</strong>
            </div>
            <p style="margin-top:8px">Why not just fair&thinsp;−&thinsp;cost? Because buying at <strong>${(opp.platform_a.implied_prob * 100).toFixed(1)}%</strong> means your payout is <strong>${(1 / opp.platform_a.implied_prob).toFixed(2)}×</strong> your stake. A small probability gap gets multiplied by a large payout — so even ${((opp.consensus_prob - opp.platform_a.implied_prob) * 100).toFixed(2)} percentage points of mispricing creates a ${formatPct(opp.ev_pct)} return on investment.</p>
            ${opp.platform_a.fee_pct > 0 ? `<p style="margin-top:4px">The ${opp.platform_a.fee_pct.toFixed(1)}% platform fee is also factored in, slightly reducing the effective payout.</p>` : ''}
            <p style="margin-top:6px"><a href="./learn.html#ev-calc" style="color:var(--blue);text-decoration:none;font-weight:500">Full explanation with examples →</a></p>
          </div>
        </details>
      </div>
      <div class="detail-section">
        <h4>Kelly Sizing</h4>
        <div class="stake-input-row">
          <label>Bankroll: $</label>
          <input type="number" value="${bankroll}" min="1" onchange="recalcKelly('${opp.id}', this.value)">
        </div>
        <div id="kelly-${opp.id}">
          ${renderKellyCards(kelly, bankroll)}
        </div>
        <div style="margin-top:10px;font-size:0.62rem;color:var(--text-dim);line-height:1.6">
          <strong>Adaptive Kelly</strong> = confidence-weighted — scales with edge reliability (books, spread, match).<br>
          <strong>Half Kelly</strong> = fixed 50% of full — 75% of the growth, half the swings.<br>
          <strong>Full Kelly</strong> = max growth, high variance — only for edges you trust completely.
        </div>
      </div>
    </div>
  `;
}

function toggleDetail(id) {
  const panel = document.getElementById(`detail-${id}`);
  if (!panel) return;

  if (state.expandedRow === id) {
    panel.classList.remove("open");
    state.expandedRow = null;
  } else {
    // Close previous
    if (state.expandedRow) {
      const prev = document.getElementById(`detail-${state.expandedRow}`);
      if (prev) prev.classList.remove("open");
    }
    panel.classList.add("open");
    state.expandedRow = id;
  }
}

function recalcStakes(id, bankroll) {
  const opp = state.filteredOpps.find(o => o.id === id);
  if (!opp) return;
  const el = document.getElementById(`stakes-${id}`);
  if (el) {
    el.innerHTML = renderStakes(opp, bankroll);
  }
}

function copyOpp(id) {
  const opp = state.filteredOpps.find(o => o.id === id);
  if (!opp) return;

  const isEV = opp.type === "ev";
  const lines = [
    `⚡ ArbScanner Alert — ${isEV ? "+EV Bet" : "Arbitrage"}`,
    `Event: ${opp.event_detail || opp.event}`,
    `Sport: ${opp.sport}`,
    ``,
    `Platform A: ${opp.platform_a.name} — ${opp.platform_a.side} @ ${formatProb(opp.platform_a.implied_prob)} (${formatOdds(opp.platform_a.american_odds)})`,
    `Platform B: ${opp.platform_b.name} — ${opp.platform_b.side} @ ${formatProb(opp.platform_b.implied_prob)} (${formatOdds(opp.platform_b.american_odds)})`,
    ``,
  ];
  if (isEV) {
    lines.push(`EV: +${formatPct(opp.ev_pct)} | Fair prob: ${formatProb(opp.consensus_prob)}`);
    const kelly = computeKelly(opp);
    if (kelly.full > 0) lines.push(`Kelly: ${(kelly.half * 100).toFixed(1)}% (half)`);
  } else {
    lines.push(`Gross: ${formatPct(opp.gross_arb_pct)} | Net: ${formatPct(opp.net_arb_pct)}`);
  }
  lines.push(`Match confidence: ${(opp.match_confidence * 100).toFixed(0)}%`);
  lines.push(`Risk: ${opp.resolution_risk}`);
  const text = lines.join("\n");

  navigator.clipboard.writeText(text).then(() => {
    const btn = document.querySelector(`tr[data-id="${id}"] .copy-btn`);
    if (btn) {
      btn.classList.add("copied");
      btn.textContent = "✓";
      setTimeout(() => { btn.classList.remove("copied"); btn.textContent = "⧉"; }, 1500);
    }
    showToast("Copied to clipboard");
  }).catch(() => {
    showToast("Failed to copy");
  });
}

// ─── Stats ────────────────────────────────────────────────────────────────────

function updateStats() {
  const opps = state.filteredOpps;
  const arbOpps = opps.filter(o => (o.type || "arb") === "arb");
  const evOpps = opps.filter(o => o.type === "ev");

  document.getElementById("statOpps").textContent = opps.length;
  const subParts = [];
  if (arbOpps.length > 0) subParts.push(`${arbOpps.length} arb`);
  if (evOpps.length > 0) subParts.push(`${evOpps.length} +EV`);
  document.getElementById("statOppsSub").textContent = subParts.length > 0 ? subParts.join(", ") : "no opportunities found";

  if (opps.length > 0) {
    // Best edge (arb net% or EV%)
    const bestEdge = opps.reduce((best, o) => {
      const edge = o.type === "ev" ? (o.ev_pct || 0) : o.net_arb_pct;
      return edge > best.edge ? { edge, opp: o } : best;
    }, { edge: -Infinity, opp: null });

    const bestLabel = bestEdge.opp.type === "ev" ? `+${formatPct(bestEdge.edge)} EV` : formatPct(bestEdge.edge);
    document.getElementById("statBestArb").textContent = bestLabel;
    document.getElementById("statBestArbSub").textContent = truncate(bestEdge.opp.event, 25);
    document.getElementById("statBestArb").className = `stat-value ${bestEdge.opp.type === "ev" ? "blue" : bestEdge.edge >= 3 ? "green" : bestEdge.edge >= 1 ? "yellow" : ""}`;

    const avgEdge = opps.reduce((s, o) => s + (o.type === "ev" ? (o.ev_pct || 0) : o.net_arb_pct), 0) / opps.length;
    document.getElementById("statAvgProfit").textContent = formatPct(avgEdge);
    document.getElementById("statAvgProfit").className = `stat-value ${avgEdge >= 3 ? "green" : avgEdge >= 1 ? "yellow" : ""}`;

    // Session stats
    state.sessionCount = Math.max(state.sessionCount, opps.length);
    state.sessionBest = Math.max(state.sessionBest, bestEdge.edge);
  } else {
    document.getElementById("statBestArb").textContent = "--%";
    document.getElementById("statBestArbSub").textContent = "--";
    document.getElementById("statAvgProfit").textContent = "--%";
  }

  if (state.meta) {
    document.getElementById("statScanTime").textContent = state.meta.scan_time + "s";
    document.getElementById("statLastScan").textContent = new Date(state.meta.timestamp).toLocaleTimeString();

    const total = (state.meta.poly_count || 0) + (state.meta.kalshi_count || 0) + (state.meta.sportsbook_count || 0);
    document.getElementById("statEvents").textContent = total || "--";
    document.getElementById("statEventsSub").textContent = `${state.meta.poly_count || 0} poly, ${state.meta.kalshi_count || 0} kalshi, ${state.meta.sportsbook_count || 0} sb`;
  }

  // Footer stats
  document.getElementById("footerToday").textContent = `${state.sessionCount} opps`;
  document.getElementById("footerBest").textContent = formatPct(state.sessionBest);
  if (opps.length > 0) {
    const avg = opps.reduce((s, o) => s + (o.type === "ev" ? (o.ev_pct || 0) : o.net_arb_pct), 0) / opps.length;
    document.getElementById("footerAvg").textContent = formatPct(avg);
  }

  // API quota display
  const remaining = state.meta?.odds_api_remaining;
  const used = state.meta?.odds_api_used;
  const quotaEl = document.getElementById("footerApiQuota");
  if (quotaEl && remaining != null) {
    quotaEl.textContent = `${remaining.toLocaleString()} left`;
    quotaEl.style.color = remaining < 100 ? "var(--red)" : remaining < 500 ? "var(--yellow)" : "";
  }
}

// Track consecutive sportsbook failures — only alarm user for persistent issues
let _sbFailCount = 0;
const _SB_FAIL_THRESHOLD = 3;  // show banner after 3 consecutive failures

function updateSourceStatus(sources, errors) {
  if (!sources) return;

  const sbStatus = sources.sportsbook;
  const sbOk = sbStatus === "ok" || sbStatus === "ok_no_arbs" || sbStatus === "empty";

  // Track consecutive failures — transient errors shouldn't alarm the user
  if (sbOk) {
    _sbFailCount = 0;
  } else if (sbStatus && sbStatus !== "no_key") {
    _sbFailCount++;
  }

  // Status dots: show green when OK or recovering (< threshold), red only for persistent issues
  const statusMap = {
    ok: "ok", empty: "stale", pending: "pending", ok_no_arbs: "ok", no_key: "no_key",
    // Transient errors show as OK (system is recovering silently)
    error: _sbFailCount >= _SB_FAIL_THRESHOLD ? "error" : "ok",
    quota_exceeded: _sbFailCount >= _SB_FAIL_THRESHOLD ? "error" : "ok",
    invalid_key: _sbFailCount >= _SB_FAIL_THRESHOLD ? "error" : "ok",
  };

  for (const [key, status] of Object.entries(sources)) {
    const dotId = `status${key.charAt(0).toUpperCase() + key.slice(1)}`;
    const dot = document.getElementById(dotId);
    if (!dot) continue;
    dot.className = "status-dot";
    dot.classList.add(statusMap[status] || "error");
  }

  // Banner: only show for persistent issues or missing key (always actionable)
  const banner = document.getElementById("demoBanner");

  if (sbStatus === "no_key") {
    // Always show — user needs to add key
    banner.textContent = "";
    banner.className = "demo-banner warning";
    banner.appendChild(document.createTextNode("\u26A0 "));
    const strong = document.createElement("strong");
    strong.textContent = "No Odds API key";
    banner.appendChild(strong);
    banner.appendChild(document.createTextNode(" \u2014 add your key in "));
    const link = document.createElement("a");
    link.href = "#";
    link.style.cssText = "color:inherit;text-decoration:underline";
    link.textContent = "Settings";
    link.addEventListener("click", (e) => { e.preventDefault(); openSettings(); });
    banner.appendChild(link);
    banner.appendChild(document.createTextNode(" to enable sportsbook data."));
  } else if (sbStatus === "quota_exceeded" && _sbFailCount >= _SB_FAIL_THRESHOLD) {
    // Persistent quota issue — user might need to upgrade
    banner.textContent = "";
    banner.className = "demo-banner warning";
    banner.appendChild(document.createTextNode("\u26A0 "));
    const strong = document.createElement("strong");
    strong.textContent = "Odds API quota exceeded";
    banner.appendChild(strong);
    banner.appendChild(document.createTextNode(" \u2014 usage limit reached. "));
    const link = document.createElement("a");
    link.href = "https://the-odds-api.com";
    link.target = "_blank";
    link.style.cssText = "color:inherit;text-decoration:underline";
    link.textContent = "Check your plan";
    banner.appendChild(link);
    banner.appendChild(document.createTextNode(" or wait for it to reset."));
  } else {
    // Everything else (ok, transient errors recovering, etc.) — hide banner
    banner.className = "demo-banner hidden";
  }
}

// ─── Scan Execution ───────────────────────────────────────────────────────────

async function runScan(forceMode) {
  if (state.isLoading) return;
  state.isLoading = true;

  // Determine scan mode: manual clicks always do full, auto-scans alternate
  state._scanCycle++;
  const mode = forceMode || (state._scanCycle % FULL_SCAN_EVERY === 0 ? "full" : "quick");
  const isQuick = mode === "quick";

  const overlay = document.getElementById("loadingOverlay");
  const btn = document.getElementById("btnRefresh");

  if (!isQuick) {
    overlay.classList.add("active");
    btn.disabled = true;
    btn.textContent = "⟳ SCANNING...";
  } else {
    // Quick scan: subtle indicator — pulse the scan button
    btn.classList.add("quick-pulse");
  }

  try {
    const data = await fetchScan(mode);

    // If scan returned empty due to API error but we have existing opps, keep them
    const newOpps = data.opportunities || [];
    const sbStatus = data.meta?.sources?.sportsbook;
    const sbFailed = sbStatus === "quota_exceeded" || sbStatus === "error" || sbStatus === "invalid_key";
    if (newOpps.length === 0 && state.opportunities.length > 0 && sbFailed) {
      // Keep existing opportunities silently
      updateSourceStatus(data.meta?.sources, data.meta?.errors);
      return;
    }

    state.opportunities = newOpps;
    state.meta = data.meta || {};

    // Update source status (don't flash sportsbook dot for quick scans)
    updateSourceStatus(data.meta?.sources, data.meta?.errors);

    // Check for new high-value opportunities
    const newHighValue = state.opportunities.filter(o =>
      !state.previousIds.has(o.id) && o.net_arb_pct >= (state.config.notify_above_pct || 2)
    );

    if (newHighValue.length > 0 && state.previousIds.size > 0) {
      for (const opp of newHighValue) {
        showToast(`New opportunity: ${opp.event} — ${formatPct(opp.net_arb_pct)} net profit`);
        if (state.config.sound_alerts) playChime();
      }
      // Browser notification
      if (Notification.permission === "granted") {
        const best = newHighValue[0];
        new Notification("ArbScanner Alert", {
          body: `${best.event}: ${formatPct(best.net_arb_pct)} net arb (${best.platform_a.name} vs ${best.platform_b.name})`,
          icon: "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⚡</text></svg>",
        });
      }
    }

    applyFilters();

    // Cache for instant display on next visit (only on full scans to avoid thrashing)
    if (!isQuick) {
      try { localStorage.setItem("arbscanner_last_scan", JSON.stringify(data)); } catch (e) { /* quota */ }
      logScanToHistory(data);
    }

  } catch (err) {
    console.error("Scan error:", err);
    if (!isQuick) {
      showToast("Scan failed: " + err.message);
      document.getElementById("statusPolymarket").className = "status-dot error";
      document.getElementById("statusKalshi").className = "status-dot error";
      document.getElementById("statusSportsbook").className = "status-dot error";
    }
  } finally {
    state.isLoading = false;
    overlay.classList.remove("active");
    btn.classList.remove("quick-pulse");
    btn.disabled = false;
    btn.textContent = "↻ SCAN";
    resetCountdown();
  }
}

// ─── Countdown & Auto-Refresh ─────────────────────────────────────────────────

function resetCountdown() {
  // Quick scans run at QUICK_SCAN_INTERVAL; fall back to full interval if auto-scan is off
  const fullInterval = getRefreshInterval();
  state.countdownSeconds = fullInterval ? QUICK_SCAN_INTERVAL : 0;
  updateCountdownDisplay();
}

function updateCountdownDisplay() {
  const el = document.getElementById("countdown");
  const s = state.countdownSeconds;
  if (s >= 60) {
    el.textContent = `${Math.floor(s / 60)}m ${s % 60}s`;
  } else {
    el.textContent = `${s}s`;
  }
}

function getScanMode() {
  // Returns "prime" | "extended" | "off" based on US Eastern time
  const now = new Date();
  const etHour = parseInt(
    now.toLocaleString("en-US", { timeZone: "America/New_York", hour: "numeric", hour12: false })
  );
  if (etHour >= 19) return "prime";     // 7 PM – 11:59 PM ET: 30s refresh
  if (etHour >= 12) return "extended";   // 12 PM – 6:59 PM ET: configured interval
  return "off";                          // 12 AM – 11:59 AM ET: manual only
}

function getRefreshInterval() {
  const mode = getScanMode();
  if (mode === "prime") return 30;                          // 30s during prime time
  if (mode === "extended") return state.config.refresh_interval || 45;  // user setting
  return 0;                                                 // no auto-refresh
}

function startAutoRefresh() {
  if (state.countdownInterval) clearInterval(state.countdownInterval);
  if (state.scanInterval) clearInterval(state.scanInterval);

  state.countdownInterval = setInterval(() => {
    if (!state.autoScanEnabled) {
      document.getElementById("countdown").textContent = "paused";
      return;
    }
    const mode = getScanMode();
    if (mode === "off") {
      document.getElementById("countdown").textContent = "off-hours";
      return;
    }

    state.countdownSeconds--;
    if (state.countdownSeconds <= 0) {
      runScan();  // mode (quick vs full) is determined inside runScan
    }
    updateCountdownDisplay();
  }, 1000);
}

// ─── Toast Notifications ──────────────────────────────────────────────────────

function showToast(message) {
  const container = document.getElementById("toastContainer");
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.textContent = message;
  container.appendChild(toast);

  setTimeout(() => {
    toast.classList.add("fade-out");
    setTimeout(() => toast.remove(), 300);
  }, 4000);
}

// ─── Kelly Criterion ──────────────────────────────────────────────────────

function computeKelly(opp) {
  // f* = (b*p - q) / b
  // b = net payout ratio (what you win per $1 bet, after fees)
  // p = fair probability of winning
  // q = 1 - p
  const price = opp.platform_a.implied_prob;
  const fairProb = opp.consensus_prob || 0;
  const feeRate = (opp.platform_a.fee_pct || 0) / 100;

  if (!price || price <= 0 || price >= 1 || fairProb <= 0) {
    return { full: 0, half: 0, quarter: 0, adaptive: 0, confidence: 0 };
  }

  const grossPayout = 1.0 / price;
  const b = grossPayout - 1.0 - (grossPayout - 1.0) * feeRate; // net profit per $1
  const p = fairProb;
  const q = 1.0 - p;

  if (b <= 0) return { full: 0, half: 0, quarter: 0, adaptive: 0, confidence: 0 };

  const fullKelly = Math.max(0, (b * p - q) / b);

  // Use backend adaptive Kelly if available, else fall back to half Kelly
  const adaptive = opp.kelly_adaptive || fullKelly / 2;
  const confidence = opp.kelly_confidence || 0.5;

  return {
    full: fullKelly,
    half: fullKelly / 2,
    quarter: fullKelly / 4,
    adaptive: adaptive,
    confidence: confidence,
  };
}

function renderKellyCards(kelly, bankroll) {
  bankroll = parseFloat(bankroll) || 100;
  if (kelly.full <= 0) {
    return '<div style="color:var(--text-dim);font-size:0.72rem">Edge too small for Kelly sizing</div>';
  }
  const confPct = (kelly.confidence * 100).toFixed(0);
  const confColor = kelly.confidence >= 0.7 ? "var(--green)" : kelly.confidence >= 0.4 ? "var(--yellow)" : "var(--red, #ff4444)";
  return `
    <div class="kelly-grid">
      <div class="kelly-card" style="border-left:3px solid var(--blue)">
        <div class="kelly-label">Adaptive Kelly</div>
        <div class="kelly-value" style="color:var(--blue)">${(kelly.adaptive * 100).toFixed(1)}%</div>
        <div class="kelly-amount">${formatMoney(kelly.adaptive * bankroll)}</div>
        <div style="font-size:0.55rem;color:var(--text-dim);margin-top:2px">Confidence: <span style="color:${confColor}">${confPct}%</span></div>
      </div>
      <div class="kelly-card">
        <div class="kelly-label">Half Kelly</div>
        <div class="kelly-value">${(kelly.half * 100).toFixed(1)}%</div>
        <div class="kelly-amount">${formatMoney(kelly.half * bankroll)}</div>
      </div>
      <div class="kelly-card">
        <div class="kelly-label">Full Kelly</div>
        <div class="kelly-value">${(kelly.full * 100).toFixed(1)}%</div>
        <div class="kelly-amount">${formatMoney(kelly.full * bankroll)}</div>
      </div>
    </div>
  `;
}

function recalcKelly(id, bankroll) {
  const opp = state.filteredOpps.find(o => o.id === id);
  if (!opp) return;
  const kelly = computeKelly(opp);
  const el = document.getElementById(`kelly-${id}`);
  if (el) {
    // Values are from trusted internal computations, not user-controlled strings
    el.innerHTML = renderKellyCards(kelly, bankroll);
  }
}

// ─── Mobile Sidebar Toggle ────────────────────────────────────────────────

function openSidebar() {
  document.getElementById("sidebar").classList.add("open");
  document.getElementById("sidebarBackdrop").classList.add("open");
}

function closeSidebar() {
  document.getElementById("sidebar").classList.remove("open");
  document.getElementById("sidebarBackdrop").classList.remove("open");
}

// ─── CSV Export ───────────────────────────────────────────────────────────

function exportCSV() {
  const opps = state.filteredOpps;
  if (!opps.length) {
    showToast("No data to export");
    return;
  }
  const headers = [
    "Type", "Sport", "Event", "Time", "Platform A", "Side A", "Odds A", "Prob A",
    "Platform B", "Side B", "Odds B", "Prob B", "Gross %", "Net %", "EV %",
    "Risk", "Confidence", "Liquidity"
  ];
  const rows = opps.map(o => [
    o.type || "arb",
    o.sport,
    `"${(o.event || "").replace(/"/g, '""')}"`,
    o.time_display || "",
    o.platform_a.name,
    `"${(o.platform_a.side || "").replace(/"/g, '""')}"`,
    formatOdds(o.platform_a.american_odds),
    o.platform_a.implied_prob ? (o.platform_a.implied_prob * 100).toFixed(1) + "%" : "",
    o.platform_b.name,
    `"${(o.platform_b.side || "").replace(/"/g, '""')}"`,
    formatOdds(o.platform_b.american_odds),
    o.platform_b.implied_prob ? (o.platform_b.implied_prob * 100).toFixed(1) + "%" : "",
    o.gross_arb_pct != null ? o.gross_arb_pct.toFixed(2) : "",
    o.net_arb_pct != null ? o.net_arb_pct.toFixed(2) : "",
    o.ev_pct != null ? o.ev_pct.toFixed(2) : "",
    o.resolution_risk || "",
    o.match_confidence != null ? (o.match_confidence * 100).toFixed(0) + "%" : "",
    o.liquidity || ""
  ].join(","));
  const csv = [headers.join(","), ...rows].join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `arbscanner_${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
  showToast(`Exported ${opps.length} opportunities`);
}

// ─── Sound Alert ──────────────────────────────────────────────────────────────

function playChime() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.frequency.value = 880;
    osc.type = "sine";
    gain.gain.setValueAtTime(0.1, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.5);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + 0.5);
  } catch (e) {
    // Ignore audio errors
  }
}

// ─── Settings Modal ───────────────────────────────────────────────────────────

function openSettings() {
  document.getElementById("settingsModal").classList.add("open");
  // Populate fields — API keys show placeholder if set server-side, blank if not
  const oddsKeyInput = document.getElementById("inputOddsApiKey");
  const papiKeyInput = document.getElementById("inputOddsPapiKey");
  oddsKeyInput.value = "";
  papiKeyInput.value = "";
  oddsKeyInput.placeholder = state.config.has_odds_api_key ? (state.config.odds_api_key_masked || "Key saved on server") : "Enter your API key...";
  papiKeyInput.placeholder = state.config.has_oddspapi_key ? (state.config.oddspapi_key_masked || "Key saved on server") : "Optional unified API key...";
  document.getElementById("inputDefaultBankroll").value = state.config.default_bankroll || 100;
  document.getElementById("inputNotifyThreshold").value = state.config.notify_above_pct || 2;
  document.getElementById("inputSoundAlerts").value = state.config.sound_alerts ? "true" : "false";
  document.getElementById("inputDevigMethod").value = state.config.devig_method || "power";
  // Webhook fields
  document.getElementById("inputDiscordWebhook").value = state.config.discord_webhook || "";
  document.getElementById("inputTelegramToken").value = state.config.telegram_bot_token || "";
  document.getElementById("inputTelegramChat").value = state.config.telegram_chat_id || "";
  document.getElementById("inputAlertMinEdge").value = state.config.alert_min_edge ?? 2;
}

function closeSettings() {
  document.getElementById("settingsModal").classList.remove("open");
}

async function handleSaveSettings() {
  const newConfig = {
    default_bankroll: parseFloat(document.getElementById("inputDefaultBankroll").value) || 100,
    notify_above_pct: parseFloat(document.getElementById("inputNotifyThreshold").value) || 2,
    sound_alerts: document.getElementById("inputSoundAlerts").value === "true",
    devig_method: document.getElementById("inputDevigMethod").value,
    discord_webhook: document.getElementById("inputDiscordWebhook").value.trim(),
    telegram_bot_token: document.getElementById("inputTelegramToken").value.trim(),
    telegram_chat_id: document.getElementById("inputTelegramChat").value.trim(),
    alert_min_edge: parseFloat(document.getElementById("inputAlertMinEdge").value) || 2,
  };
  // Only send API keys if user typed a new value (not left blank)
  const oddsKey = document.getElementById("inputOddsApiKey").value.trim();
  const papiKey = document.getElementById("inputOddsPapiKey").value.trim();
  if (oddsKey) newConfig.odds_api_key = oddsKey;
  if (papiKey) newConfig.oddspapi_key = papiKey;

  Object.assign(state.config, newConfig);
  await saveConfig(newConfig);
  closeSettings();
  showToast("Configuration saved");

  // Re-scan with new config
  runScan("full");
}

// ─── Event Listeners ──────────────────────────────────────────────────────────

function setupEventListeners() {
  // Refresh button
  document.getElementById("btnRefresh").addEventListener("click", () => runScan("full"));

  // Mobile sidebar
  document.getElementById("btnBurger").addEventListener("click", openSidebar);
  document.getElementById("sidebarBackdrop").addEventListener("click", closeSidebar);

  // CSV export
  document.getElementById("btnExportCSV").addEventListener("click", exportCSV);

  // Auto-scan toggle (disabled — Pro feature)
  const autoToggle = document.getElementById("toggleAutoScan");
  if (autoToggle) {
    autoToggle.closest("label").addEventListener("click", (e) => {
      e.preventDefault();
      showToast("Auto-scan is available for Pro users only");
    });
  }

  // Settings
  document.getElementById("btnSettings").addEventListener("click", openSettings);
  document.getElementById("modalClose").addEventListener("click", closeSettings);
  document.getElementById("modalCancel").addEventListener("click", closeSettings);
  document.getElementById("modalSave").addEventListener("click", handleSaveSettings);
  document.getElementById("settingsModal").addEventListener("click", (e) => {
    if (e.target === document.getElementById("settingsModal")) closeSettings();
  });

  // Filters
  const filterInputs = document.querySelectorAll(".sidebar input, .sidebar select");
  filterInputs.forEach(input => {
    input.addEventListener("change", () => {
      applyFilters();
      saveFiltersToStorage();
    });
  });

  // Min profit slider
  const slider = document.getElementById("minProfitSlider");
  slider.addEventListener("input", () => {
    document.getElementById("minProfitValue").textContent = slider.value + "%";
  });
  slider.addEventListener("change", () => {
    applyFilters();
    saveFiltersToStorage();
  });

  // Table header sorting
  document.querySelectorAll("th[data-sort]").forEach(th => {
    th.addEventListener("click", () => {
      const col = th.dataset.sort;
      if (state.sortColumn === col) {
        state.sortDirection = state.sortDirection === "desc" ? "asc" : "desc";
      } else {
        state.sortColumn = col;
        state.sortDirection = "desc";
      }
      // Update sort arrows
      document.querySelectorAll("th").forEach(h => h.classList.remove("sorted"));
      th.classList.add("sorted");
      th.querySelector(".sort-arrow").textContent = state.sortDirection === "desc" ? "▼" : "▲";
      applyFilters();
    });
  });

  // Request notification permission
  if ("Notification" in window && Notification.permission === "default") {
    // Wait for user interaction before requesting
    document.addEventListener("click", function requestNotif() {
      Notification.requestPermission();
      document.removeEventListener("click", requestNotif);
    }, { once: true });
  }

  // Keyboard shortcuts
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      closeSettings();
      closeSidebar();
      closeBetModal();
      document.getElementById("onboardingModal").classList.remove("open");
      if (state.expandedRow) {
        toggleDetail(state.expandedRow);
      }
    }
    if (e.key === "r" && !e.ctrlKey && !e.metaKey && document.activeElement.tagName !== "INPUT") {
      runScan("full");
    }
  });
}

// ─── Tab Navigation ───────────────────────────────────────────────────────────

function setupTabs() {
  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.tab;
      document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
      document.querySelectorAll(".tab-panel").forEach(p => p.classList.remove("active"));
      btn.classList.add("active");
      const panel = document.getElementById(`tab${target.charAt(0).toUpperCase() + target.slice(1)}`);
      if (panel) panel.classList.add("active");

      if (target === "journal") loadBets();
      if (target === "analytics") loadAnalytics();
    });
  });
}

// ─── Onboarding ───────────────────────────────────────────────────────────────

function checkOnboarding() {
  const seen = localStorage.getItem("arbscanner_onboarded");
  if (seen) return;
  if (!state.config.has_odds_api_key) {
    document.getElementById("onboardingModal").classList.add("open");
  } else {
    localStorage.setItem("arbscanner_onboarded", "1");
  }
}

function nextOnboardStep(step) {
  document.querySelectorAll(".onboarding-step").forEach(s => s.classList.remove("active"));
  document.getElementById(`onboardStep${step}`).classList.add("active");
}

function skipOnboarding() {
  localStorage.setItem("arbscanner_onboarded", "1");
  document.getElementById("onboardingModal").classList.remove("open");
}

async function finishOnboarding() {
  const key = document.getElementById("onboardApiKey").value.trim();
  if (key) {
    await saveConfig({ odds_api_key: key });
  }
  localStorage.setItem("arbscanner_onboarded", "1");
  document.getElementById("onboardingModal").classList.remove("open");
  if (key) {
    showToast("API key saved — running first scan...");
    runScan("full");
  }
}

// ─── Bet Journal ──────────────────────────────────────────────────────────────

let _bets = [];

async function loadBets() {
  try {
    const resp = await fetch(`${API_BASE}/bets`);
    const data = await resp.json();
    _bets = data.bets || [];
  } catch (e) {
    try { _bets = JSON.parse(localStorage.getItem("arbscanner_bets") || "[]"); } catch { _bets = []; }
  }
  renderJournal();
}

function _buildBetRow(b) {
  const tr = document.createElement("tr");
  const date = b.created_at ? new Date(b.created_at).toLocaleDateString() : "--";

  const cells = [
    date,
    truncate(b.event, 30),
    b.sport,
    b.platform,
    b.side,
    formatOdds(b.odds),
    formatMoney(b.stake),
  ];

  cells.forEach(text => {
    const td = document.createElement("td");
    td.textContent = text;
    tr.appendChild(td);
  });

  // Status cell
  const statusTd = document.createElement("td");
  const badge = document.createElement("span");
  badge.className = `bet-status ${b.status}`;
  badge.textContent = b.status.toUpperCase();
  statusTd.appendChild(badge);
  tr.appendChild(statusTd);

  // P&L cell
  const pnlTd = document.createElement("td");
  pnlTd.textContent = b.status !== "open" ? formatMoney(b.pnl) : "--";
  if (b.pnl > 0) pnlTd.style.color = "var(--green)";
  else if (b.pnl < 0) pnlTd.style.color = "var(--red)";
  tr.appendChild(pnlTd);

  // Action cell
  const actTd = document.createElement("td");
  if (b.status === "open") {
    ["won", "lost", "void"].forEach(outcome => {
      const btn = document.createElement("button");
      btn.className = `bet-action-btn${outcome !== "void" ? " resolve" : ""}`;
      btn.textContent = outcome.charAt(0).toUpperCase() + outcome.slice(1);
      btn.addEventListener("click", () => resolveBet(b.id, outcome));
      actTd.appendChild(btn);
    });
  } else {
    const del = document.createElement("button");
    del.className = "bet-action-btn delete";
    del.textContent = "Del";
    del.addEventListener("click", () => deleteBet(b.id));
    actTd.appendChild(del);
  }
  tr.appendChild(actTd);
  return tr;
}

function renderJournal() {
  const filter = document.getElementById("journalFilter").value;
  const bets = filter === "all" ? _bets : _bets.filter(b => b.status === filter);
  const tbody = document.getElementById("journalTableBody");
  const empty = document.getElementById("journalEmpty");

  tbody.replaceChildren();
  if (bets.length === 0) {
    empty.style.display = "flex";
  } else {
    empty.style.display = "none";
    bets.forEach(b => tbody.appendChild(_buildBetRow(b)));
  }

  // Update journal stats
  const resolved = _bets.filter(b => b.status !== "open");
  const wins = _bets.filter(b => b.status === "won").length;
  const totalPnL = resolved.reduce((s, b) => s + (b.pnl || 0), 0);
  const totalStaked = resolved.reduce((s, b) => s + (b.stake || 0), 0);

  document.getElementById("journalTotalBets").textContent = _bets.length;
  document.getElementById("journalWinRate").textContent = resolved.length > 0
    ? (wins / resolved.length * 100).toFixed(0) + "%" : "--%";

  const pnlEl = document.getElementById("journalPnL");
  pnlEl.textContent = formatMoney(totalPnL);
  pnlEl.className = `stat-value ${totalPnL > 0 ? "green" : totalPnL < 0 ? "red" : ""}`;

  document.getElementById("journalROI").textContent = totalStaked > 0
    ? (totalPnL / totalStaked * 100).toFixed(1) + "%" : "--%";
}

function trackBet(oppId) {
  const opp = state.filteredOpps.find(o => o.id === oppId);
  if (!opp) return;

  document.getElementById("betLogOppId").value = oppId;
  document.getElementById("betLogEvent").value = opp.event_detail || opp.event || "";
  document.getElementById("betLogSport").value = opp.sport || "";
  document.getElementById("betLogPlatform").value = opp.platform_a.name || "";
  document.getElementById("betLogSide").value = opp.platform_a.side || "";
  document.getElementById("betLogOdds").value = opp.platform_a.american_odds || "";
  document.getElementById("betLogType").value = opp.type === "ev" ? "ev" : "arb";

  const bankroll = state.config.default_bankroll || 100;
  if (opp.type === "ev") {
    const kelly = computeKelly(opp);
    const fraction = kelly.adaptive > 0 ? kelly.adaptive : (kelly.half > 0 ? kelly.half : 0.05);
    document.getElementById("betLogStake").value = (fraction * bankroll).toFixed(2);
  } else {
    document.getElementById("betLogStake").value = (bankroll * (opp.platform_a.implied_prob || 0.5)).toFixed(2);
  }

  document.getElementById("betLogNotes").value = "";
  document.getElementById("betLogModal").classList.add("open");
}

function closeBetModal() {
  document.getElementById("betLogModal").classList.remove("open");
}

async function saveBet() {
  const bet = {
    opp_id: document.getElementById("betLogOppId").value,
    event: document.getElementById("betLogEvent").value,
    sport: document.getElementById("betLogSport").value,
    platform: document.getElementById("betLogPlatform").value,
    side: document.getElementById("betLogSide").value,
    odds: parseFloat(document.getElementById("betLogOdds").value) || 0,
    stake: parseFloat(document.getElementById("betLogStake").value) || 0,
    bet_type: document.getElementById("betLogType").value,
    notes: document.getElementById("betLogNotes").value,
    action: "create",
  };

  try {
    await fetch(`${API_BASE}/bets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(bet),
    });
  } catch (e) {
    bet.id = Date.now().toString(36);
    bet.status = "open";
    bet.pnl = 0;
    bet.created_at = new Date().toISOString();
    const local = JSON.parse(localStorage.getItem("arbscanner_bets") || "[]");
    local.unshift(bet);
    localStorage.setItem("arbscanner_bets", JSON.stringify(local));
  }

  closeBetModal();
  showToast("Bet tracked");
  loadBets();
}

async function resolveBet(id, outcome) {
  try {
    await fetch(`${API_BASE}/bets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "resolve", id, outcome }),
    });
  } catch (e) {
    const local = JSON.parse(localStorage.getItem("arbscanner_bets") || "[]");
    const bet = local.find(b => b.id === id);
    if (bet) {
      bet.status = outcome;
      const odds = bet.odds || 0;
      if (outcome === "won") {
        bet.pnl = odds > 0 ? bet.stake * (odds / 100) : bet.stake * (100 / Math.abs(odds));
      } else if (outcome === "void") { bet.pnl = 0; }
      else { bet.pnl = -bet.stake; }
      bet.resolved_at = new Date().toISOString();
      localStorage.setItem("arbscanner_bets", JSON.stringify(local));
    }
  }
  loadBets();
}

async function deleteBet(id) {
  try {
    await fetch(`${API_BASE}/bets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "delete", id }),
    });
  } catch (e) {
    const local = JSON.parse(localStorage.getItem("arbscanner_bets") || "[]");
    localStorage.setItem("arbscanner_bets", JSON.stringify(local.filter(b => b.id !== id)));
  }
  loadBets();
}

// ─── Analytics ────────────────────────────────────────────────────────────────

let _scanHistory = [];

async function loadAnalytics() {
  try {
    const resp = await fetch(`${API_BASE}/bets?endpoint=scan_history`);
    const data = await resp.json();
    _scanHistory = data.scan_history || [];
  } catch (e) {
    _scanHistory = [];
  }

  await loadBets();
  renderAnalytics();
}

let _analyticsFilter = "all"; // "all", "arb", "ev"

function setupAnalyticsToggle() {
  document.querySelectorAll(".analytics-type-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".analytics-type-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      _analyticsFilter = btn.dataset.atype;
      renderAnalytics();
    });
  });
}

function renderAnalytics() {
  const filter = _analyticsFilter;

  // ── Scan summary stats (separated) ──
  document.getElementById("analyticsScans").textContent = _scanHistory.length;

  const totalArbs = _scanHistory.reduce((s, h) => s + (h.arb_count || 0), 0);
  const totalEvs = _scanHistory.reduce((s, h) => s + (h.ev_count || 0), 0);
  document.getElementById("analyticsArbCount").textContent = totalArbs;
  document.getElementById("analyticsEvCount").textContent = totalEvs;

  if (_scanHistory.length > 0) {
    const avgArbs = totalArbs / _scanHistory.length;
    const avgEvs = totalEvs / _scanHistory.length;
    document.getElementById("analyticsAvgArbs").textContent = avgArbs.toFixed(1);
    document.getElementById("analyticsAvgEvs").textContent = avgEvs.toFixed(1);

    // Best edges — we store overall best_edge per scan, but arb/ev split
    // isn't in the DB schema yet. Use best_edge as proxy for now.
    const bestEdge = Math.max(..._scanHistory.map(h => h.best_edge || 0));
    // For arb-specific: scans with arbs likely had arb best_edge
    const arbScans = _scanHistory.filter(h => h.arb_count > 0);
    const evScans = _scanHistory.filter(h => h.ev_count > 0);
    const bestArbEdge = arbScans.length > 0 ? Math.max(...arbScans.map(h => h.best_edge || 0)) : 0;
    const bestEvEdge = evScans.length > 0 ? Math.max(...evScans.map(h => h.best_edge || 0)) : 0;
    document.getElementById("analyticsBestArb").textContent = bestArbEdge > 0 ? bestArbEdge.toFixed(1) + "%" : "--%";
    document.getElementById("analyticsBestEv").textContent = bestEvEdge > 0 ? bestEvEdge.toFixed(1) + "%" : "--%";

    // Most common sport
    const sportCounts = {};
    _scanHistory.forEach(h => {
      (h.sports || "").split(",").filter(Boolean).forEach(s => {
        sportCounts[s] = (sportCounts[s] || 0) + 1;
      });
    });
    const topSport = Object.entries(sportCounts).sort((a, b) => b[1] - a[1])[0];
    document.getElementById("analyticsTopSport").textContent = topSport ? topSport[0] : "--";

    // Best hour — filter by type
    const hourCounts = {};
    _scanHistory.forEach(h => {
      let count = h.opp_count;
      if (filter === "arb") count = h.arb_count || 0;
      else if (filter === "ev") count = h.ev_count || 0;
      if (count > 0) {
        hourCounts[h.hour] = (hourCounts[h.hour] || 0) + count;
      }
    });
    const bestHour = Object.entries(hourCounts).sort((a, b) => b[1] - a[1])[0];
    if (bestHour) {
      const h = parseInt(bestHour[0]);
      const label = h === 0 ? "12 AM" : h < 12 ? `${h} AM` : h === 12 ? "12 PM" : `${h - 12} PM`;
      document.getElementById("analyticsBestHour").textContent = label;
    }

    // By-hour chart (filtered)
    renderBarChart("chartByHour", hourCounts, h => {
      const hr = parseInt(h);
      return hr === 0 ? "12a" : hr < 12 ? `${hr}a` : hr === 12 ? "12p" : `${hr - 12}p`;
    });

    // Edge distribution (filtered by type)
    const edgeBuckets = { "0-1%": 0, "1-2%": 0, "2-3%": 0, "3-5%": 0, "5%+": 0 };
    _scanHistory.forEach(h => {
      // Only count scans relevant to the filter
      if (filter === "arb" && !h.arb_count) return;
      if (filter === "ev" && !h.ev_count) return;
      const e = h.best_edge || 0;
      if (e < 1) edgeBuckets["0-1%"]++;
      else if (e < 2) edgeBuckets["1-2%"]++;
      else if (e < 3) edgeBuckets["2-3%"]++;
      else if (e < 5) edgeBuckets["3-5%"]++;
      else edgeBuckets["5%+"]++;
    });
    renderBarChart("chartEdgeDist", edgeBuckets);
  }

  // ── P&L chart from bet journal (filtered by type) ──
  let resolved = _bets.filter(b => b.status !== "open" && b.resolved_at);
  if (filter === "arb") resolved = resolved.filter(b => b.bet_type === "arb");
  else if (filter === "ev") resolved = resolved.filter(b => b.bet_type === "ev");

  const pnlContainer = document.getElementById("chartPnL");
  if (resolved.length > 1) {
    resolved.sort((a, b) => new Date(a.resolved_at) - new Date(b.resolved_at));
    let cumPnL = 0;
    const points = resolved.map(b => {
      cumPnL += b.pnl || 0;
      return { date: b.resolved_at, pnl: cumPnL };
    });
    renderPnLChart("chartPnL", points);
  } else {
    const ph = document.createElement("div");
    ph.className = "chart-placeholder";
    ph.textContent = filter === "all"
      ? "Track bets in the Journal to see your P&L curve"
      : `No resolved ${filter === "arb" ? "arb" : "+EV"} bets yet`;
    pnlContainer.replaceChildren(ph);
  }

  // ── Journal Performance breakdown ──
  const arbBets = _bets.filter(b => b.bet_type === "arb");
  const evBets = _bets.filter(b => b.bet_type === "ev");
  const arbResolved = arbBets.filter(b => b.status !== "open");
  const evResolved = evBets.filter(b => b.status !== "open");
  const arbWins = arbBets.filter(b => b.status === "won").length;
  const arbLosses = arbBets.filter(b => b.status === "lost").length;
  const evWins = evBets.filter(b => b.status === "won").length;
  const evLosses = evBets.filter(b => b.status === "lost").length;
  const arbPnL = arbResolved.reduce((s, b) => s + (b.pnl || 0), 0);
  const evPnL = evResolved.reduce((s, b) => s + (b.pnl || 0), 0);
  const totalStaked = [...arbResolved, ...evResolved].reduce((s, b) => s + (b.stake || 0), 0);

  document.getElementById("perfTotalBets").textContent = _bets.length;
  document.getElementById("perfArbBets").textContent = `${arbBets.length} (${arbWins}W / ${arbLosses}L)`;

  const arbPnLEl = document.getElementById("perfArbPnL");
  arbPnLEl.textContent = formatMoney(arbPnL);
  arbPnLEl.style.color = arbPnL > 0 ? "var(--green)" : arbPnL < 0 ? "var(--red)" : "";

  document.getElementById("perfEvBets").textContent = `${evBets.length} (${evWins}W / ${evLosses}L)`;

  const evPnLEl = document.getElementById("perfEvPnL");
  evPnLEl.textContent = formatMoney(evPnL);
  evPnLEl.style.color = evPnL > 0 ? "var(--green)" : evPnL < 0 ? "var(--red)" : "";

  document.getElementById("perfEvWinRate").textContent = evResolved.length > 0
    ? (evWins / evResolved.length * 100).toFixed(0) + "%" : "--%";

  const totalPnL = arbPnL + evPnL;
  const roiEl = document.getElementById("perfROI");
  roiEl.textContent = totalStaked > 0 ? (totalPnL / totalStaked * 100).toFixed(1) + "%" : "--%";
  roiEl.style.color = totalPnL > 0 ? "var(--green)" : totalPnL < 0 ? "var(--red)" : "";

  // ── Scanner Auto-Tracker Stats ──
  renderTrackerStats();
}

function renderTrackerStats() {
  const tracker = state.meta?.tracker;
  if (!tracker || !tracker.stats) return;

  const arb = tracker.stats.arb || {};
  const ev = tracker.stats.ev || {};

  // Arb stats
  document.getElementById("trackerArbTotal").textContent = arb.total || 0;
  document.getElementById("trackerArbResolved").textContent = (arb.won || 0) + (arb.lost || 0);
  const arbPnLEl = document.getElementById("trackerArbPnL");
  arbPnLEl.textContent = formatMoney(arb.pnl || 0);
  arbPnLEl.style.color = (arb.pnl || 0) > 0 ? "var(--green)" : (arb.pnl || 0) < 0 ? "var(--red)" : "";
  const arbResolved = (arb.won || 0) + (arb.lost || 0);
  document.getElementById("trackerArbAvg").textContent = arbResolved > 0
    ? formatMoney((arb.pnl || 0) / arbResolved) : "$0.00";

  // EV stats
  document.getElementById("trackerEvTotal").textContent = ev.total || 0;
  const evWon = ev.won || 0;
  const evLost = ev.lost || 0;
  const evResolved = evWon + evLost;
  document.getElementById("trackerEvResolved").textContent = `${evResolved} (${evWon}W / ${evLost}L)`;
  document.getElementById("trackerEvWinRate").textContent = evResolved > 0
    ? (evWon / evResolved * 100).toFixed(0) + "%" : "--%";
  const evPnLEl = document.getElementById("trackerEvPnL");
  evPnLEl.textContent = formatMoney(ev.pnl || 0);
  evPnLEl.style.color = (ev.pnl || 0) > 0 ? "var(--green)" : (ev.pnl || 0) < 0 ? "var(--red)" : "";
  document.getElementById("trackerEvPending").textContent = ev.pending || 0;

  // Build cumulative P&L charts from recent resolved bets
  const recent = tracker.recent || [];
  if (recent.length > 1) {
    const arbRecent = recent.filter(r => r.opp_type === "arb").reverse();
    const evRecent = recent.filter(r => r.opp_type === "ev").reverse();

    if (arbRecent.length > 1) {
      let cum = 0;
      const pts = arbRecent.map(r => { cum += r.pnl || 0; return { date: r.resolved_at, pnl: cum }; });
      renderPnLChart("chartTrackerArb", pts);
    }
    if (evRecent.length > 1) {
      let cum = 0;
      const pts = evRecent.map(r => { cum += r.pnl || 0; return { date: r.resolved_at, pnl: cum }; });
      renderPnLChart("chartTrackerEv", pts);
    }
  }
}

function renderBarChart(containerId, dataObj, labelFn) {
  const container = document.getElementById(containerId);
  const entries = Object.entries(dataObj);
  if (entries.length === 0) return;

  const maxVal = Math.max(...entries.map(e => e[1]), 1);
  const chart = document.createElement("div");
  chart.className = "bar-chart";

  entries.forEach(([key, val]) => {
    const label = labelFn ? labelFn(key) : key;
    const height = Math.max(2, (val / maxVal) * 110);
    const col = document.createElement("div");
    col.className = "bar-col";

    const bar = document.createElement("div");
    bar.className = "bar";
    bar.style.height = height + "px";
    bar.title = `${key}: ${val}`;

    const lbl = document.createElement("div");
    lbl.className = "bar-label";
    lbl.textContent = label;

    col.appendChild(bar);
    col.appendChild(lbl);
    chart.appendChild(col);
  });

  container.replaceChildren(chart);
}

function renderPnLChart(containerId, points) {
  const container = document.getElementById(containerId);
  if (points.length < 2) return;

  const vals = points.map(p => p.pnl);
  const minV = Math.min(0, ...vals);
  const maxV = Math.max(0, ...vals);
  const range = maxV - minV || 1;
  const w = 100;
  const h = 130;
  const zeroY = h - ((0 - minV) / range) * h;

  const pathPoints = points.map((p, i) => {
    const x = (i / (points.length - 1)) * w;
    const y = h - ((p.pnl - minV) / range) * h;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");

  const color = vals[vals.length - 1] >= 0 ? "var(--green)" : "var(--red)";

  const wrapper = document.createElement("div");
  wrapper.className = "pnl-chart";

  // SVG for the line chart — data is numeric (not user input), safe for SVG construction
  const svgNS = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(svgNS, "svg");
  svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
  svg.setAttribute("preserveAspectRatio", "none");

  const line = document.createElementNS(svgNS, "line");
  line.setAttribute("x1", "0"); line.setAttribute("y1", String(zeroY));
  line.setAttribute("x2", String(w)); line.setAttribute("y2", String(zeroY));
  line.setAttribute("stroke", "var(--border-subtle)");
  line.setAttribute("stroke-width", "0.5");
  line.setAttribute("stroke-dasharray", "2");

  const poly = document.createElementNS(svgNS, "polyline");
  poly.setAttribute("points", pathPoints);
  poly.setAttribute("fill", "none");
  poly.setAttribute("stroke", color);
  poly.setAttribute("stroke-width", "1.5");

  svg.appendChild(line);
  svg.appendChild(poly);
  wrapper.appendChild(svg);

  const label = document.createElement("div");
  label.style.cssText = `position:absolute;top:4px;right:8px;font-size:0.6rem;color:${color};font-family:var(--font-mono);font-weight:700`;
  label.textContent = formatMoney(vals[vals.length - 1]);
  wrapper.appendChild(label);

  container.replaceChildren(wrapper);
}

// Log scan results for analytics
async function logScanToHistory(data) {
  if (!data || !data.opportunities) return;
  const opps = data.opportunities;
  const arbs = opps.filter(o => o.type === "arb");
  const evs = opps.filter(o => o.type === "ev");
  const edges = opps.map(o => o.ev_pct || o.net_arb_pct || 0);
  const bestEdge = edges.length > 0 ? Math.max(...edges) : 0;
  const avgEdge = edges.length > 0 ? edges.reduce((s, e) => s + e, 0) / edges.length : 0;
  const sports = [...new Set(opps.map(o => o.sport).filter(Boolean))].join(",");

  try {
    await fetch(`${API_BASE}/bets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "log_scan",
        opp_count: opps.length,
        arb_count: arbs.length,
        ev_count: evs.length,
        best_edge: bestEdge,
        avg_edge: avgEdge,
        sports,
      }),
    });
  } catch (e) { /* analytics logging is best-effort */ }
}

// ─── Initialize ───────────────────────────────────────────────────────────────

async function init() {
  restoreFiltersFromStorage();
  setupEventListeners();
  setupTabs();
  setupAnalyticsToggle();

  // Journal filter listener
  const journalFilter = document.getElementById("journalFilter");
  if (journalFilter) journalFilter.addEventListener("change", renderJournal);
  const btnManualBet = document.getElementById("btnAddBetManual");
  if (btnManualBet) btnManualBet.addEventListener("click", () => {
    document.getElementById("betLogOppId").value = "";
    document.getElementById("betLogEvent").value = "";
    document.getElementById("betLogSport").value = "";
    document.getElementById("betLogPlatform").value = "";
    document.getElementById("betLogSide").value = "";
    document.getElementById("betLogOdds").value = "";
    document.getElementById("betLogStake").value = "";
    document.getElementById("betLogType").value = "manual";
    document.getElementById("betLogNotes").value = "";
    document.getElementById("betLogModal").classList.add("open");
  });

  // Show cached data instantly (stale-while-revalidate)
  const cached = localStorage.getItem("arbscanner_last_scan");
  if (cached) {
    try {
      const data = JSON.parse(cached);
      state.opportunities = data.opportunities || [];
      state.meta = data.meta || {};
      updateSourceStatus(data.meta?.sources, data.meta?.errors);
      applyFilters();
    } catch (e) { /* ignore corrupt cache */ }
  }

  // Fire config + scan in parallel (scan reads API key from env on backend)
  const configPromise = loadConfig();
  const scanPromise = runScan("full");
  await configPromise;

  // Check if onboarding needed (after config loads)
  checkOnboarding();

  document.getElementById("countdown").textContent = "manual";
}

// Start
init();

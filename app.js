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
  sortColumn: "net",
  sortDirection: "desc",
  previousIds: new Set(),
  sessionBest: 0,
  sessionCount: 0,
};

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

function platformClass(name) {
  if (!name) return "";
  const n = name.toLowerCase();
  if (n.includes("polymarket")) return "polymarket";
  if (n.includes("kalshi")) return "kalshi";
  if (n.includes("draftkings") || n.includes("draft")) return "draftkings";
  if (n.includes("fanduel") || n.includes("fan")) return "fanduel";
  if (n.includes("betrivers") || n.includes("rivers")) return "betrivers";
  if (n.includes("pinnacle")) return "pinnacle";
  if (n.includes("betmgm") || n.includes("mgm")) return "betmgm";
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

async function fetchScan() {
  const params = new URLSearchParams();
  if (state.config.odds_api_key) params.set("api_key", state.config.odds_api_key);
  const minPct = parseFloat(document.getElementById("minProfitSlider").value) || 0;
  if (minPct > 0) params.set("min_pct", minPct.toString());

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

  const marketTypes = [];
  if (document.getElementById("fMoneyline").checked) marketTypes.push("h2h");
  if (document.getElementById("fSpreads").checked) marketTypes.push("spreads");
  if (document.getElementById("fTotals").checked) marketTypes.push("totals");
  if (document.getElementById("fProps").checked) marketTypes.push("player_points", "player_rebounds", "player_assists", "player_threes");

  const includeLive = document.getElementById("fIncludeLive").checked;
  const minProfit = parseFloat(document.getElementById("minProfitSlider").value) || 0;

  return { sports, platforms, marketTypes, includeLive, minProfit };
}

function applyFilters() {
  const filters = getActiveFilters();
  let opps = state.opportunities;

  opps = opps.filter(o => {
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

    // Min profit
    if (o.net_arb_pct < filters.minProfit) return false;

    return true;
  });

  // Sort
  const sortBy = document.getElementById("sortSelect").value;
  opps.sort((a, b) => {
    let va, vb;
    switch (sortBy) {
      case "net_pct": va = a.net_arb_pct; vb = b.net_arb_pct; break;
      case "gross_pct": va = a.gross_arb_pct; vb = b.gross_arb_pct; break;
      case "time":
        va = a.commence_time ? new Date(a.commence_time).getTime() : Infinity;
        vb = b.commence_time ? new Date(b.commence_time).getTime() : Infinity;
        return va - vb; // Ascending for time
      case "liquidity": va = a.liquidity || 0; vb = b.liquidity || 0; break;
      case "confidence": va = a.match_confidence || 0; vb = b.match_confidence || 0; break;
      default: va = a.net_arb_pct; vb = b.net_arb_pct;
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
      case "net": va = a.net_arb_pct; vb = b.net_arb_pct; break;
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
    const tier = tierClass(opp.net_arb_pct);
    const rowClass = `opp-row ${tier} ${isNew ? "new-row-enter" : ""}`;

    const timeCell = opp.is_live
      ? '<span class="live-badge"><span class="live-pulse"></span>LIVE</span>'
      : escapeHtml(opp.time_display || "--");

    const oddsA = opp.platform_a.implied_prob
      ? `${formatProb(opp.platform_a.implied_prob)}<br><span style="color:var(--text-dim);font-size:0.6rem">${formatOdds(opp.platform_a.american_odds)}</span>`
      : formatOdds(opp.platform_a.american_odds);

    const oddsB = opp.platform_b.implied_prob
      ? `${formatProb(opp.platform_b.implied_prob)}<br><span style="color:var(--text-dim);font-size:0.6rem">${formatOdds(opp.platform_b.american_odds)}</span>`
      : formatOdds(opp.platform_b.american_odds);

    html += `
      <tr class="${rowClass}" data-id="${opp.id}" onclick="toggleDetail('${opp.id}')">
        <td><span class="sport-tag ${sportClass(opp.sport)}">${escapeHtml(opp.sport)}</span></td>
        <td title="${escapeHtml(opp.event_detail || opp.event)}" style="white-space:normal;line-height:1.3">${escapeHtml(truncate(opp.event, 35))}</td>
        <td>${timeCell}</td>
        <td><span class="platform-name ${platformClass(opp.platform_a.name)}">${escapeHtml(opp.platform_a.name)}</span></td>
        <td>${escapeHtml(opp.platform_a.side)}</td>
        <td class="odds-cell">${oddsA}</td>
        <td><span class="platform-name ${platformClass(opp.platform_b.name)}">${escapeHtml(opp.platform_b.name)}</span></td>
        <td>${escapeHtml(opp.platform_b.side)}</td>
        <td class="odds-cell">${oddsB}</td>
        <td class="profit-cell ${profitClass(opp.gross_arb_pct)}">${formatPct(opp.gross_arb_pct)}</td>
        <td class="profit-cell ${profitClass(opp.net_arb_pct)}">${formatPct(opp.net_arb_pct)}</td>
        <td>
          <div class="tooltip-wrapper">
            ${riskIcon(opp.resolution_risk)}
            <div class="tooltip-text">${escapeHtml(opp.risk_note || "No additional risk notes")}</div>
          </div>
        </td>
        <td>
          <button class="copy-btn" onclick="event.stopPropagation(); copyOpp('${opp.id}')" title="Copy details">⧉</button>
        </td>
      </tr>
      <tr class="detail-panel" id="detail-${opp.id}">
        <td colspan="13">
          <div class="detail-content" id="detail-content-${opp.id}">
            <div class="detail-grid">
              <div class="detail-section">
                <h4>Stake Calculator</h4>
                <div class="stake-input-row">
                  <label>Bankroll: $</label>
                  <input type="number" value="${state.config.default_bankroll || 100}" min="1" onchange="recalcStakes('${opp.id}', this.value)">
                </div>
                <div class="stake-result" id="stakes-${opp.id}">
                  ${renderStakes(opp, state.config.default_bankroll || 100)}
                </div>
              </div>
              <div class="detail-section">
                <h4>Fee Breakdown</h4>
                <table class="fee-table">
                  <tr><td>${escapeHtml(opp.platform_a.name)} fee</td><td>${opp.platform_a.fee_pct.toFixed(1)}%</td></tr>
                  <tr><td>${escapeHtml(opp.platform_b.name)} fee</td><td>${opp.platform_b.fee_pct.toFixed(1)}%</td></tr>
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
                <h4>Market Details</h4>
                <div style="font-size:0.72rem;color:var(--text-secondary);line-height:1.6">
                  <div><strong>Event:</strong> ${escapeHtml(opp.event_detail || opp.event)}</div>
                  <div><strong>Type:</strong> ${escapeHtml(opp.market_type)}</div>
                  <div><strong>Status:</strong> ${opp.is_live ? '<span style="color:var(--red)">LIVE</span>' : 'Pre-match'}</div>
                  ${opp.commence_time ? `<div><strong>Start:</strong> ${new Date(opp.commence_time).toLocaleString()}</div>` : ""}
                </div>
                <div class="detail-links">
                  ${opp.platform_a.url ? `<a href="${opp.platform_a.url}" target="_blank" rel="noopener noreferrer">${escapeHtml(opp.platform_a.name)} →</a>` : ""}
                  ${opp.platform_b.url ? `<a href="${opp.platform_b.url}" target="_blank" rel="noopener noreferrer">${escapeHtml(opp.platform_b.name)} →</a>` : ""}
                </div>
              </div>
            </div>
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

  if (!pa || !pb || pa <= 0 || pb <= 0) {
    return '<div style="color:var(--text-dim);font-size:0.72rem">Insufficient data for stake calculation</div>';
  }

  const stakeA = (bankroll * pa).toFixed(2);
  const stakeB = (bankroll * pb).toFixed(2);
  const totalStaked = (parseFloat(stakeA) + parseFloat(stakeB)).toFixed(2);
  const profit = (bankroll - parseFloat(totalStaked)).toFixed(2);
  const roi = ((profit / totalStaked) * 100).toFixed(2);

  return `
    <div class="stake-line"><span class="label">Stake on ${escapeHtml(opp.platform_a.name)} (${escapeHtml(opp.platform_a.side)}):</span><span class="value">${formatMoney(stakeA)}</span></div>
    <div class="stake-line"><span class="label">Stake on ${escapeHtml(opp.platform_b.name)} (${escapeHtml(opp.platform_b.side)}):</span><span class="value">${formatMoney(stakeB)}</span></div>
    <div class="stake-line total"><span class="label">Total staked:</span><span class="value">${formatMoney(totalStaked)}</span></div>
    <div class="stake-line"><span class="label">Payout (either outcome):</span><span class="value">${formatMoney(bankroll)}</span></div>
    <div class="stake-line"><span class="label">Guaranteed profit:</span><span class="value green">${formatMoney(profit)} (${roi}%)</span></div>
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

  const text = [
    `⚡ ArbScanner Alert`,
    `Event: ${opp.event_detail || opp.event}`,
    `Sport: ${opp.sport}`,
    ``,
    `Platform A: ${opp.platform_a.name} — ${opp.platform_a.side} @ ${formatProb(opp.platform_a.implied_prob)} (${formatOdds(opp.platform_a.american_odds)})`,
    `Platform B: ${opp.platform_b.name} — ${opp.platform_b.side} @ ${formatProb(opp.platform_b.implied_prob)} (${formatOdds(opp.platform_b.american_odds)})`,
    ``,
    `Gross: ${formatPct(opp.gross_arb_pct)} | Net: ${formatPct(opp.net_arb_pct)}`,
    `Match confidence: ${(opp.match_confidence * 100).toFixed(0)}%`,
    `Risk: ${opp.resolution_risk}`,
  ].join("\n");

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

  document.getElementById("statOpps").textContent = opps.length;
  document.getElementById("statOppsSub").textContent = opps.length > 0 ? "active opportunities" : "no opportunities found";

  if (opps.length > 0) {
    const best = opps[0];
    document.getElementById("statBestArb").textContent = formatPct(best.net_arb_pct);
    document.getElementById("statBestArbSub").textContent = truncate(best.event, 25);
    document.getElementById("statBestArb").className = `stat-value ${best.net_arb_pct >= 3 ? "green" : best.net_arb_pct >= 1 ? "yellow" : ""}`;

    const avg = opps.reduce((s, o) => s + o.net_arb_pct, 0) / opps.length;
    document.getElementById("statAvgProfit").textContent = formatPct(avg);
    document.getElementById("statAvgProfit").className = `stat-value ${avg >= 3 ? "green" : avg >= 1 ? "yellow" : ""}`;

    // Session stats
    state.sessionCount = Math.max(state.sessionCount, opps.length);
    state.sessionBest = Math.max(state.sessionBest, best.net_arb_pct);
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
    const avg = opps.reduce((s, o) => s + o.net_arb_pct, 0) / opps.length;
    document.getElementById("footerAvg").textContent = formatPct(avg);
  }
}

function updateSourceStatus(sources, errors) {
  if (!sources) return;
  const statusMap = {
    ok: "ok", empty: "stale", error: "error", no_key: "no_key",
    pending: "pending", ok_no_arbs: "ok",
    quota_exceeded: "error", invalid_key: "error",
  };

  for (const [key, status] of Object.entries(sources)) {
    const dotId = `status${key.charAt(0).toUpperCase() + key.slice(1)}`;
    const dot = document.getElementById(dotId);
    if (!dot) continue;
    dot.className = "status-dot";
    dot.classList.add(statusMap[status] || "error");
  }

  // Show API key warning banner if needed
  const banner = document.getElementById("demoBanner");
  const sbStatus = sources.sportsbook;
  if (sbStatus === "quota_exceeded" || sbStatus === "invalid_key" || sbStatus === "no_key" ||
      (sbStatus === "error" && errors && errors.length)) {
    banner.textContent = "";
    banner.className = "demo-banner warning";
    const icon = document.createTextNode("\u26A0 ");
    banner.appendChild(icon);
    const strong = document.createElement("strong");
    if (sbStatus === "quota_exceeded") {
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
    } else if (sbStatus === "invalid_key") {
      strong.textContent = "Odds API key invalid";
      banner.appendChild(strong);
      banner.appendChild(document.createTextNode(" \u2014 update your key in "));
      const link = document.createElement("a");
      link.href = "#";
      link.style.cssText = "color:inherit;text-decoration:underline";
      link.textContent = "Settings";
      link.addEventListener("click", (e) => { e.preventDefault(); document.getElementById("settingsDrawer").classList.add("open"); });
      banner.appendChild(link);
      banner.appendChild(document.createTextNode("."));
    } else if (sbStatus === "no_key") {
      strong.textContent = "No Odds API key";
      banner.appendChild(strong);
      banner.appendChild(document.createTextNode(" \u2014 add your key in "));
      const link = document.createElement("a");
      link.href = "#";
      link.style.cssText = "color:inherit;text-decoration:underline";
      link.textContent = "Settings";
      link.addEventListener("click", (e) => { e.preventDefault(); document.getElementById("settingsDrawer").classList.add("open"); });
      banner.appendChild(link);
      banner.appendChild(document.createTextNode(" to enable sportsbook data."));
    } else {
      strong.textContent = "Sportsbook error";
      banner.appendChild(strong);
      const errMsg = (errors || []).find(e => e.startsWith("Sportsbook:")) || (errors || [])[0] || "Unknown error";
      banner.appendChild(document.createTextNode(" \u2014 " + errMsg));
    }
  } else {
    banner.className = "demo-banner hidden";
  }
}

// ─── Scan Execution ───────────────────────────────────────────────────────────

async function runScan() {
  if (state.isLoading) return;
  state.isLoading = true;

  const overlay = document.getElementById("loadingOverlay");
  const btn = document.getElementById("btnRefresh");
  overlay.classList.add("active");
  btn.disabled = true;
  btn.textContent = "⟳ SCANNING...";

  try {
    const data = await fetchScan();

    state.opportunities = data.opportunities || [];
    state.meta = data.meta || {};

    // Update source status
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

    // Cache for instant display on next visit
    try { localStorage.setItem("arbscanner_last_scan", JSON.stringify(data)); } catch (e) { /* quota */ }

  } catch (err) {
    console.error("Scan error:", err);
    showToast("Scan failed: " + err.message);
    // Set all statuses to error
    document.getElementById("statusPolymarket").className = "status-dot error";
    document.getElementById("statusKalshi").className = "status-dot error";
    document.getElementById("statusSportsbook").className = "status-dot error";
  } finally {
    state.isLoading = false;
    overlay.classList.remove("active");
    btn.disabled = false;
    btn.textContent = "↻ SCAN";
    resetCountdown();
  }
}

// ─── Countdown & Auto-Refresh ─────────────────────────────────────────────────

function resetCountdown() {
  state.countdownSeconds = getRefreshInterval() || (state.config.refresh_interval || 60);
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
  if (mode === "extended") return state.config.refresh_interval || 60;  // user setting
  return 0;                                                 // no auto-refresh
}

function startAutoRefresh() {
  if (state.countdownInterval) clearInterval(state.countdownInterval);
  if (state.scanInterval) clearInterval(state.scanInterval);

  state.countdownInterval = setInterval(() => {
    const mode = getScanMode();
    if (mode === "off") {
      document.getElementById("countdown").textContent = "off-hours";
      return;
    }
    // Sync interval to current mode (prime=30s, extended=configured)
    const target = getRefreshInterval();
    if (state.countdownSeconds > target) state.countdownSeconds = target;

    state.countdownSeconds--;
    if (state.countdownSeconds <= 0) {
      runScan();
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
  // Populate fields
  document.getElementById("inputOddsApiKey").value = state.config.odds_api_key || "";
  document.getElementById("inputOddsPapiKey").value = state.config.oddspapi_key || "";
  document.getElementById("inputRefreshInterval").value = state.config.refresh_interval || 60;
  document.getElementById("inputDefaultBankroll").value = state.config.default_bankroll || 100;
  document.getElementById("inputNotifyThreshold").value = state.config.notify_above_pct || 2;
  document.getElementById("inputSoundAlerts").value = state.config.sound_alerts ? "true" : "false";
}

function closeSettings() {
  document.getElementById("settingsModal").classList.remove("open");
}

async function handleSaveSettings() {
  const newConfig = {
    odds_api_key: document.getElementById("inputOddsApiKey").value.trim(),
    oddspapi_key: document.getElementById("inputOddsPapiKey").value.trim(),
    refresh_interval: parseInt(document.getElementById("inputRefreshInterval").value) || 60,
    default_bankroll: parseFloat(document.getElementById("inputDefaultBankroll").value) || 100,
    notify_above_pct: parseFloat(document.getElementById("inputNotifyThreshold").value) || 2,
    sound_alerts: document.getElementById("inputSoundAlerts").value === "true",
  };

  Object.assign(state.config, newConfig);
  await saveConfig(newConfig);
  closeSettings();
  resetCountdown();
  startAutoRefresh();
  showToast("Configuration saved");

  // Re-scan with new config
  runScan();
}

// ─── Event Listeners ──────────────────────────────────────────────────────────

function setupEventListeners() {
  // Refresh button
  document.getElementById("btnRefresh").addEventListener("click", runScan);

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
    input.addEventListener("change", applyFilters);
  });

  // Min profit slider
  const slider = document.getElementById("minProfitSlider");
  slider.addEventListener("input", () => {
    document.getElementById("minProfitValue").textContent = slider.value + "%";
  });
  slider.addEventListener("change", applyFilters);

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
      if (state.expandedRow) {
        toggleDetail(state.expandedRow);
      }
    }
    if (e.key === "r" && !e.ctrlKey && !e.metaKey && document.activeElement.tagName !== "INPUT") {
      runScan();
    }
  });
}

// ─── Initialize ───────────────────────────────────────────────────────────────

async function init() {
  setupEventListeners();

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
  const scanPromise = runScan();
  await configPromise;

  resetCountdown();
  startAutoRefresh();
}

// Start
init();

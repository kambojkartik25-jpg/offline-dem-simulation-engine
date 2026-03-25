const payloadEl = document.getElementById("payload");
const rawOutEl = document.getElementById("rawOut");
const validationOutEl = document.getElementById("validationOut");
const validationSummaryEl = document.getElementById("validationSummary");
const optOutEl = document.getElementById("optOut");
const summaryCardsEl = document.getElementById("summaryCards");
const siloTableWrapEl = document.getElementById("siloTableWrap");
const upcomingLotsWrapEl = document.getElementById("upcomingLotsWrap");
const incomingLotsWrapEl = document.getElementById("incomingLotsWrap");
const stateLedgerWrapEl = document.getElementById("stateLedgerWrap");
const remainingFocusWrapEl = document.getElementById("remainingFocusWrap");
const contributionWrapEl = document.getElementById("contributionWrap");
const runStatusEl = document.getElementById("runStatus");
const candidateTableWrapEl = document.getElementById("candidateTableWrap");
const optPresetEl = document.getElementById("opt_preset");
const optSeedEl = document.getElementById("opt_seed");
const candidateSortEl = document.getElementById("candidateSort");
const optimizeBtn = document.getElementById("optimizeBtn");
const runBtn = document.getElementById("runBtn");
const tabStudioBtn = document.getElementById("tabStudioBtn");
const tabScheduleBtn = document.getElementById("tabScheduleBtn");
const tabPanels = Array.from(document.querySelectorAll("[data-tab-panel]"));
const scheduleBrewDetailsWrapEl = document.getElementById("scheduleBrewDetailsWrap");
const schedCandidateSortEl = document.getElementById("schedCandidateSort");
const schedCandidateTableWrapEl = document.getElementById("schedCandidateTableWrap");
const schedOptimizationOutEl = document.getElementById("schedOptimizationOut");
// Schedule simulation card elements
const schedRunStatusEl = document.getElementById("schedRunStatus");
const schedSummaryCardsEl = document.getElementById("schedSummaryCards");
const schedRemainingFocusWrapEl = document.getElementById("schedRemainingFocusWrap");
const schedContributionWrapEl = document.getElementById("schedContributionWrap");
const schedSiloTableWrapEl = document.getElementById("schedSiloTableWrap");
const schedStepRunEl = document.getElementById("schedStepRun");
const schedSimStatusEl = document.getElementById("schedSimStatus");
const schedStepOptimizeEl = document.getElementById("schedStepOptimize");
const schedOptStatusEl = document.getElementById("schedOptStatus");
// Schedule generate-random card elements
const schedRandomStatusEl = document.getElementById("schedRandomStatus");
const schedGenSilosWrapEl = document.getElementById("schedGenSilosWrap");
const schedGenSuppliersWrapEl = document.getElementById("schedGenSuppliersWrap");
const schedGenLoadedLotsWrapEl = document.getElementById("schedGenLoadedLotsWrap");
const schedGenIncomingLotsWrapEl = document.getElementById("schedGenIncomingLotsWrap");
// Schedule generate-schedule card elements
const schedScheduleStatusEl = document.getElementById("schedScheduleStatus");
const schedGenBrewsWrapEl = document.getElementById("schedGenBrewsWrap");
// Buttons
const schedGenerateRandomBtn = document.getElementById("schedGenerateRandomBtn");
const schedGenerateScheduleBtn = document.getElementById("schedGenerateScheduleBtn");
const schedRunSimulationBtn = document.getElementById("schedRunSimulationBtn");
const schedOptimizeBtn = document.getElementById("schedOptimizeBtn");
const schedOptBrewIdEl = document.getElementById("sched_opt_brew_id");

const kpiValidationEl = document.getElementById("kpiValidation");
const kpiDischargedEl = document.getElementById("kpiDischarged");
const kpiRemainingEl = document.getElementById("kpiRemaining");

const stepInput = document.getElementById("stepInput");
const stepRun = document.getElementById("stepRun");
const stepResults = document.getElementById("stepResults");
const stepOptimize = document.getElementById("stepOptimize");
const statusInput = document.getElementById("statusInput");
const statusRun = document.getElementById("statusRun");
const statusResults = document.getElementById("statusResults");
const statusOptimize = document.getElementById("statusOptimize");
let lastRunResult = null;
let lastOptimizePayload = null;
let lastOptimizeContext = { mode: "studio", scheduleId: "", brewId: "" };
let isValidating = false;
let isRunning = false;
let isOptimizing = false;
let currentScheduleId = "";
let scheduleSimulationSnapshot = null;
const scheduleBrewState = new Map();

function nowIso() {
  return new Date().toISOString();
}

function formatTimeLabel(value) {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

function formatMass(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return `${Number(value).toFixed(3)} kg`;
}

function formatEstimatedParam(key, value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  if (key === "diastatic_power_WK") return String(Math.round(n));
  return n.toFixed(2);
}

function updateScheduleBrewState(brewId, patch) {
  if (!brewId) return;
  const prev = scheduleBrewState.get(brewId) || {
    brew_id: brewId,
    brew_index: null,
    simulation_status: "pending",
    simulation_at: null,
    simulation_note: "Not run",
    optimization_status: "pending",
    optimization_at: null,
    top_candidate_mass_kg: null,
    candidate_count: 0,
    discharge_status: "pending",
    discharge_at: null,
    discharged_mass_kg: null,
    selected_candidate_index: null,
    applied_event_id: null,
  };
  scheduleBrewState.set(brewId, { ...prev, ...patch });
}

function seedScheduleBrewState(items = []) {
  scheduleBrewState.clear();
  (items || []).forEach((item) => {
    const brewId = String(item?.brew_id || "").trim();
    if (!brewId) return;
    updateScheduleBrewState(brewId, {
      brew_index: Number(item?.brew_index || 0) || null,
      optimization_status: item?.status === "optimized" || item?.status === "applied" ? "complete" : "pending",
      discharge_status: item?.status === "applied" ? "complete" : "pending",
      selected_candidate_index:
        item?.selected_candidate_index === null || item?.selected_candidate_index === undefined
          ? null
          : Number(item.selected_candidate_index),
      applied_event_id: item?.applied_event_id ?? null,
    });
  });
  renderScheduleBrewDetails();
}

function renderScheduleBrewDetails() {
  if (!scheduleBrewDetailsWrapEl) return;
  const rows = Array.from(scheduleBrewState.values()).sort(
    (a, b) => Number(a.brew_index || 0) - Number(b.brew_index || 0)
  );
  if (!rows.length) {
    scheduleBrewDetailsWrapEl.innerHTML = "<div class='run-status'>Generate a schedule to view brew-wise simulation, optimization, and discharge details.</div>";
    return;
  }
  const htmlRows = rows
    .map(
      (row) => `
      <tr>
        <td>${row.brew_id}</td>
        <td>${row.simulation_status}</td>
        <td>${row.simulation_note || "-"}</td>
        <td>${row.optimization_status}</td>
        <td>${formatMass(row.top_candidate_mass_kg)} (${Number(row.candidate_count || 0)} cand.)</td>
        <td>${row.discharge_status}</td>
        <td>${formatMass(row.discharged_mass_kg)}</td>
        <td>${
          row.selected_candidate_index === null || row.selected_candidate_index === undefined
            ? "-"
            : Number(row.selected_candidate_index) + 1
        }</td>
        <td>${row.applied_event_id ?? "-"}</td>
        <td>${formatTimeLabel(row.discharge_at || row.optimization_at || row.simulation_at)}</td>
      </tr>
    `
    )
    .join("");
  scheduleBrewDetailsWrapEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Brew</th>
          <th>Simulation</th>
          <th>Simulation Detail</th>
          <th>Optimization</th>
          <th>Top Candidate</th>
          <th>Discharge</th>
          <th>Discharged</th>
          <th>Candidate #</th>
          <th>Applied Event</th>
          <th>Last Updated</th>
        </tr>
      </thead>
      <tbody>${htmlRows}</tbody>
    </table>
  `;
}

async function refreshScheduleState(scheduleId) {
  const resolved = String(scheduleId || "").trim();
  if (!resolved) return;
  const r = await fetch(`/api/schedules/${encodeURIComponent(resolved)}`);
  const data = await r.json();
  if (!r.ok) throw new Error(data?.detail || "Failed to fetch schedule state.");
  const items = data?.items || [];
  items.forEach((item) => {
    const brewId = String(item?.brew_id || "").trim();
    if (!brewId) return;
    const status = String(item?.status || "pending");
    updateScheduleBrewState(brewId, {
      brew_index: Number(item?.brew_index || 0) || null,
      optimization_status: status === "optimized" || status === "applied" ? "complete" : "pending",
      discharge_status: status === "applied" ? "complete" : "pending",
      selected_candidate_index:
        item?.selected_candidate_index === null || item?.selected_candidate_index === undefined
          ? null
          : Number(item.selected_candidate_index),
      applied_event_id: item?.applied_event_id ?? null,
    });
  });
  renderScheduleBrewDetails();
}

function setActiveTab(tabName) {
  tabPanels.forEach((panel) => {
    const active = panel.getAttribute("data-tab-panel") === tabName;
    panel.classList.toggle("is-active", active);
    panel.hidden = !active;
  });
  if (tabStudioBtn) {
    const active = tabName === "studio";
    tabStudioBtn.classList.toggle("is-active", active);
    tabStudioBtn.setAttribute("aria-selected", String(active));
  }
  if (tabScheduleBtn) {
    const active = tabName === "schedule";
    tabScheduleBtn.classList.toggle("is-active", active);
    tabScheduleBtn.setAttribute("aria-selected", String(active));
  }
}

function printScheduleDebug(targetEl, data) {
  if (!targetEl) return;
  targetEl.textContent = typeof data === "string" ? data : JSON.stringify(data, null, 2);
}

// ── Schedule structured renders ────────────────────────────────────────────

function renderSchedRandomData(payload) {
  const silos = payload.silos || [];
  const suppliers = payload.suppliers || [];
  const queue = payload.incoming_queue || [];

  if (schedGenSilosWrapEl) {
    const rows = silos.map(s => `<tr><td>${s.silo_id}</td><td>${Number(s.capacity_kg||0).toFixed(0)}</td><td>${Number(s.body_diameter_m||0).toFixed(3)}</td><td>${Number(s.outlet_diameter_m||0).toFixed(3)}</td></tr>`).join("");
    schedGenSilosWrapEl.innerHTML = `<table><thead><tr><th>Silo</th><th>Capacity (kg)</th><th>Body Dia (m)</th><th>Outlet Dia (m)</th></tr></thead><tbody>${rows || "<tr><td colspan='4'>No silos</td></tr>"}</tbody></table>`;
  }
  if (schedGenSuppliersWrapEl) {
    const rows = suppliers.map(s => `<tr><td>${s.supplier}</td><td>${Number(s.moisture_pct||0).toFixed(2)}</td><td>${Number(s.fine_extract_db_pct||0).toFixed(2)}</td><td>${Number(s.diastatic_power_WK||0).toFixed(0)}</td><td>${Number(s.wort_colour_EBC||0).toFixed(2)}</td></tr>`).join("");
    schedGenSuppliersWrapEl.innerHTML = `<table><thead><tr><th>Supplier</th><th>Moisture%</th><th>Fine Extract%</th><th>Diast. Power</th><th>Colour EBC</th></tr></thead><tbody>${rows || "<tr><td colspan='5'>No suppliers</td></tr>"}</tbody></table>`;
  }
  // loaded lots from layers
  const loadedMap = new Map();
  (payload.layers || []).forEach(r => {
    const mass = Number(r.remaining_mass_kg ?? r.loaded_mass ?? r.segment_mass_kg ?? 0);
    if (mass <= 0) return;
    const key = `${r.lot_id}__${r.supplier}`;
    loadedMap.set(key, (loadedMap.get(key) || 0) + mass);
  });
  if (schedGenLoadedLotsWrapEl) {
    const loaded = Array.from(loadedMap.entries());
    if (!loaded.length) {
      schedGenLoadedLotsWrapEl.innerHTML = "<div style='color:var(--muted);font-size:12px;padding:6px 0'>No lots currently loaded in silos.</div>";
    } else {
      const rows = loaded.map(([k, mass]) => { const [lot, sup] = k.split("__"); return `<tr><td>${lot}</td><td>${sup}</td><td>${mass.toFixed(3)}</td></tr>`; }).join("");
      schedGenLoadedLotsWrapEl.innerHTML = `<table><thead><tr><th>Lot</th><th>Supplier</th><th>Total Loaded (kg)</th></tr></thead><tbody>${rows}</tbody></table>`;
    }
  }
  if (schedGenIncomingLotsWrapEl) {
    const rows = queue.slice(0, 20).map(item => `<tr><td>${item.lot_id||""}</td><td>${item.supplier||""}</td><td>${Number(item.mass_kg||0).toFixed(3)}</td></tr>`).join("");
    const more = queue.length > 20 ? `<tr><td colspan="3" style="color:var(--muted)">…and ${queue.length - 20} more lots</td></tr>` : "";
    schedGenIncomingLotsWrapEl.innerHTML = `<table><thead><tr><th>Lot</th><th>Supplier</th><th>Mass (kg)</th></tr></thead><tbody>${rows || "<tr><td colspan='3'>Empty queue</td></tr>"}${more}</tbody></table>`;
  }
  if (schedRandomStatusEl) {
    const total = queue.reduce((acc, i) => acc + Number(i.mass_kg||0), 0);
    schedRandomStatusEl.className = "run-status success top-gap";
    schedRandomStatusEl.textContent = `Generated: ${silos.length} silos · ${suppliers.length} suppliers · ${queue.length} lots (${total.toFixed(0)} kg queue).`;
  }
}

function renderSchedSchedulePlan(data) {
  const items = data.items || [];
  if (schedScheduleStatusEl) {
    schedScheduleStatusEl.className = "run-status success top-gap";
    schedScheduleStatusEl.textContent = `Schedule "${data.name || ""}" (ID: ${data.schedule_id || ""}) — ${items.length} brews planned.`;
  }
  if (schedGenBrewsWrapEl) {
    if (!items.length) {
      schedGenBrewsWrapEl.innerHTML = "<div style='color:var(--muted);font-size:12px;padding:6px 0'>No brews in schedule.</div>";
      return;
    }
    const rows = items.map(item => {
      const tp = item.target_params || {};
      const statusColor = item.status === "applied" ? "ok" : item.status === "optimized" ? "warn" : "";
      return `<tr>
        <td>${item.brew_index||""}</td>
        <td><strong>${item.brew_id||""}</strong></td>
        <td>${Number(tp.moisture_pct||0).toFixed(2)}</td>
        <td>${Number(tp.fine_extract_db_pct||0).toFixed(2)}</td>
        <td>${Number(tp.diastatic_power_WK||0).toFixed(0)}</td>
        <td>${Number(tp.wort_pH||0).toFixed(2)}</td>
        <td>${statusColor ? `<span class="badge ${statusColor}">${item.status}</span>` : item.status||"pending"}</td>
      </tr>`;
    }).join("");
    schedGenBrewsWrapEl.innerHTML = `<table><thead><tr><th>#</th><th>Brew ID</th><th>Moisture%</th><th>Fine Ext%</th><th>Diast.Power</th><th>Wort pH</th><th>Status</th></tr></thead><tbody>${rows}</tbody></table>`;
  }
}

function renderSchedSimResults(summary, statusMsg, statusClass) {
  if (schedRunStatusEl) {
    schedRunStatusEl.className = `run-status ${statusClass || "success"}`;
    schedRunStatusEl.textContent = statusMsg || "Simulation complete.";
  }
  const silos = summary?.silos || [];
  const totalCapacity = silos.reduce((a, s) => a + Number(s.capacity_kg || 0), 0);
  const totalUsed = silos.reduce((a, s) => a + Number(s.used_kg || 0), 0);
  const totalRemaining = silos.reduce((a, s) => a + Number(s.remaining_kg || 0), 0);
  const cumDischarged = Number(summary?.cumulative_discharged_kg || 0);
  const queueCount = Number(summary?.incoming_queue?.count || 0);
  const queueMass = Number(summary?.incoming_queue?.total_mass_kg || 0);

  if (schedSummaryCardsEl) {
    schedSummaryCardsEl.innerHTML = `
      <div class="card"><div class="card-key">Total Capacity (kg)</div><div class="card-value">${totalCapacity.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Current Inventory (kg)</div><div class="card-value">${totalUsed.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Total Remaining (kg)</div><div class="card-value">${totalRemaining.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Incoming Queue Count</div><div class="card-value">${queueCount}</div></div>
      <div class="card"><div class="card-key">Incoming Queue Mass (kg)</div><div class="card-value">${queueMass.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Cumulative Discharged (kg)</div><div class="card-value">${cumDischarged.toFixed(3)}</div></div>
    `;
  }
  if (schedRemainingFocusWrapEl) {
    schedRemainingFocusWrapEl.innerHTML = !silos.length ? "" : silos.map(s => {
      const pct = Math.max(0, Math.min(100, Number(s.remaining_pct || 0)));
      return `<article class="gauge-card"><div class="gauge-top"><span class="gauge-id">${s.silo_id}</span><span class="gauge-pct">${pct.toFixed(1)}%</span></div><div class="gauge-track"><div class="gauge-fill" style="width:${pct.toFixed(2)}%"></div></div><div class="gauge-meta">${Number(s.remaining_kg||0).toFixed(3)} kg remaining</div></article>`;
    }).join("");
  }
  if (schedSiloTableWrapEl) {
    const rows = silos.map(s => {
      const lotDisplay = (s.lots || []).filter(l => Number(l.remaining_mass_kg||0) > 0.5).map(l => `${Number(l.current_layer_index ?? l.layer_index ?? 0)}:${String(l.lot_id||"")}(${Number(l.remaining_mass_kg||0).toFixed(0)})`).join(" | ");
      return `<tr><td>${s.silo_id}</td><td>${Number(s.capacity_kg||0).toFixed(3)}</td><td>${Number(s.used_kg||0).toFixed(3)}</td><td>${Number(s.remaining_kg||0).toFixed(3)}</td><td>${Number(s.remaining_pct||0).toFixed(2)}</td><td>${lotDisplay}</td></tr>`;
    }).join("");
    schedSiloTableWrapEl.innerHTML = `<table><thead><tr><th>Silo</th><th>Capacity (kg)</th><th>Used (kg)</th><th>Remaining (kg)</th><th>Remaining (%)</th><th>Lots (bottom→top)</th></tr></thead><tbody>${rows || "<tr><td colspan='6'>No silo data</td></tr>"}</tbody></table>`;
  }
  if (schedStepRunEl && schedSimStatusEl) {
    schedStepRunEl.classList.remove("is-success", "is-active", "is-warning");
    schedStepRunEl.classList.add("is-success");
    schedSimStatusEl.textContent = "Complete";
  }
}

function renderSchedDischargeContribution(predictedRun) {
  if (!schedContributionWrapEl) return;
  const perSilo = predictedRun?.per_silo || {};
  const entries = Object.entries(perSilo);
  const total = entries.reduce((acc, [, v]) => acc + Number(v.discharged_mass_kg || 0), 0);
  if (!entries.length || total <= 0) { schedContributionWrapEl.innerHTML = ""; return; }
  schedContributionWrapEl.innerHTML = entries.map(([id, v]) => {
    const mass = Number(v.discharged_mass_kg || 0);
    const pct = (mass / total) * 100;
    return `<div class="bar-row"><div class="bar-label">${id}</div><div class="bar-track"><div class="bar-fill" style="width:${pct.toFixed(2)}%"></div></div><div class="bar-value">${pct.toFixed(1)}%</div></div>`;
  }).join("");
}

function syncScheduleResultAnalysisFromStudio() {
  // no-op: schedule now renders its own results directly
}

function populateScheduleBrewSelect(items = []) {
  if (!schedOptBrewIdEl) return;
  const options = (items || [])
    .map((item) => String(item?.brew_id || "").trim())
    .filter(Boolean)
    .map((brewId) => `<option value="${brewId}">${brewId}</option>`)
    .join("");
  schedOptBrewIdEl.innerHTML = options || '<option value="">No brews available</option>';
}

function printRaw(data) {
  rawOutEl.textContent = typeof data === "string" ? data : JSON.stringify(data, null, 2);
}

function setStepState(stepEl, statusEl, stateClass, label) {
  stepEl.classList.remove("is-success", "is-active", "is-warning");
  if (stateClass) stepEl.classList.add(stateClass);
  statusEl.textContent = label;
}

function classifyIssue(message) {
  const lower = String(message || "").toLowerCase();
  let severity = "error";
  if (lower.includes("coa warning")) {
    severity = "warn";
  } else if (lower.includes("coa error")) {
    severity = "error";
  } else if (lower.includes("duplicate") || lower.includes("unknown") || lower.includes("missing")) {
    severity = "error";
  } else if (lower.includes("must have") || lower.includes("between 0 and 1")) {
    severity = "error";
  } else if (lower.includes("expected 3 silos")) {
    severity = "warn";
  }

  let file = "input";
  if (lower.includes("silos.csv")) file = "silos.csv";
  if (lower.includes("layers.csv")) file = "layers.csv";
  if (lower.includes("suppliers.csv")) file = "suppliers.csv";
  if (lower.includes("discharge.csv")) file = "discharge.csv";

  let field = "general";
  if (lower.includes("silo_id")) field = "silo_id";
  if (lower.includes("layer_index")) field = "layer_index";
  if (lower.includes("segment_mass_kg")) field = "segment_mass_kg";
  if (lower.includes("discharge_fraction")) field = "discharge_fraction";
  if (lower.includes("discharge_mass_kg")) field = "discharge_mass_kg";
  if (lower.includes("capacity_kg")) field = "capacity_kg";
  // COA parameter fields
  if (lower.includes("moisture")) field = "moisture_pct";
  if (lower.includes("fine extract")) field = "fine_extract_db_pct";
  if (lower.includes("wort ph")) field = "wort_pH";
  if (lower.includes("diastatic power")) field = "diastatic_power_WK";
  if (lower.includes("total protein")) field = "total_protein_pct";
  if (lower.includes("wort colour")) field = "wort_colour_EBC";

  let hint = "Review this field value and rerun validation.";
  if (lower.includes("missing")) hint = "Add the required column in the referenced CSV file.";
  if (lower.includes("duplicate")) hint = "Remove duplicates and keep unique key rows only.";
  if (lower.includes("between 0 and 1")) hint = "Set value within range 0.0 to 1.0.";
  if (lower.includes("must have")) hint = "Ensure all rows satisfy the numeric constraint.";
  if (lower.includes("coa error")) hint = "Check units or data entry — value is outside physical science bounds (EBC/ASBC).";
  if (lower.includes("coa warning")) hint = "Value is unusual but physically possible. Verify against the supplier COA document before proceeding.";

  return { severity, file, field, message: String(message), hint };
}

function renderValidationIssues(errors = []) {
  const issues = errors.map(classifyIssue);
  if (!issues.length) {
    validationOutEl.innerHTML = "";
    return { blockingCount: 0, warningCount: 0 };
  }
  validationOutEl.innerHTML = issues
    .map(
      (i) => `
      <article class="issue-card">
        <div class="issue-head">
          <span class="badge ${i.severity}">${i.severity}</span>
          <span class="issue-target">${i.file} / ${i.field}</span>
        </div>
        <div class="issue-message">${i.message}</div>
        <div class="issue-hint">Action: ${i.hint}</div>
      </article>
    `
    )
    .join("");
  const blockingCount = issues.filter((i) => i.severity === "error").length;
  const warningCount = issues.filter((i) => i.severity === "warn").length;
  return { blockingCount, warningCount };
}

function setValidationState(valid, blockingCount = 0, warningCount = 0) {
  validationSummaryEl.classList.remove("ready", "blocked", "warn");
  if (valid) {
    kpiValidationEl.textContent = warningCount > 0 ? `Ready (${warningCount} Warnings)` : "Ready";
    validationSummaryEl.textContent =
      warningCount > 0
        ? `Validation passed with ${warningCount} warning(s). Simulation can run.`
        : "Validation passed. Inputs are ready to run.";
    validationSummaryEl.classList.add(warningCount > 0 ? "warn" : "ready");
    setStepState(stepInput, statusInput, warningCount > 0 ? "is-warning" : "is-success", "Ready");
    runBtn.disabled = false;
  } else {
    kpiValidationEl.textContent = blockingCount > 0 ? `${blockingCount} Blocking` : "Not Ready";
    validationSummaryEl.textContent =
      blockingCount > 0
        ? `${blockingCount} blocking issue(s) must be fixed before simulation.`
        : "Validation not executed.";
    validationSummaryEl.classList.add(blockingCount > 0 ? "blocked" : "warn");
    setStepState(stepInput, statusInput, "is-warning", "Needs Attention");
    runBtn.disabled = true;
  }
}

function updateRunKpis(result) {
  kpiDischargedEl.textContent = Number(result.total_discharged_mass_kg || 0).toFixed(3);
  kpiRemainingEl.textContent = Number(result.total_remaining_mass_kg || 0).toFixed(3);
}

function renderSummary(result) {
  const blend = result.total_blended_params || {};
  const total = Number(result.total_discharged_mass_kg || 0).toFixed(3);
  const remaining = Number(result.total_remaining_mass_kg || 0).toFixed(3);

  const cards = [
    { key: "Total Discharged (kg)", value: total },
    { key: "Total Remaining (kg)", value: remaining },
    ...Object.entries(blend).map(([k, v]) => ({ key: k, value: Number(v).toFixed(4) })),
  ];
  summaryCardsEl.innerHTML = cards
    .map(
      (c) =>
        `<div class="card"><div class="card-key">${c.key}</div><div class="card-value">${c.value}</div></div>`
    )
    .join("");
}

function renderSiloTable(result) {
  const perSilo = result.per_silo || {};
  const rows = Object.entries(perSilo)
    .map(
      ([id, v]) => `
    <tr>
      <td>${id}</td>
      <td>${Number(v.discharged_mass_kg).toFixed(3)}</td>
      <td>${Number(v.mass_flow_rate_kg_s).toFixed(3)}</td>
      <td>${Number(v.discharge_time_s).toFixed(3)}</td>
      <td>${Number(v.sigma_m).toFixed(4)}</td>
    </tr>
  `
    )
    .join("");

  siloTableWrapEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Silo</th>
          <th>Discharged (kg)</th>
          <th>Flow (kg/s)</th>
          <th>Time (s)</th>
          <th>Sigma (m)</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderSiloFillTable(summary) {
  const rows = (summary?.silos || [])
    .map(
      (s) => {
        const lotDisplay = (s.lots || [])
          .filter((l) => Number(l.remaining_mass_kg || 0) > 0.5)
          .map(
            (l) =>
              `${Number(l.current_layer_index ?? l.layer_index ?? 0)}:${String(l.lot_id || "")}(${Number(
                l.remaining_mass_kg || 0
              ).toFixed(0)})`
          )
          .join(" | ");
        return `
    <tr>
      <td>${s.silo_id}</td>
      <td>${Number(s.capacity_kg || 0).toFixed(3)}</td>
      <td>${Number(s.used_kg || 0).toFixed(3)}</td>
      <td>${Number(s.remaining_kg || 0).toFixed(3)}</td>
      <td>${Number(s.remaining_pct || 0).toFixed(2)}</td>
      <td>${lotDisplay}</td>
    </tr>
  `;
      }
    )
    .join("");

  siloTableWrapEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Silo</th>
          <th>Capacity (kg)</th>
          <th>Used (kg)</th>
          <th>Remaining (kg)</th>
          <th>Remaining (%)</th>
          <th>Lots (bottom-&gt;top)</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderUpcomingLots(payload) {
  const layers = payload.layers || [];
  const loadedMap = new Map();
  layers.forEach((r) => {
    const mass = Number(
      r.remaining_mass_kg ?? r.loaded_mass ?? r.segment_mass_kg ?? 0
    );
    if (mass <= 0) return;
    const key = `${r.lot_id}__${r.supplier}`;
    const prev = loadedMap.get(key) || 0;
    loadedMap.set(key, prev + mass);
  });
  const loadedRows = Array.from(loadedMap.entries())
    .filter(([, mass]) => Number(mass) > 0)
    .map(([k, mass]) => {
      const [lotId, supplier] = k.split("__");
      return `<tr><td>${lotId}</td><td>${supplier}</td><td>${mass.toFixed(3)}</td></tr>`;
    })
    .join("");
  upcomingLotsWrapEl.innerHTML = `
    <table>
      <thead><tr><th>Lot</th><th>Supplier</th><th>Total Loaded (kg)</th></tr></thead>
      <tbody>${loadedRows}</tbody>
    </table>
  `;

  const incoming = payload.incoming_queue || [];
  const incomingRows = incoming
    .map((item) => {
      if (typeof item === "string") {
        return `<tr><td>${item}</td><td>-</td><td>-</td></tr>`;
      }
      const lotId = String(item?.lot_id ?? item?.lot ?? "");
      const supplier = String(item?.supplier ?? "-");
      const mass = Number(item?.mass_kg ?? item?.remaining_mass_kg ?? 0);
      return `<tr><td>${lotId}</td><td>${supplier}</td><td>${mass.toFixed(3)}</td></tr>`;
    })
    .join("");
  incomingLotsWrapEl.innerHTML = `
    <table>
      <thead><tr><th>Lot</th><th>Supplier</th><th>Mass (kg)</th></tr></thead>
      <tbody>${incomingRows || "<tr><td colspan='3'>No incoming lots</td></tr>"}</tbody>
    </table>
  `;
}

function renderStateLedger(result) {
  const rows = (result.silo_state_ledger || [])
    .map(
      (r) => `<tr>
        <td>${r.silo_id}</td>
        <td>${Number(r.initial_mass_kg).toFixed(3)}</td>
        <td>${Number(r.discharged_mass_kg).toFixed(3)}</td>
        <td>${Number(r.remaining_mass_kg).toFixed(3)}</td>
        <td>${Number(r.remaining_pct).toFixed(2)}</td>
      </tr>`
    )
    .join("");
  stateLedgerWrapEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Silo</th><th>Initial (kg)</th><th>Discharged (kg)</th><th>Remaining (kg)</th><th>Remaining (%)</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderRemainingFocus(result) {
  const rows = result.silo_state_ledger || [];
  if (!rows.length) {
    remainingFocusWrapEl.innerHTML = "";
    return;
  }
  remainingFocusWrapEl.innerHTML = rows
    .map((r) => {
      const pct = Number(r.remaining_pct || 0);
      const pctSafe = Math.max(0, Math.min(100, pct));
      return `
      <article class="gauge-card">
        <div class="gauge-top">
          <span class="gauge-id">${r.silo_id}</span>
          <span class="gauge-pct">${pctSafe.toFixed(1)}%</span>
        </div>
        <div class="gauge-track"><div class="gauge-fill" style="width:${pctSafe.toFixed(2)}%"></div></div>
        <div class="gauge-meta">${Number(r.remaining_mass_kg || 0).toFixed(3)} kg remaining</div>
      </article>
    `;
    })
    .join("");
}

function renderContributionBars(result) {
  const perSilo = result.per_silo || {};
  const entries = Object.entries(perSilo);
  const total = entries.reduce((acc, [, v]) => acc + Number(v.discharged_mass_kg || 0), 0);
  if (!entries.length || total <= 0) {
    contributionWrapEl.innerHTML = "";
    return;
  }
  contributionWrapEl.innerHTML = entries
    .map(([id, v]) => {
      const mass = Number(v.discharged_mass_kg || 0);
      const pct = (mass / total) * 100;
      return `
      <div class="bar-row">
        <div class="bar-label">${id}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${pct.toFixed(2)}%"></div></div>
        <div class="bar-value">${pct.toFixed(1)}%</div>
      </div>
    `;
    })
    .join("");
}

function renderCoaWarnings(warnings, anchorEl) {
  // Render a COA warning panel before anchorEl (run or optimize output area).
  const bannerId = "coaWarningBanner_" + (anchorEl ? anchorEl.id : "global");
  let bannerEl = document.getElementById(bannerId);
  if (!bannerEl) {
    bannerEl = document.createElement("div");
    bannerEl.id = bannerId;
    bannerEl.style.cssText = "margin:8px 0;padding:10px 14px;border-radius:6px;font-size:13px;line-height:1.55;display:none;";
    if (anchorEl && anchorEl.parentNode) {
      anchorEl.parentNode.insertBefore(bannerEl, anchorEl);
    }
  }
  if (!warnings || warnings.length === 0) {
    bannerEl.style.display = "none";
    bannerEl.innerHTML = "";
    return;
  }
  const rows = warnings.map((w) => {
    // Extract supplier and param from message for colour-coded display.
    const supplierMatch = w.match(/supplier '([^']+)'/);
    const supplier = supplierMatch ? supplierMatch[1] : "unknown";
    const paramMatch = w.match(/— ([^v]+) value ([\d.]+) is outside typical contract range \[([\d.]+)–([\d.]+)\]/);
    if (paramMatch) {
      return `<li><strong>${supplier}</strong> — <span style="color:#92400e">${paramMatch[1].trim()}</span>: ` +
        `value <strong>${paramMatch[2]}</strong> outside typical range [${paramMatch[3]}–${paramMatch[4]}]</li>`;
    }
    return `<li>${w}</li>`;
  }).join("");
  bannerEl.style.cssText =
    "margin:8px 0;padding:10px 14px;border-radius:6px;font-size:13px;line-height:1.55;" +
    "background:#fffbeb;border:1.5px solid #f59e0b;color:#78350f;display:block;";
  bannerEl.innerHTML =
    `<strong>⚠ Supplier COA Warnings (${warnings.length})</strong>` +
    `<p style="margin:4px 0 6px">These suppliers have parameter values outside the typical contract range. ` +
    `Verify against original COA documents before acting on results.</p>` +
    `<ul style="margin:0;padding-left:18px">${rows}</ul>`;
}

function renderFeasibilityWarnings(warnings) {
  // Find or create the warning banner container just above the opt output.
  let bannerEl = document.getElementById("feasibilityWarningBanner");
  if (!bannerEl) {
    bannerEl = document.createElement("div");
    bannerEl.id = "feasibilityWarningBanner";
    bannerEl.style.cssText =
      "margin:8px 0;padding:10px 14px;border-radius:6px;font-size:13px;line-height:1.55;display:none;";
    if (optOutEl && optOutEl.parentNode) {
      optOutEl.parentNode.insertBefore(bannerEl, optOutEl);
    }
  }

  if (!warnings || warnings.length === 0) {
    bannerEl.style.display = "none";
    bannerEl.innerHTML = "";
    return;
  }

  const rows = warnings
    .map(
      (w) =>
        `<li>⚠ <strong>${w.param}</strong>: target <strong>${w.target}</strong> is ${w.direction} achievable range ` +
        `[<strong>${w.achievable_min}</strong> – <strong>${w.achievable_max}</strong>]</li>`
    )
    .join("");

  bannerEl.style.cssText =
    "margin:8px 0;padding:10px 14px;border-radius:6px;font-size:13px;line-height:1.55;" +
    "background:#fef3c7;border:1.5px solid #f59e0b;color:#92400e;display:block;";
  bannerEl.innerHTML =
    `<strong>⚠ Inventory Feasibility Warnings (${warnings.length})</strong>` +
    `<p style="margin:4px 0 6px">The following target parameters cannot be reached with the current silo inventory. ` +
    `The optimizer will return the closest achievable blend.</p>` +
    `<ul style="margin:0;padding-left:18px">${rows}</ul>`;
}

function renderCandidateTable(payload, options = {}) {
  const targetWrapEl = options.targetWrapEl || candidateTableWrapEl;
  const sortSelectEl = options.sortSelectEl || candidateSortEl;
  if (!targetWrapEl) return;
  const candidates = (payload?.top_candidates || []).slice();
  const sortKey = sortSelectEl?.value || "discharged";
  const mode = options.mode || "studio";
  const scheduleId = String(options.scheduleId || "").trim();
  const brewId = String(options.brewId || "").trim();
  // Keep Standard scenario at the end (Scenario 5), sort only optimized entries.
  const indexed = candidates.map((c, originalIndex) => ({ c, originalIndex }));
  const optimized = indexed.filter((x) => x.c?.scenario_type !== "standard_equal_split");
  const standard = indexed.find((x) => x.c?.scenario_type === "standard_equal_split");
  if (sortKey === "discharged") {
    optimized.sort(
      (a, b) => Number(b.c?.total_discharged_mass_kg || 0) - Number(a.c?.total_discharged_mass_kg || 0)
    );
  }
  const orderedCandidates = standard ? [...optimized, standard] : optimized;

  if (!orderedCandidates.length) {
    targetWrapEl.innerHTML = "";
    return;
  }
  const rows = orderedCandidates
    .map(
      ({ c, originalIndex }, idx) => `
    <tr>
      <td>${idx + 1}</td>
      <td>${c?.scenario_type === "standard_equal_split" ? "Standard (Equal Split)" : "Optimized"}</td>
      <td>${Number(c.total_discharged_mass_kg || 0).toFixed(3)}</td>
      <td>${
        c?.feasible === false
          ? `<span style="color:#b91c1c;">Not feasible: ${String(c?.reason || "insufficient mass")}</span>`
          : (c.recommended_discharge || [])
              .map((r) => `${r.silo_id}:${Number(r.discharge_mass_kg || 0).toFixed(3)} kg`)
              .join(" | ")
      }</td>
      <td>${Object.entries(c.blended_params || {})
        .map(([k, v]) => `${k}:${formatEstimatedParam(k, v)}`)
        .join(" | ")}</td>
      <td>${
        c?.feasible === false
          ? '<span style="color:var(--muted);">N/A</span>'
          : `<button class="btn btn-alt candidate-discharge-btn" data-candidate-index="${originalIndex}" data-context-mode="${mode}" data-schedule-id="${scheduleId}" data-brew-id="${brewId}">${
              mode === "schedule" ? "Discharge Brew" : "Discharge"
            }</button>`
      }</td>
    </tr>
  `
    )
    .join("");
  targetWrapEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Scenario</th>
          <th>Type</th>
          <th>Discharged (kg)</th>
          <th>Recommended Discharge (kg)</th>
          <th>Blended Params</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

async function loadSample() {
  setStepState(stepInput, statusInput, "is-active", "Loading");
  const r = await fetch("/api/sample");
  const payload = await r.json();
  payloadEl.value = JSON.stringify(payload, null, 2);
  renderUpcomingLots(payload);
  candidateTableWrapEl.innerHTML = "";
  lastRunResult = null;
  lastOptimizePayload = null;
  validationOutEl.innerHTML = "";
  validationSummaryEl.textContent = "Sample loaded. Run validation to check readiness.";
  setValidationState(false, 0, 0);
  setStepState(stepInput, statusInput, "is-active", "Loaded");
  printRaw("Sample loaded.");
}

function scheduleInitialSampleLoad() {
  const startLoad = () => {
    loadSample().catch((e) => {
      printRaw(`Sample load error: ${String(e)}`);
    });
  };
  if (typeof window !== "undefined" && typeof window.requestIdleCallback === "function") {
    window.requestIdleCallback(startLoad, { timeout: 500 });
    return;
  }
  setTimeout(startLoad, 0);
}

function parsePayload() {
  try {
    return JSON.parse(payloadEl.value);
  } catch (e) {
    throw new Error(`Invalid JSON: ${e.message}`);
  }
}

function safePayloadOrEmpty() {
  try {
    return parsePayload();
  } catch (_) {
    return {};
  }
}

async function validatePayload() {
  if (isValidating) return;
  try {
    isValidating = true;
    document.getElementById("validateBtn").setAttribute("aria-busy", "true");
    setStepState(stepInput, statusInput, "is-active", "Validating");
    const payload = parsePayload();
    const r = await fetch("/api/validate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    const allIssues = [...(data.errors || []), ...(data.coa_warnings || [])];
    if (data.valid) {
      const counts = renderValidationIssues(allIssues);
      setValidationState(true, counts.blockingCount, counts.warningCount);
    } else {
      const counts = renderValidationIssues(allIssues);
      setValidationState(false, counts.blockingCount, counts.warningCount);
    }
    printRaw(data);
  } catch (e) {
    renderValidationIssues([String(e)]);
    setValidationState(false, 1, 0);
    printRaw(String(e));
  } finally {
    isValidating = false;
    document.getElementById("validateBtn").removeAttribute("aria-busy");
  }
}

async function runSimulation() {
  if (isRunning) return;
  if (!validatePayloadContents()) return;
  try {
    isRunning = true;
    runBtn.disabled = true;
    runBtn.textContent = "Running...";
    runBtn.setAttribute("aria-busy", "true");
    stepResults.setAttribute("aria-busy", "true");
    setStepState(stepRun, statusRun, "is-active", "Running");
    runStatusEl.className = "run-status running";
    runStatusEl.textContent = "Filling silos from incoming queue...";
    const payload = parsePayload();
    const r = await fetch("/api/process/run_simulation", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        silos: payload.silos || [],
        layers: payload.layers || [],
        suppliers: payload.suppliers || [],
        incoming_queue: payload.incoming_queue || [],
      }),
    });
    const data = await r.json();
    if (!r.ok) {
      setStepState(stepRun, statusRun, "is-warning", "Run Failed");
      runStatusEl.className = "run-status error";
      runStatusEl.textContent = "Simulation failed. See response details for diagnostics.";
      printRaw(data);
      return { ok: false, data };
    }
    const summary = data.summary || {};
    const state = data.state || {};
    const totalRemaining = Number((summary.silos || []).reduce((a, s) => a + Number(s.remaining_kg || 0), 0));
    const totalCapacity = Number((summary.silos || []).reduce((a, s) => a + Number(s.capacity_kg || 0), 0));
    const totalUsed = Number((summary.silos || []).reduce((a, s) => a + Number(s.used_kg || 0), 0));

    kpiDischargedEl.textContent = Number(0).toFixed(3);
    kpiRemainingEl.textContent = totalRemaining.toFixed(3);
    renderSiloFillTable(summary);
    renderUpcomingLots({
      layers: state.layers || [],
      incoming_queue: state.incoming_queue || [],
    });
    summaryCardsEl.innerHTML = `
      <div class="card"><div class="card-key">Total Capacity (kg)</div><div class="card-value">${totalCapacity.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Current Inventory (kg)</div><div class="card-value">${totalUsed.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Total Remaining (kg)</div><div class="card-value">${totalRemaining.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Incoming Queue Count</div><div class="card-value">${Number(summary.incoming_queue?.count || 0)}</div></div>
      <div class="card"><div class="card-key">Incoming Queue Mass (kg)</div><div class="card-value">${Number(summary.incoming_queue?.total_mass_kg || 0).toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Cumulative Discharged (kg)</div><div class="card-value">${Number(summary.cumulative_discharged_kg || 0).toFixed(3)}</div></div>
    `;
    payloadEl.value = JSON.stringify(
      {
        ...payload,
        silos: state.silos || payload.silos || [],
        layers: state.layers || payload.layers || [],
        suppliers: state.suppliers || payload.suppliers || [],
        incoming_queue: state.incoming_queue || payload.incoming_queue || [],
      },
      null,
      2
    );
    remainingFocusWrapEl.innerHTML = "";
    contributionWrapEl.innerHTML = "";
    stateLedgerWrapEl.innerHTML = "";
    lastRunResult = null;
    setStepState(stepRun, statusRun, "is-success", "Complete");
    setStepState(stepResults, statusResults, "is-success", "Ready");
    runStatusEl.className = "run-status success";
    runStatusEl.textContent = "Fill complete. No discharge performed.";
    renderCoaWarnings(data.coa_warnings || [], rawOutEl);
    printRaw(data);
    return { ok: true, data };
  } catch (e) {
    setStepState(stepRun, statusRun, "is-warning", "Run Failed");
    runStatusEl.className = "run-status error";
    runStatusEl.textContent = "Simulation failed before completion.";
    printRaw(String(e));
    return { ok: false, error: String(e) };
  } finally {
    isRunning = false;
    runBtn.removeAttribute("aria-busy");
    stepResults.removeAttribute("aria-busy");
    runBtn.textContent = "Run Simulation";
    runBtn.disabled = !kpiValidationEl.textContent.startsWith("Ready");
  }
}

function targetParamsFromUI() {
  return {
    moisture_pct: Number(document.getElementById("t_moisture_pct").value),
    fine_extract_db_pct: Number(document.getElementById("t_fine_extract_db_pct").value),
    wort_pH: Number(document.getElementById("t_wort_pH").value),
    diastatic_power_WK: Number(document.getElementById("t_diastatic_power_WK").value),
    total_protein_pct: Number(document.getElementById("t_total_protein_pct").value),
    wort_colour_EBC: Number(document.getElementById("t_wort_colour_EBC").value),
  };
}

async function optimizeBlend() {
  if (isOptimizing) return;
  if (!validateGroup(OPT_FIELDS)) return;
  try {
    if (!optOutEl) {
      throw new Error("UI element #optOut not found.");
    }
    setStepState(stepOptimize, statusOptimize, "is-active", "Optimizing");
    optOutEl.textContent = "Running optimization...";
    if (optimizeBtn) {
      isOptimizing = true;
      optimizeBtn.disabled = true;
      optimizeBtn.textContent = "Optimizing...";
      optimizeBtn.setAttribute("aria-busy", "true");
    }

    const payloadSafe = safePayloadOrEmpty();
    const r = await fetch("/api/process/optimize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target_params: targetParamsFromUI(),
        iterations: Number(document.getElementById("opt_iterations").value || 120),
        seed: Number(optSeedEl?.value || 42),
        config: payloadSafe.config || {},
      }),
    });
    const data = await r.json();
    if (!r.ok) {
      optOutEl.textContent = JSON.stringify(data, null, 2);
      setStepState(stepOptimize, statusOptimize, "is-warning", "Failed");
      if (optimizeBtn) optimizeBtn.disabled = false;
      return;
    }
    // Optimization is advisory only; do not mutate current fill-state visuals.
    lastOptimizeContext = { mode: "studio", scheduleId: "", brewId: "" };
    renderCoaWarnings(data.coa_warnings || [], optOutEl);
    renderFeasibilityWarnings(data.feasibility_warnings || []);
    renderCandidateTable(data, lastOptimizeContext);
    lastOptimizePayload = data;

    const top = data.top_candidates || [];
    const lines = [];
    lines.push(`Method: ${data.objective_method}`);
    lines.push("Top candidates:");
    top.forEach((c, i) => {
      const label = c?.scenario_type === "standard_equal_split" ? "standard" : "optimized";
      const feasible = c?.feasible === false ? " (infeasible)" : "";
      lines.push(
        `${i + 1}. [${label}] discharged=${Number(c.total_discharged_mass_kg || 0).toFixed(3)}kg${feasible}`
      );
    });
    lines.push("");
    lines.push(JSON.stringify(data, null, 2));
    optOutEl.textContent = lines.join("\n");
    setStepState(stepOptimize, statusOptimize, "is-success", "Complete");
    if (optimizeBtn) optimizeBtn.disabled = false;
  } catch (e) {
    renderCoaWarnings([], optOutEl);
    renderFeasibilityWarnings([]);
    if (optOutEl) optOutEl.textContent = String(e);
    setStepState(stepOptimize, statusOptimize, "is-warning", "Failed");
    printRaw(`Optimization error: ${String(e)}`);
    if (optimizeBtn) optimizeBtn.disabled = false;
  } finally {
    isOptimizing = false;
    if (optimizeBtn) {
      optimizeBtn.removeAttribute("aria-busy");
      optimizeBtn.textContent = "Optimize Blend";
    }
  }
}

async function generateRandomScheduleData() {
  if (!schedGenerateRandomBtn) return;
  if (!validateGroup(SCHED_RANDOM_FIELDS)) return;
  try {
    schedGenerateRandomBtn.disabled = true;
    schedGenerateRandomBtn.textContent = "Generating...";
    const seed = Number(document.getElementById("sched_seed")?.value || 42);
    const silosCount = Number(document.getElementById("sched_silos_count")?.value || 3);
    const lotsCount = Number(document.getElementById("sched_lots_count")?.value || 100);
    const lotSizeKg = Number(document.getElementById("sched_lot_size_kg")?.value || 2000);

    const r = await fetch("/api/data/generate-random", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        seed,
        silos_count: silosCount,
        lots_count: lotsCount,
        lot_size_kg: lotSizeKg,
      }),
    });
    const data = await r.json();
    if (!r.ok) {
      if (schedRandomStatusEl) { schedRandomStatusEl.className = "run-status error top-gap"; schedRandomStatusEl.textContent = `Error: ${data?.detail || "Generate random failed."}`; }
      return;
    }
    const payload = data.payload || {};
    payloadEl.value = JSON.stringify(payload, null, 2);
    renderUpcomingLots(payload);
    renderSchedRandomData(payload);
    candidateTableWrapEl.innerHTML = "";
    if (schedCandidateTableWrapEl) schedCandidateTableWrapEl.innerHTML = "";
    lastRunResult = null;
    lastOptimizePayload = null;
    validationOutEl.innerHTML = "";
    validationSummaryEl.textContent = "Random dataset generated. Run validation to check readiness.";
    setValidationState(false, 0, 0);
    setStepState(stepInput, statusInput, "is-active", "Loaded");
  } catch (e) {
    if (schedRandomStatusEl) { schedRandomStatusEl.className = "run-status error top-gap"; schedRandomStatusEl.textContent = `Generate random failed: ${String(e)}`; }
  } finally {
    schedGenerateRandomBtn.disabled = false;
    schedGenerateRandomBtn.textContent = "Generate Random";
  }
}

async function generateSchedulePlan() {
  if (!schedGenerateScheduleBtn) return;
  if (!validateGroup(SCHED_PLAN_FIELDS)) return;
  try {
    schedGenerateScheduleBtn.disabled = true;
    schedGenerateScheduleBtn.textContent = "Generating...";
    const scheduleId = String(document.getElementById("sched_schedule_id")?.value || "").trim();
    const name = String(document.getElementById("sched_name")?.value || "MVP Brew Schedule").trim();
    const brewsCount = Number(document.getElementById("sched_brews_count")?.value || 5);
    const seed = Number(document.getElementById("sched_schedule_seed")?.value || 42);

    const r = await fetch("/api/schedules/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        schedule_id: scheduleId || null,
        name,
        brews_count: brewsCount,
        seed,
        target_params: targetParamsFromUI(),
      }),
    });
    const data = await r.json();
    if (!r.ok) {
      if (schedScheduleStatusEl) { schedScheduleStatusEl.className = "run-status error top-gap"; schedScheduleStatusEl.textContent = `Error: ${data?.detail || "Generate schedule failed."}`; }
      return;
    }
    const resolvedScheduleId = String(data.schedule_id || "");
    currentScheduleId = resolvedScheduleId;
    const schedOptScheduleIdEl = document.getElementById("sched_opt_schedule_id");
    if (schedOptScheduleIdEl) schedOptScheduleIdEl.value = resolvedScheduleId;
    populateScheduleBrewSelect(data.items || []);
    seedScheduleBrewState(data.items || []);
    renderSchedSchedulePlan(data);
  } catch (e) {
    if (schedScheduleStatusEl) { schedScheduleStatusEl.className = "run-status error top-gap"; schedScheduleStatusEl.textContent = `Generate schedule failed: ${String(e)}`; }
    printScheduleDebug(schedOptimizationOutEl, `Generate schedule failed: ${String(e)}`);
  } finally {
    schedGenerateScheduleBtn.disabled = false;
    schedGenerateScheduleBtn.textContent = "Generate Schedule";
  }
}

async function runSimulationFromScheduleTab() {
  if (!schedRunSimulationBtn) return;
  try {
    schedRunSimulationBtn.disabled = true;
    schedRunSimulationBtn.textContent = "Running...";
    if (schedRunStatusEl) { schedRunStatusEl.className = "run-status running"; schedRunStatusEl.textContent = "Filling silos from incoming queue..."; }
    const result = await runSimulation();
    if (result?.ok) {
      const summary = result?.data?.summary || {};
      const state = result?.data?.state || {};
      const totalUsed = (summary.silos || []).reduce((acc, silo) => acc + Number(silo.used_kg || 0), 0);
      const totalRemaining = (summary.silos || []).reduce((acc, silo) => acc + Number(silo.remaining_kg || 0), 0);
      scheduleSimulationSnapshot = { at: nowIso(), silos: Number(summary.silos?.length || 0), used_kg: totalUsed, remaining_kg: totalRemaining };
      Array.from(scheduleBrewState.values()).forEach((row) => {
        updateScheduleBrewState(row.brew_id, {
          simulation_status: "complete",
          simulation_at: scheduleSimulationSnapshot.at,
          simulation_note: `inventory=${totalUsed.toFixed(1)}kg, remaining=${totalRemaining.toFixed(1)}kg`,
        });
      });
      renderScheduleBrewDetails();
      renderSchedSimResults(summary, "Fill complete. Silos loaded — ready to optimize.", "success");
    } else {
      const errMsg = String(result?.data?.detail || result?.error || "Simulation failed.");
      renderSchedSimResults({}, errMsg, "error");
    }
  } catch (e) {
    if (schedRunStatusEl) { schedRunStatusEl.className = "run-status error"; schedRunStatusEl.textContent = `Simulation failed: ${String(e)}`; }
    renderSchedSimResults({}, `Simulation failed: ${String(e)}`, "error");
  } finally {
    schedRunSimulationBtn.disabled = false;
    schedRunSimulationBtn.textContent = "Run Simulation";
  }
}

async function optimizeScheduleItemFromTab() {
  if (!schedOptimizeBtn) return;
  try {
    schedOptimizeBtn.disabled = true;
    schedOptimizeBtn.textContent = "Optimizing...";
    const scheduleId = String(document.getElementById("sched_opt_schedule_id")?.value || "").trim();
    const brewId = String(schedOptBrewIdEl?.value || "").trim();
    const iterations = Number(document.getElementById("sched_opt_iterations")?.value || 120);
    const seed = Number(document.getElementById("sched_opt_seed")?.value || 42);
    if (!scheduleId || !brewId) {
      throw new Error("schedule_id and brew_id are required for optimize schedule.");
    }
    const payloadSafe = safePayloadOrEmpty();
    const r = await fetch(`/api/schedules/${encodeURIComponent(scheduleId)}/items/${encodeURIComponent(brewId)}/optimize`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ iterations, seed, config: payloadSafe.config || {} }),
    });
    const data = await r.json();
    if (!r.ok) {
      printScheduleDebug(schedOptimizationOutEl, data);
      if (schedOptStatusEl) { schedOptStatusEl.textContent = "Failed"; }
      if (schedStepOptimizeEl) { schedStepOptimizeEl.classList.remove("is-success","is-active","is-warning"); schedStepOptimizeEl.classList.add("is-warning"); }
      return;
    }
    currentScheduleId = scheduleId;
    lastOptimizeContext = { mode: "schedule", scheduleId, brewId };
    renderCandidateTable(data, {
      ...lastOptimizeContext,
      targetWrapEl: schedCandidateTableWrapEl,
      sortSelectEl: schedCandidateSortEl,
    });
    lastOptimizePayload = data;
    printScheduleDebug(schedOptimizationOutEl, data);
    if (schedStepOptimizeEl && schedOptStatusEl) {
      schedStepOptimizeEl.classList.remove("is-success","is-active","is-warning");
      schedStepOptimizeEl.classList.add("is-success");
      schedOptStatusEl.textContent = "Complete";
    }
    updateScheduleBrewState(brewId, {
      optimization_status: "complete",
      optimization_at: nowIso(),
      top_candidate_mass_kg: Number(data?.top_candidates?.[0]?.total_discharged_mass_kg || 0),
      candidate_count: Number((data?.top_candidates || []).length),
    });
    renderScheduleBrewDetails();
    // refresh brew plan status
    if (currentScheduleId) {
      refreshScheduleState(currentScheduleId).then(() => {
        const schedOptScheduleIdEl = document.getElementById("sched_opt_schedule_id");
        if (schedOptScheduleIdEl?.value) renderSchedSchedulePlan({ schedule_id: schedOptScheduleIdEl.value, name: "", items: Array.from(scheduleBrewState.values()).map(b => ({ brew_id: b.brew_id, brew_index: b.brew_index, target_params: {}, status: b.optimization_status === "complete" ? "optimized" : b.discharge_status === "complete" ? "applied" : "pending" })) });
      }).catch(() => {});
    }
  } catch (e) {
    printScheduleDebug(schedOptimizationOutEl, `Optimize schedule failed: ${String(e)}`);
  } finally {
    schedOptimizeBtn.disabled = false;
    schedOptimizeBtn.textContent = "Optimize Schedule";
  }
}

async function applyCandidateDischarge(candidateIndex) {
  if (!lastOptimizePayload?.top_candidates?.length) {
    printRaw("No optimization candidate available.");
    return;
  }
  const candidate = lastOptimizePayload.top_candidates[candidateIndex];
  const dischargePlan = candidate?.recommended_discharge || [];
  if (!dischargePlan.length) {
    printRaw("Selected candidate has no discharge plan.");
    return;
  }
  const payloadSafe = safePayloadOrEmpty();
  const r = await fetch("/api/process/apply_discharge", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      discharge: dischargePlan,
      config: payloadSafe.config || {},
    }),
  });
  const data = await r.json();
  if (!r.ok) {
    printRaw(data);
    return;
  }
  printRaw(data);
  if (data.summary) {
    const summary = data.summary;
    const totalCapacity = Number((summary.silos || []).reduce((a, s) => a + Number(s.capacity_kg || 0), 0));
    const totalUsed = Number((summary.silos || []).reduce((a, s) => a + Number(s.used_kg || 0), 0));
    const totalRemaining = Number((summary.silos || []).reduce((a, s) => a + Number(s.remaining_kg || 0), 0));
    const dischargedThisEvent = Number(data.predicted_run?.total_discharged_mass_kg || 0);

    kpiDischargedEl.textContent = dischargedThisEvent.toFixed(3);
    kpiRemainingEl.textContent = totalRemaining.toFixed(3);

    summaryCardsEl.innerHTML = `
      <div class="card"><div class="card-key">Total Capacity (kg)</div><div class="card-value">${totalCapacity.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Current Inventory (kg)</div><div class="card-value">${totalUsed.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Total Remaining (kg)</div><div class="card-value">${totalRemaining.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Discharged This Event (kg)</div><div class="card-value">${dischargedThisEvent.toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Cumulative Discharged (kg)</div><div class="card-value">${Number(summary.cumulative_discharged_kg || 0).toFixed(3)}</div></div>
      <div class="card"><div class="card-key">Incoming Queue Mass (kg)</div><div class="card-value">${Number(summary.incoming_queue?.total_mass_kg || 0).toFixed(3)}</div></div>
    `;

    renderSiloFillTable(summary);
    remainingFocusWrapEl.innerHTML = "";
    contributionWrapEl.innerHTML = "";
    stateLedgerWrapEl.innerHTML = "";
    if (data.predicted_run) {
      lastRunResult = data.predicted_run;
    }
    setStepState(stepResults, statusResults, "is-success", "Updated");
    runStatusEl.className = "run-status success";
    runStatusEl.textContent = "Discharge applied to silos using selected candidate.";
  }
  if (data.state) {
    const current = safePayloadOrEmpty();
    payloadEl.value = JSON.stringify(
      {
        ...current,
        silos: data.state.silos || [],
        layers: data.state.layers || [],
        suppliers: data.state.suppliers || [],
        incoming_queue: data.state.incoming_queue || [],
      },
      null,
      2
    );
    renderUpcomingLots({
      layers: data.state.layers || [],
      incoming_queue: data.state.incoming_queue || [],
    });
  }
}

async function applyScheduleCandidateDischarge(scheduleId, brewId, candidateIndex) {
  const resolvedScheduleId = String(scheduleId || "").trim();
  const resolvedBrewId = String(brewId || "").trim();
  if (!resolvedScheduleId || !resolvedBrewId) {
    if (schedSimStatusEl) { schedSimStatusEl.className = "run-status error"; schedSimStatusEl.textContent = "Schedule discharge requires schedule_id and brew_id."; }
    return;
  }
  const payloadSafe = safePayloadOrEmpty();
  const r = await fetch(
    `/api/schedules/${encodeURIComponent(resolvedScheduleId)}/items/${encodeURIComponent(resolvedBrewId)}/apply`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ candidate_index: Number(candidateIndex || 0), config: payloadSafe.config || {} }),
    }
  );
  const data = await r.json();
  if (!r.ok) {
    printScheduleDebug(schedOptimizationOutEl, data);
    if (schedSimStatusEl) { schedSimStatusEl.className = "run-status error"; schedSimStatusEl.textContent = `Discharge failed: ${data?.detail || "Unknown error"}`; }
    return;
  }
  const result = data?.result || {};
  const predicted = result?.predicted_run || {};
  const summary = result?.summary || {};
  // Render updated simulation results in the schedule Run Simulation card
  renderSchedSimResults(
    summary,
    `Discharge applied for brew ${resolvedBrewId}. Inventory updated.`,
    "success"
  );
  // Render contribution breakdown for discharge
  renderSchedDischargeContribution(predicted);
  // Also update studio state so Studio tab stays consistent
  if (result?.state) {
    const current = safePayloadOrEmpty();
    payloadEl.value = JSON.stringify(
      {
        ...current,
        silos: result.state.silos || [],
        layers: result.state.layers || [],
        suppliers: result.state.suppliers || [],
        incoming_queue: result.state.incoming_queue || [],
      },
      null,
      2
    );
    renderUpcomingLots({
      layers: result.state.layers || [],
      incoming_queue: result.state.incoming_queue || [],
    });
  }
  updateScheduleBrewState(resolvedBrewId, {
    discharge_status: "complete",
    discharge_at: nowIso(),
    discharged_mass_kg: Number(predicted.total_discharged_mass_kg || 0),
    selected_candidate_index: Number(data?.candidate_index || 0),
    applied_event_id: data?.applied_event_id ?? null,
  });
  renderScheduleBrewDetails();
  await refreshScheduleState(resolvedScheduleId);
  if (schedStepRunEl && schedSimStatusEl) {
    schedStepRunEl.classList.remove("is-success","is-active","is-warning");
    schedStepRunEl.classList.add("is-success");
    schedSimStatusEl.textContent = "Updated";
  }
  printScheduleDebug(schedOptimizationOutEl, data);
}

// ── Front-end validation ─────────────────────────────────────────────────────
//
// FIELD_RULES defines min/max (and optional integer flag) for every validated
// input. showFieldError / clearFieldError inject/remove a <span> underneath
// the input and toggle field-invalid / field-valid CSS classes.
// validateGroup() runs a list of rule ids and returns true only if all pass.
// ─────────────────────────────────────────────────────────────────────────────

const FIELD_RULES = {
  // Optimization workbench — COA targets
  t_moisture_pct:        { label: "Moisture %",           min: 3.0,  max: 12.0  },
  t_fine_extract_db_pct: { label: "Fine Extract db%",     min: 78.0, max: 85.0  },
  t_wort_pH:             { label: "Wort pH",              min: 5.5,  max: 6.2   },
  t_diastatic_power_WK:  { label: "Diastatic Power (WK)", min: 200,  max: 500   },
  t_total_protein_pct:   { label: "Total Protein %",      min: 8.0,  max: 14.0  },
  t_wort_colour_EBC:     { label: "Wort Colour (EBC)",    min: 2.0,  max: 8.0   },
  opt_iterations:        { label: "Iterations",           min: 1,    max: 2000, integer: true },
  opt_seed:              { label: "Seed",                 min: 0,    max: 999999, integer: true },
  // Schedule — generate random
  sched_silos_count:     { label: "Silos Count",          min: 1,    max: 10,   integer: true },
  sched_lots_count:      { label: "Lots Count",           min: 1,    max: 1000, integer: true },
  sched_lot_size_kg:     { label: "Lot Size (kg)",        min: 1,    max: 50000 },
  // Schedule — generate plan
  sched_brews_count:     { label: "Brews Count",          min: 1,    max: 50,   integer: true },
};

function _errSpanId(fieldId) { return `__verr_${fieldId}`; }

function showFieldError(fieldId, message) {
  const el = document.getElementById(fieldId);
  if (!el) return;
  el.classList.add("field-invalid");
  el.classList.remove("field-valid");
  let span = document.getElementById(_errSpanId(fieldId));
  if (!span) {
    span = document.createElement("span");
    span.id = _errSpanId(fieldId);
    span.className = "field-error-msg";
    span.setAttribute("role", "alert");
    el.parentNode.appendChild(span);
  }
  span.textContent = message;
}

function clearFieldError(fieldId) {
  const el = document.getElementById(fieldId);
  if (!el) return;
  el.classList.remove("field-invalid");
  el.classList.add("field-valid");
  const span = document.getElementById(_errSpanId(fieldId));
  if (span) span.textContent = "";
}

function validateOneField(fieldId) {
  const rule = FIELD_RULES[fieldId];
  if (!rule) return true;
  const el = document.getElementById(fieldId);
  if (!el) return true;
  const raw = el.value.trim();
  if (raw === "") {
    showFieldError(fieldId, `${rule.label} is required.`);
    return false;
  }
  const val = Number(raw);
  if (Number.isNaN(val)) {
    showFieldError(fieldId, `${rule.label} must be a number.`);
    return false;
  }
  if (rule.integer && !Number.isInteger(val)) {
    showFieldError(fieldId, `${rule.label} must be a whole number.`);
    return false;
  }
  if (val < rule.min || val > rule.max) {
    showFieldError(fieldId, `${rule.label}: enter a value between ${rule.min} and ${rule.max}.`);
    return false;
  }
  clearFieldError(fieldId);
  return true;
}

function validateGroup(fieldIds) {
  let ok = true;
  for (const id of fieldIds) {
    if (!validateOneField(id)) ok = false;
  }
  return ok;
}

const OPT_FIELDS = [
  "t_moisture_pct", "t_fine_extract_db_pct", "t_wort_pH",
  "t_diastatic_power_WK", "t_total_protein_pct", "t_wort_colour_EBC",
  "opt_iterations", "opt_seed",
];
const SCHED_RANDOM_FIELDS = ["sched_silos_count", "sched_lots_count", "sched_lot_size_kg"];
const SCHED_PLAN_FIELDS   = ["sched_brews_count"];

function validatePayloadContents() {
  const raw = payloadEl ? payloadEl.value.trim() : "";
  const showErr = (msg) => {
    if (payloadEl) { payloadEl.classList.add("field-invalid"); payloadEl.classList.remove("field-valid"); }
    if (runStatusEl) { runStatusEl.className = "run-status error"; runStatusEl.textContent = msg; }
  };
  if (!raw) {
    showErr("Payload is empty. Load sample data or enter a valid JSON payload first.");
    return false;
  }
  let parsed;
  try { parsed = JSON.parse(raw); } catch (_) {
    showErr("Payload is not valid JSON. Fix the syntax before running.");
    return false;
  }
  const missing = ["silos", "layers", "suppliers", "discharge"].filter((k) => !(k in parsed));
  if (missing.length) {
    showErr(`Payload missing required keys: ${missing.join(", ")}.`);
    return false;
  }
  const badFracs = (Array.isArray(parsed.discharge) ? parsed.discharge : []).filter((d) => {
    const f = Number(d.discharge_fraction);
    return !Number.isNaN(f) && (f < 0 || f > 1);
  });
  if (badFracs.length) {
    showErr(`Discharge fractions must be between 0 and 1. Check: ${badFracs.map((d) => d.silo_id).join(", ")}.`);
    return false;
  }
  if (payloadEl) { payloadEl.classList.remove("field-invalid"); payloadEl.classList.add("field-valid"); }
  return true;
}

// Attach blur listeners so errors appear as soon as the user tabs away.
function _attachBlurValidation(fieldIds) {
  for (const id of fieldIds) {
    const el = document.getElementById(id);
    if (el) el.addEventListener("blur", () => validateOneField(id));
  }
}
_attachBlurValidation([...OPT_FIELDS, ...SCHED_RANDOM_FIELDS, ...SCHED_PLAN_FIELDS]);

// Validate payload textarea on blur (only when it already has content).
if (payloadEl) {
  payloadEl.addEventListener("blur", () => {
    if (payloadEl.value.trim()) validatePayloadContents();
  });
}

// ─────────────────────────────────────────────────────────────────────────────

document.getElementById("validateBtn").addEventListener("click", validatePayload);
document.getElementById("runBtn").addEventListener("click", runSimulation);
if (optimizeBtn) optimizeBtn.addEventListener("click", optimizeBlend);
if (tabStudioBtn) tabStudioBtn.addEventListener("click", () => setActiveTab("studio"));
if (tabScheduleBtn) tabScheduleBtn.addEventListener("click", () => setActiveTab("schedule"));
if (schedGenerateRandomBtn) schedGenerateRandomBtn.addEventListener("click", generateRandomScheduleData);
if (schedGenerateScheduleBtn) schedGenerateScheduleBtn.addEventListener("click", generateSchedulePlan);
if (schedRunSimulationBtn) schedRunSimulationBtn.addEventListener("click", runSimulationFromScheduleTab);
if (schedOptimizeBtn) schedOptimizeBtn.addEventListener("click", optimizeScheduleItemFromTab);
if (optPresetEl) {
  optPresetEl.addEventListener("change", () => {
    const mode = optPresetEl.value;
    const iterationsEl = document.getElementById("opt_iterations");
    if (!iterationsEl) return;
    if (mode === "fast") iterationsEl.value = "40";
    if (mode === "balanced") iterationsEl.value = "120";
    if (mode === "aggressive") iterationsEl.value = "260";
  });
}
if (candidateSortEl) {
  candidateSortEl.addEventListener("change", () => {
    if (!lastOptimizePayload) return;
    if (lastOptimizeContext.mode === "schedule") {
      renderCandidateTable(lastOptimizePayload, {
        ...lastOptimizeContext,
        targetWrapEl: schedCandidateTableWrapEl,
        sortSelectEl: schedCandidateSortEl,
      });
      return;
    }
    renderCandidateTable(lastOptimizePayload, {
      ...lastOptimizeContext,
      targetWrapEl: candidateTableWrapEl,
      sortSelectEl: candidateSortEl,
    });
  });
}
if (schedCandidateSortEl) {
  schedCandidateSortEl.addEventListener("change", () => {
    if (!lastOptimizePayload || lastOptimizeContext.mode !== "schedule") return;
    renderCandidateTable(lastOptimizePayload, {
      ...lastOptimizeContext,
      targetWrapEl: schedCandidateTableWrapEl,
      sortSelectEl: schedCandidateSortEl,
    });
  });
}
if (candidateTableWrapEl) {
  candidateTableWrapEl.addEventListener("click", (e) => {
    const btn = e.target.closest(".candidate-discharge-btn");
    if (!btn) return;
    const idx = Number(btn.getAttribute("data-candidate-index") || 0);
    const contextMode = String(btn.getAttribute("data-context-mode") || "studio");
    const scheduleId = String(btn.getAttribute("data-schedule-id") || "");
    const brewId = String(btn.getAttribute("data-brew-id") || "");
    if (contextMode === "schedule") {
      applyScheduleCandidateDischarge(scheduleId, brewId, idx);
      return;
    }
    applyCandidateDischarge(idx);
  });
}
if (schedCandidateTableWrapEl) {
  schedCandidateTableWrapEl.addEventListener("click", (e) => {
    const btn = e.target.closest(".candidate-discharge-btn");
    if (!btn) return;
    const idx = Number(btn.getAttribute("data-candidate-index") || 0);
    const contextMode = String(btn.getAttribute("data-context-mode") || "schedule");
    const scheduleId = String(btn.getAttribute("data-schedule-id") || "");
    const brewId = String(btn.getAttribute("data-brew-id") || "");
    if (contextMode === "schedule") {
      applyScheduleCandidateDischarge(scheduleId, brewId, idx);
      return;
    }
    applyCandidateDischarge(idx);
  });
}

document.addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "enter") {
    e.preventDefault();
    if (!isRunning && !runBtn.disabled) runSimulation();
  }
  if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key.toLowerCase() === "o") {
    e.preventDefault();
    if (!isOptimizing && optimizeBtn && !optimizeBtn.disabled) optimizeBlend();
  }
});

setValidationState(false, 0, 0);
setStepState(stepRun, statusRun, "", "Pending");
setStepState(stepResults, statusResults, "", "Pending");
setStepState(stepOptimize, statusOptimize, "", "Pending");
runStatusEl.className = "run-status";
runStatusEl.textContent = "Simulation not started.";
printRaw("UI ready. Load sample, validate inputs, run, then optimize.");
printScheduleDebug(schedOptimizationOutEl, "Optimization not run yet.");
renderScheduleBrewDetails();

scheduleInitialSampleLoad();

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
let isValidating = false;
let isRunning = false;
let isOptimizing = false;

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
  if (lower.includes("duplicate") || lower.includes("unknown") || lower.includes("missing")) {
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

  let hint = "Review this field value and rerun validation.";
  if (lower.includes("missing")) hint = "Add the required column in the referenced CSV file.";
  if (lower.includes("duplicate")) hint = "Remove duplicates and keep unique key rows only.";
  if (lower.includes("between 0 and 1")) hint = "Set value within range 0.0 to 1.0.";
  if (lower.includes("must have")) hint = "Ensure all rows satisfy the numeric constraint.";

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

function renderCandidateTable(payload) {
  const candidates = (payload?.top_candidates || []).slice();
  const sortKey = candidateSortEl?.value || "discharged";
  if (sortKey === "discharged") {
    candidates.sort(
      (a, b) => Number(b.total_discharged_mass_kg || 0) - Number(a.total_discharged_mass_kg || 0)
    );
  }

  if (!candidates.length) {
    candidateTableWrapEl.innerHTML = "";
    return;
  }
  const rows = candidates
    .map(
      (c, idx) => `
    <tr>
      <td>${idx + 1}</td>
      <td>${Number(c.total_discharged_mass_kg).toFixed(3)}</td>
      <td>${(c.recommended_discharge || []).map((r) => `${r.silo_id}:${Number(r.discharge_fraction).toFixed(3)}`).join(" | ")}</td>
      <td>${Object.entries(c.blended_params || {})
        .map(([k, v]) => `${k}:${Number(v).toFixed(3)}`)
        .join(" | ")}</td>
      <td><button class="btn btn-alt candidate-discharge-btn" data-candidate-index="${idx}">Discharge</button></td>
    </tr>
  `
    )
    .join("");
  candidateTableWrapEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Discharged (kg)</th>
          <th>Recommended Fractions</th>
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
    if (data.valid) {
      const counts = renderValidationIssues(data.errors || []);
      setValidationState(true, counts.blockingCount, counts.warningCount);
    } else {
      const counts = renderValidationIssues(data.errors || []);
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
      return;
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
    printRaw(data);
  } catch (e) {
    setStepState(stepRun, statusRun, "is-warning", "Run Failed");
    runStatusEl.className = "run-status error";
    runStatusEl.textContent = "Simulation failed before completion.";
    printRaw(String(e));
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
    renderCandidateTable(data);
    lastOptimizePayload = data;

    const top = data.top_candidates || [];
    const lines = [];
    lines.push(`Method: ${data.objective_method}`);
    lines.push("Top candidates:");
    top.forEach((c, i) => {
      lines.push(
        `${i + 1}. discharged=${Number(c.total_discharged_mass_kg).toFixed(3)}kg`
      );
    });
    lines.push("");
    lines.push(JSON.stringify(data, null, 2));
    optOutEl.textContent = lines.join("\n");
    setStepState(stepOptimize, statusOptimize, "is-success", "Complete");
    if (optimizeBtn) optimizeBtn.disabled = false;
  } catch (e) {
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

document.getElementById("validateBtn").addEventListener("click", validatePayload);
document.getElementById("runBtn").addEventListener("click", runSimulation);
if (optimizeBtn) optimizeBtn.addEventListener("click", optimizeBlend);
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
    if (lastOptimizePayload) renderCandidateTable(lastOptimizePayload);
  });
}
if (candidateTableWrapEl) {
  candidateTableWrapEl.addEventListener("click", (e) => {
    const btn = e.target.closest(".candidate-discharge-btn");
    if (!btn) return;
    const idx = Number(btn.getAttribute("data-candidate-index") || 0);
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

scheduleInitialSampleLoad();

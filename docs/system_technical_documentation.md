# 1. Executive Summary

This application is an offline-first silo discharge and malt blending optimization platform. It helps users decide how much material to discharge from each silo so the resulting blend is as close as possible to quality targets while respecting inventory and physics constraints.

It appears intended for production planning, quality, and operations teams in a malting/brewing context.

# 2. Product Purpose and Business Context

Based on code naming (`brew_schedule`, `COA`, `diastatic_power_WK`, `wort_pH`), the product models a brewing supply process where lots are stored in silos and discharged into brews.

Business problem solved:
- choose per-silo discharge quantities for each brew,
- meet quality targets,
- preserve traceability and inventory state over sequential brews.

Inferred intent:
- replace manual trial-and-error with a repeatable optimization workflow,
- give users several candidate scenarios including a standard baseline.

## 2.1 Historical vs Current Operating Configuration

To make the workflow more realistic for production-scale behavior, the operating scale was expanded from an earlier demo setup.

Earlier setup (historical):
- fixed discharge target was around **7000 kg**,
- silo capacity was around **8000 kg**.

Current setup (realism-oriented):
- lot size: **25 tonnes** (25,000 kg),
- silo capacity: **100 tonnes** (100,000 kg),
- fixed discharge target: **9 tonnes** (9,000 kg).

Operational behavior clarification:
- earlier runs could appear to refill very frequently around discharge cycles,
- current intended behavior is: **fill only when available silo mass is insufficient** for required discharge target,
- otherwise discharge proceeds without forcing a fill cycle.

Also introduced:
- **Standard split scenario** (equal mass split across silos) as a baseline alongside optimized scenarios,
- if equal split is infeasible due to any silo shortage, scenario is returned as infeasible with explicit reason.

# 3. High-Level Architecture

- Frontend: static UI (`src/dem_sim/ui/index.html`, `app.js`, `styles.css`) for state view, optimize, schedule, apply discharge.
- Backend API: FastAPI (`src/dem_sim/web.py`) orchestrates validation, optimization, persistence, and state transitions.
- Optimization engine: `src/dem_sim/model.py` (physics + blending), called through `src/dem_sim/service.py::run_blend`.
- State engine: `src/dem_sim/state.py` maintains in-memory current state and mutation logic.
- DB: PostgreSQL via `src/dem_sim/db.py`; schema in `src/dem_sim/schema.py`.
- Optional result snapshot storage abstraction in `src/dem_sim/storage.py`.

Text flow:
UI action -> API endpoint -> validation -> optimization/simulation -> DB/state updates -> response rendered in UI.

# 4. Repository Structure

Key folders/files:
- `src/dem_sim/web.py`: all major endpoints, optimization pipeline, schedule flow.
- `src/dem_sim/model.py`: physics discharge model, layer contribution logic, blend calculation.
- `src/dem_sim/service.py`: `RunConfig`, `run_blend` wrapper.
- `src/dem_sim/state.py`: live state, fill-only simulation, apply discharge, summaries.
- `src/dem_sim/reporting.py`: input/COA validation and reporting helpers.
- `src/dem_sim/charger.py`: lot charging logic into silos/layers.
- `src/dem_sim/schema.py`: DB DDL and compatibility migrations.
- `src/dem_sim/db.py`: DB connection + `execute`/`fetchall`.
- `src/dem_sim/sample_data.py`: embedded CSV sample constants.
- `src/dem_sim/synthetic.py`: synthetic lot generation utility.
- `src/dem_sim/ui/app.js`: frontend API integration and scenario rendering.
- `scripts/optimize_standalone_db.py`: script-level optimization workflow.

# 5. End-to-End Functional Workflow

1. Bootstrap state from latest event snapshot, DB tables, or static sample fallback.
2. Optionally generate random data (`/api/data/generate-random`) and seed DB/state.
3. Run fill simulation (`/api/process/run_simulation`) to move queue lots into silo layers.
4. Trigger optimization (`/api/process/optimize` or schedule optimize endpoint).
5. Backend evaluates candidate discharge plans and returns ranked scenarios.
6. User selects scenario and applies discharge (`/api/process/apply_discharge`).
7. State and DB snapshots/events are updated for next brew.

## 5.1 Charging Functionality (Fill Process) - Detailed

Charging is the process of moving material from `incoming_queue` into silo `layers`.

Main path:
- API: `POST /api/process/run_simulation`
- Backend: `web.py` calls `state.py::run_fill_only_simulation()`
- Fill logic: implemented through state/charger flow (`state.py` + `charger.py`)

What happens:
1. Current state is loaded (`silos`, `layers`, `suppliers`, `incoming_queue`).
2. Eligible queue lots (remaining mass > 0, not fully consumed) are considered in order.
3. Lots are assigned into silos according to charging policy/available capacity rules.
4. New or updated layer entries are created (`silo_id`, `layer_index`, `lot_id`, `supplier`, remaining mass).
5. Queue rows are updated with `remaining_mass_kg`; if exhausted, `is_fully_consumed=true`.
6. Stage/history summary is recorded in state.
7. DB synchronization:
   - queue sync via `_sync_incoming_queue_to_db(...)`,
   - layer snapshot via `_sync_layers_to_db(...)`,
   - event logged in `sim_events`.

Business effect:
- silos become loaded with stratified layers,
- incoming queue shrinks,
- next optimization uses the newly charged layer structure.

## 5.2 Silo Discharge Functionality (Apply Candidate) - Detailed

Discharge is the process of consuming silo layers according to a selected plan.

Main path:
- API: `POST /api/process/apply_discharge`
- Backend flow in `web.py` + state mutation in `state.py::apply_discharge_to_state(...)`

What happens:
1. Request discharge plan is read (`discharge_mass_kg` or `discharge_fraction` per silo).
2. Plan is converted to per-silo mass map.
3. `_normalize_discharge_to_target(...)` enforces exact total target and per-silo caps.
4. System runs a prediction using `/api/run` logic (`run_blend`) before mutation.
5. If predicted total discharge misses strict tolerance, request is rejected.
6. If valid, state mutation consumes layer masses in each silo:
   - decrement `remaining_mass_kg` layer-by-layer,
   - remove/zero depleted layers as applicable,
   - update cumulative discharged mass and summaries.
7. DB writes:
   - `sim_events` row with before/after state,
   - `discharge_results` insert,
   - `layers` snapshot append,
   - optional persisted result bundle.

Business effect:
- inventory is truly advanced to post-discharge state,
- next brew optimization is based on updated residual layers, not original inventory.

# 6. Input Data Model and Business Meaning

## Silos
- Fields: `silo_id`, `capacity_kg`, `body_diameter_m`, `outlet_diameter_m`, `initial_mass_kg`.
- Role: physical constraints and flow geometry.

## Incoming Queue (lots)
- Fields include `lot_id`, `supplier`, `mass_kg`, `remaining_mass_kg`, quality parameters.
- Role: future inventory source for filling.

## Layers
- Fields: `silo_id`, `layer_index`, `lot_id`, `supplier`, segment/remaining mass.
- Role: stratification model used in discharge physics and blend composition.

## Suppliers / COA
- Quality fields used for blended parameter estimation.
- If missing, suppliers can be inferred from incoming queue rows.

## Discharge
- Per-silo `discharge_mass_kg` or `discharge_fraction`.
- Normalized to fixed total target during optimization.

## Config
- Physics/simulation config (`rho_bulk_kg_m3`, `beverloo_*`, `sigma_m`, `steps`, optional correction params).

# 7. Random / Sample Data Generation

Sources:
- static CSV constants in `sample_data.py`,
- random payload builder in `web.py::_generate_random_payload`,
- optional synthetic utility in `synthetic.py`.

Random generation:
- reproducible via seed,
- generates silos/lots and COA-like parameter values in configured ranges,
- used for demo and testing workflows.

Risk:
- if random ranges exceed COA validation bounds, optimize requests fail with 422 COA errors.

# 8. Optimization Engine Deep Dive

Entry point:
- `POST /api/optimize` in `web.py`.

What is optimized:
- per-silo discharge fractions/masses under fixed total discharge.

Core constraints:
- exact discharge target,
- no silo over-discharge beyond available mass,
- validation and COA checks.

Search method:
- randomized hybrid search (explore + exploit).
- each candidate is normalized to fixed target and evaluated via `run_blend`.

Evaluation:
- call `run_blend` -> `model.run_multi_silo_blend`,
- compute objective score by normalized weighted parameter error,
- keep best and diverse top candidates,
- include standard equal-split scenario for comparison.

## 8.1 Practical Optimization Flow (Step-by-Step)

1. Build optimization input from current state (or provided payload): silos, layers, suppliers.
2. Compute available mass by silo from current active layers.
3. Generate candidate discharge fractions within configured bounds.
4. Convert candidate fractions to per-silo discharge masses.
5. Normalize masses so summed discharge equals fixed target mass exactly.
6. Run physics + blending simulation (`run_blend`) for that candidate.
7. Reject candidate if physical output discharge is outside fixed-target tolerance.
8. Score candidate against target parameters using normalized weighted error.
9. Repeat for all iterations (explore + exploit).
10. Rank/select best and diverse top scenarios, then append standard equal-split scenario.

## 8.2 Candidate-to-Ranking Trace (Text Flowchart)

`fractions -> normalized discharge masses -> run_blend -> blended_params -> objective_score -> candidate pool -> top/diverse selection -> API response`

Where:
- `run_blend` is the expensive/physics-heavy part,
- scoring/ranking is lightweight compared to simulation,
- feasibility checks are applied both before and after simulation.

## 8.3 Mini Numerical Example (Conceptual)

Assume:
- fixed discharge target = 9000 kg,
- 3 silos with available masses: S1=12000, S2=10000, S3=8000,
- candidate fractions: [0.50, 0.40, 0.30].

Raw candidate masses:
- S1: 6000, S2: 4000, S3: 2400 (total=12400).

Normalization to target 9000:
- scale factor = 9000 / 12400 = 0.7258,
- normalized masses: S1=4354.8, S2=2903.2, S3=1741.9 (sum=9000).

This normalized plan is sent to `run_blend`.
`run_blend` returns blended parameters (for example moisture/extract/pH/DP/protein/color).
Then objective score is computed (lower is better).

The optimizer compares this score against other candidates and keeps the best/scenario-diverse set.

# 9. Layers Logic

Layers represent stacked lot segments inside each silo and are central to realism:
- discharge is front-based and probabilistic, not a simple average over full silo.
- layer order and mass determine which lots contribute more at each timestep.
- feasibility (available mass) is layer-mass-driven.

Persistence:
- snapshots stored append-only in `layers` table with `snapshot_id`.

## 9.1 How Layers Influence Blend Outputs

The blend is not computed as a simple average over all silo material. Instead:
- each silo has ordered layers,
- discharge front progresses during simulation timesteps,
- layers near the active front contribute more mass in that step,
- contribution spread is controlled by sigma/mixing settings.

Result:
- two silos with same total average quality can still produce different discharge quality if layer ordering differs.

## 9.2 Layer Contribution in Each Timestep (Conceptual)

Per timestep:
1. Determine allowable mass to remove in that step.
2. Compute probabilistic contribution weights for layers around current front.
3. Propose per-layer removal amounts from those weights.
4. Clamp removals by each layer’s remaining mass.
5. Renormalize so total removal still matches allowed step removal.
6. Update layer remaining masses and continue.

This gives strict mass conservation and avoids negative layer masses.

## 9.3 Why This Matters Operationally

Because each brew usually discharges only a fraction of silo inventory:
- the optimizer mostly “sees” the currently reachable layer zone,
- deeper layers may have low influence in current brew,
- after applying discharge, front position changes and next brew sees different effective composition.

This explains why sequential brew optimization is stateful and why applying one scenario changes future recommendations.

# 10. Scoring Logic

Implemented in `web.py` (`_score_blend*` functions).

General form:
- weighted normalized distance between blended result and target parameters.
- lower score = better.

Details:
- per-parameter normalization by configured ranges,
- business weights via `PARAM_WEIGHTS`,
- infeasible candidates are rejected before ranking.

# 11. Business Rules and Requirements Mapping

| Rule | Code Location | Enforcement | Violation |
|---|---|---|---|
| Fixed total discharge per brew | `web.py` fixed target constants + normalize function | strict normalization + tolerance checks | 422 or candidate rejection |
| Cannot exceed available silo mass | `_normalize_discharge_to_target` | capping + redistribution | 422 if impossible |
| Queue consumption tracking | `_sync_incoming_queue_to_db` | updates remaining/consumed flags | stale queue avoided |
| COA plausibility checks | `reporting.py::validate_supplier_coa` | bounds validation | 422 with detailed messages |
| Schedule item existence for optimize/apply | schedule endpoints in `web.py` | DB lookups | 404 |
| Candidate index validity on apply | apply endpoint | bounds check | 422 |
| Fill is conditional (not mandatory every discharge) | optimize/apply prechecks + fill call path | trigger fill only when available mass < fixed target | no fill when enough mass exists |
| Standard equal split baseline scenario | optimize post-processing | add equal-per-silo scenario + feasibility check | marked infeasible with shortage reason |

# 12. Data Persistence and State Management

Main tables:
- `silos`, `layers`, `suppliers`, `incoming_queue`,
- `sim_events`, `results_run`, `results_optimize`, `discharge_results`,
- `brew_schedules`, `brew_schedule_items`.

State:
- in-memory singleton in `state.py`,
- synchronized to DB snapshots/events at key operations.

Tracking:
- remaining mass and consumed status both in layers and incoming queue.

## 12.1 State Changes During Charging vs Discharge

Charging (`run_simulation_fill_only`):
- decreases `incoming_queue.remaining_mass_kg`,
- increases/creates silo layer masses,
- writes fill-stage event and new layer snapshot.

Discharge (`apply_discharge`):
- decreases silo layer remaining masses,
- leaves queue unchanged unless a prior auto-fill was triggered,
- writes apply-stage event and new layer snapshot.

Why this matters:
- optimization for subsequent brews always depends on the latest persisted + in-memory residual layer state.

# 13. API Documentation from Code

Core endpoints:
- `GET /health`
- `GET /api/sample`
- `POST /api/data/generate-random`
- `GET /api/state`
- `POST /api/state/reset`
- `POST /api/process/run_simulation`
- `POST /api/process/optimize`
- `POST /api/process/apply_discharge`
- `POST /api/validate`
- `POST /api/run`
- `POST /api/optimize`
- `POST /api/schedules/generate`
- `GET /api/schedules/{schedule_id}`
- `POST /api/schedules/{schedule_id}/items/{brew_id}/optimize`
- `POST /api/schedules/{schedule_id}/items/{brew_id}/apply`

Request models:
- `RunRequest`, `OptimizeRequest`, `Process*Request`, `Schedule*Request` classes in `web.py`.

# 14. Frontend Functional Understanding

UI supports:
- data generation/reset,
- fill simulation,
- optimization for process and schedule flows,
- candidate review and discharge apply.

Frontend depends on backend response contracts for:
- candidate arrays,
- blended params,
- objective scores,
- feasibility/status fields.

# 15. Important Algorithms and Core Functions

- `service.py::run_blend`: simulation entry used by run + optimize.
- `model.py::run_multi_silo_blend`: aggregate multi-silo discharge and blending.
- `model.py::estimate_discharge_contrib_for_silo`: per-silo contribution simulation.
- `model.py::_simulate_for_sigma`: timestep discharge/mixing kernel.
- `model.py::blend_params_from_contrib`: weighted blended parameters from lot contributions.
- `web.py::optimize`: heuristic candidate search, scoring, ranking, persistence.
- `web.py::_normalize_discharge_to_target`: exact-target mass normalization.
- `state.py::run_fill_only_simulation`: queue-to-silo loading.
- `state.py::apply_discharge_to_state`: consume layer masses and update state.
- `reporting.py::validate_inputs_shape` + `validate_supplier_coa`: request integrity and COA guardrails.

# 16. Technical Assumptions, Risks, and Limitations

Assumptions:
- fixed discharge target is meaningful for each brew,
- target params and weight settings reflect business priorities,
- layer model approximates real discharge behavior sufficiently.

Risks:
- heuristic search may miss global optimum,
- search runtime grows with iterations, steps, and scenario count,
- custom random ranges can conflict with COA validators,
- frontend/backend version drift can hide newer response fields.

# 17. Suggested Questions for Business Validation

1. Are parameter weights aligned with actual production KPIs?
2. Should some targets be one-sided constraints instead of exact numeric goals?
3. Is fixed discharge quantity always constant per brew?
4. Is equal-split baseline always required in result sets?
5. What candidate differences are operationally meaningful to planners?
6. Should COA validations be strict or mode-dependent (demo vs production)?

# 18. Suggested Technical Improvements

Code quality:
- separate optimization orchestration from API controller.

Architecture:
- add async/background execution for heavy optimize runs.

Data modeling:
- version target profiles and scoring settings.

Optimization:
- optional staged search and richer diagnostics.

Explainability:
- return per-parameter objective contribution and constraint-binding reasons.

Testing:
- stronger contract tests for API response shape and schedule flows.

Readiness:
- API versioning and improved runtime telemetry dashboards.

# 19. Glossary

- Silo: storage vessel for malt lots.
- Lot: discrete batch with quality attributes.
- Incoming queue: lots waiting to be charged into silos.
- Layer: stacked lot segment in silo.
- Discharge: material drawn out per brew.
- Blend: combined discharged material quality profile.
- COA: certificate-of-analysis quality values.
- RunConfig: physics and simulation settings.
- Remaining mass: unconsumed quantity.
- Fully consumed: lot exhausted (`is_fully_consumed=true`).
- Objective score: optimization error metric (lower is better).

# 20. Appendix: Code-to-Concept Traceability

| Concept | Files / Functions |
|---|---|
| API orchestration | `src/dem_sim/web.py` |
| Physics + blending | `src/dem_sim/model.py` |
| Simulation config and dispatch | `src/dem_sim/service.py` |
| State transitions | `src/dem_sim/state.py` |
| Validation + COA rules | `src/dem_sim/reporting.py` |
| Fill/charge mechanics | `src/dem_sim/charger.py` |
| DB schema and tables | `src/dem_sim/schema.py` |
| DB operations | `src/dem_sim/db.py` |
| Sample datasets | `src/dem_sim/sample_data.py` |
| UI integration | `src/dem_sim/ui/app.js` |

# 21. Short Business-Friendly Summary

This system helps operations and quality teams decide how much to discharge from each silo so each brew is closer to quality targets. It uses silo/layer inventory, supplier lot properties, and a physics-aware discharge model to estimate resulting blend quality.

Instead of returning only one plan, it can return multiple candidate scenarios plus a standard equal-split baseline. Teams can compare alternatives, apply a selected discharge, and continue to the next brew with updated inventory state.

Key dependencies:
- accurate inventory/layer state,
- realistic lot quality data,
- aligned target values and score weights.

Before production rollout, business users should validate:
- whether target definitions and score weights reflect real KPIs,
- whether fixed discharge assumptions match plant operations,
- whether current validation limits and candidate diversity are appropriate for decision-making.

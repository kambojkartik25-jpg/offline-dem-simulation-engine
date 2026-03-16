# Offline DEM Simulation Engine

Production-grade simulator for estimating discharged malt blend composition from layered silo discharge, built for **brewery operations**. Uses the Beverloo mass-flow equation coupled with a Gaussian mixing kernel (and three optional brewery-specific physics extensions) to predict the lot-level blend at the discharge outlet.

---

## Table of Contents

- [Overview](#overview)
- [Physics Model](#physics-model)
  - [Beverloo Mass-Flow Equation](#beverloo-mass-flow-equation)
  - [Gaussian Mixing Kernel](#gaussian-mixing-kernel)
  - [Brewery Physics Extensions](#brewery-physics-extensions)
- [Optimizer](#optimizer)
  - [Search Strategy](#search-strategy)
  - [Brew-Master Parameter Weights](#brew-master-parameter-weights)
  - [Inventory Feasibility Check](#inventory-feasibility-check)
  - [Diversity Selection](#diversity-selection)
- [Data Quality & Validation](#data-quality--validation)
  - [Structural Validation](#structural-validation)
  - [Supplier COA Validation](#supplier-coa-validation)
- [Synthetic Data Generator](#synthetic-data-generator)
- [Front-End Validation](#front-end-validation)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [CLI Usage](#cli-usage)
- [Web UI / API](#web-ui--api)
- [PostgreSQL Persistence](#postgresql-persistence)
- [Lifecycle DB Testing](#lifecycle-db-testing)
- [Running Tests](#running-tests)
- [Input Files](#input-files)
- [Output Artifacts](#output-artifacts)
- [Docker](#docker)

---

## Overview

Brewery malt silos are filled in layers (each layer = one supplier lot). During discharge, grains from different layers mix as they flow through the hopper. This engine simulates that process to answer:

> *"Given the current silo state and a target discharge volume, what lot proportions and blended COA parameters will come out?"*

It supports the full silo lifecycle — charge → discharge → recharge → discharge — and validates seven physical invariants at every step.

---

## Physics Model

### Beverloo Mass-Flow Equation

The instantaneous mass-flow rate from each silo outlet is:

```
Q = C · ρ · √g · (D - k·d)^2.5
```

| Symbol | Meaning | Typical value |
|--------|---------|--------------|
| `C` | discharge coefficient | 0.58 |
| `ρ` | bulk density (kg/m³) | 610 (malt) |
| `g` | gravity (m/s²) | 9.81 |
| `D` | outlet diameter (m) | silo-specific |
| `k` | dead-zone correction | 1.4 |
| `d` | grain diameter (m) | 0.004 (malt) |

The effective diameter `D - k·d` corrects for the grain dead-zone at the outlet edge.

### Gaussian Mixing Kernel

Discharge is simulated in time steps. At each step the discharge front height `z_front` descends. The probability that a given layer contributes to the current timestep's outflow follows a Normal CDF centred on `z_front`:

```
P(layer i) ∝ Φ((z1_i - z_front) / σ) - Φ((z0_i - z_front) / σ)
```

`σ` (sigma_m) controls the mixing width — larger values mean more inter-layer blending.

### Brewery Physics Extensions

Three optional improvements tuned for **malt grain** (3–6 mm, 550–650 kg/m³, moisture 3–12%). All default to `0.0` (off) and are fully backward-compatible.

#### 1. Moisture-Dependent Cohesion (`moisture_beta`)

Wet malt is more cohesive and flows slower. The effective mass-flow rate is reduced per-layer:

```
dm_eff = dm × exp(−β × moisture_pct)
```

- `moisture_beta = 0.0` → no correction (default)
- Recommended production value: `0.05`
- Per-layer moisture is sourced from `suppliers.csv`

#### 2. Sigma Height-Scaling (`sigma_alpha`)

As the silo empties, the grain column shortens and the mixing zone narrows:

```
σ(t) = σ₀ × (h_remaining / h_initial) ^ α
```

- `sigma_alpha = 0.0` → constant σ (default)
- Recommended production value: `0.4`
- Higher `α` → mixing concentrates earlier, spreads less as silo empties

#### 3. Asymmetric Mixing Kernel (`skew_alpha`)

Real hopper geometry creates convergence zones that bias discharge toward sub-front layers. Implemented as an **exponential tilt** applied after the Normal CDF:

```
weight_i = exp(α × (z_center_i − z_front) / σ)
```

- `skew_alpha = 0.0` → symmetric Gaussian (default)
- `skew_alpha = −2.0` → biases toward layers below the front (hopper convergence zone)
- Recommended production value: `−2.0`
- Always strictly positive (unlike skew-normal CDF surrogates which can go non-monotone)

**Combined recommended settings for brewery malt:**

```bash
dem-sim run --in data/sample --out outputs/latest \
  --moisture-beta 0.05 \
  --sigma-alpha 0.4 \
  --skew-alpha -2.0
```

---

## Optimizer

### Search Strategy

The `/api/optimize` endpoint searches for discharge fractions that produce a blended COA as close as possible to a user-supplied target, while keeping total discharged mass fixed at 12,000 kg.

**Algorithm — Hybrid Explore / Exploit:**

```
total_iterations = N
  explore phase (60%): stratified random sampling across [0.2, 0.8] fraction space
  exploit phase (40%): simulated-annealing local perturbation around current best
```

Each candidate is evaluated by running the full physics simulation and scoring the resulting blended COA against the target using the objective function below.

**Objective function — Normalised Weighted L2:**

```
score = √( Σ  w_i × ((actual_i − target_i) / range_i)² )
```

where `range_i` is the EBC/ASBC reference range for parameter `i` and `w_i` is the brew-master importance weight.

---

### Brew-Master Parameter Weights

Equal weights across all six COA parameters would allow the optimizer to sacrifice diastatic power (critical, uncorrectable in-process) in favour of colour (cosmetic, least important for base malt). The optimizer uses domain-informed weights:

| Parameter | Weight | Brewing rationale |
|-----------|--------|-------------------|
| `diastatic_power_WK` | **0.30** | Enzyme activity — cannot be corrected in-process without exogenous enzymes |
| `fine_extract_db_pct` | **0.25** | Yield and economics — directly drives alcohol content and cost per brew |
| `wort_pH` | **0.20** | Mash chemistry — affects enzyme activity windows and hop utilisation; partially correctable with salts |
| `total_protein_pct` | **0.15** | Haze and head retention — manageable through process (finings, protein rest) |
| `moisture_pct` | **0.07** | Storage and yield — predictable and consistent; low brew-day impact |
| `wort_colour_EBC` | **0.03** | Spec parameter — least critical for base-malt blending |

The weights are defined in `PARAM_WEIGHTS` in `web.py` and are returned in every `/api/optimize` response under `param_weights` for full auditability.

---

### Inventory Feasibility Check

Before the optimizer runs, the engine computes the **achievable range** for each COA parameter from the active silo inventory by joining layers with supplier COA data:

```
achievable_min = min(supplier_value) across all layers with positive remaining mass
achievable_max = max(supplier_value) across all layers with positive remaining mass
```

If any target parameter falls outside `[achievable_min, achievable_max]`, a feasibility warning is returned:

```json
"feasibility_warnings": [
  {
    "param": "wort_colour_EBC",
    "target": 4.5,
    "achievable_min": 3.86,
    "achievable_max": 3.98,
    "direction": "above",
    "message": "wort_colour_EBC: target 4.5 is above the achievable inventory range [3.86 – 3.98]"
  }
]
```

The optimizer still runs and returns the closest achievable blend — the warning tells the brewer why the objective score is high.

---

### Diversity Selection

The top-5 candidates returned are selected using a **Maximin diversity criterion** rather than simply taking the 5 lowest-scoring results. This prevents all top candidates from being near-identical discharge plans:

1. Pool the top `min(len(candidates), 30)` scored candidates.
2. Seed selection with the best-scoring candidate.
3. Greedily add the candidate whose minimum distance to the already-selected set (in discharge-fraction space) is largest.

This ensures the brewer sees meaningfully different blend strategies, not minor variations of the same plan.

---

## Data Quality & Validation

### Structural Validation

`dem-sim validate` (CLI) and `POST /api/validate` (API) check:

- Required columns present in all four input files
- No duplicate `silo_id` or `(silo_id, layer_index)` pairs
- All suppliers referenced in `layers.csv` exist in `suppliers.csv`
- `capacity_kg`, `body_diameter_m`, `outlet_diameter_m` > 0
- `segment_mass_kg` ≥ 0
- `discharge_fraction` ∈ [0, 1]

---

### Supplier COA Validation

Every supplier's COA parameters are validated against two tiers of bounds grounded in **EBC Analytica** and **ASBC Methods of Analysis**:

**Tier 1 — Hard science bounds (blocks request with HTTP 422):**

Values outside these ranges are physically impossible for commercial base malt and indicate a data entry error or unit mismatch.

| Parameter | Min | Max | Reference |
|-----------|-----|-----|-----------|
| `moisture_pct` | 3.5% | 6.5% | EBC 4.2 — kiln floor minimum / storage limit |
| `fine_extract_db_pct` | 78.0% | 86.0% | EBC 4.5.1 — minimum conversion / 2-row ceiling |
| `wort_pH` | 5.6 | 6.2 | ASBC Malt-6 — buffering minimum / under-modification maximum |
| `diastatic_power_WK` | 150 | 550 | EBC 4.12 — self-conversion minimum / specialty strain maximum |
| `total_protein_pct` | 8.5% | 13.5% | EBC 4.3.1 — nitrogen-deficient minimum / haze-risk maximum |
| `wort_colour_EBC` | 2.5 | 12.0 | EBC 8.5 — any kilned malt floor / pre-crystal ceiling |

**Tier 2 — Typical contract range (advisory warning, simulation runs):**

Values inside science bounds but outside the typical range a commercial maltster would ship. The simulation proceeds but `coa_warnings` is populated in the response.

| Parameter | Typical min | Typical max |
|-----------|------------|------------|
| `moisture_pct` | 3.8% | 5.5% |
| `fine_extract_db_pct` | 80.0% | 84.5% |
| `wort_pH` | 5.7 | 6.1 |
| `diastatic_power_WK` | 200 | 450 |
| `total_protein_pct` | 9.5% | 12.5% |
| `wort_colour_EBC` | 3.0 | 8.0 |

COA warnings are shown as amber cards in the validation panel and as an inline banner above run/optimize results in the UI.

**CLI output example:**
```
[ERROR] suppliers.csv COA error: supplier 'SupA' — Moisture % value 95 is outside
        physical science bounds [3.5–6.5] (EBC/ASBC reference). Check units or data entry.
[WARN]  suppliers.csv COA warning: supplier 'SupB' — Diastatic Power (WK) value 480 is
        outside typical contract range [200.0–450.0] but within science bounds [150.0–550.0].
        Verify COA document.
```

---

## Synthetic Data Generator

The `dem-sim init-synthetic` command generates a physically realistic brewery dataset using a **multivariate normal model** grounded in malting science, replacing the original independent-uniform sampling.

### Improvements over the original generator

| Feature | Before | After |
|---------|--------|-------|
| COA correlations | None — all parameters independent | Multivariate normal with physics-backed correlation matrix |
| Supplier profiles | Generic random values | Three commercial archetypes |
| Lot-to-lot variation | Spans full parameter range | Tight ±σ reflecting commercial process control |
| Lot mass | Fixed 2,000 kg | Variable 800–5,000 kg (truncated normal, mean 2,000 kg) |
| Silo fill level | Always 100% full | 40–100% fill (reflects real mid-cycle inventory) |
| Science bounds | Not enforced | All values clipped to EBC/ASBC limits |

### Correlation matrix (malting science basis)

| Parameter pair | Correlation | Physical reason |
|----------------|-------------|-----------------|
| Extract ↔ Protein | **−0.70** | Higher protein = less starch = less fermentable yield |
| Protein ↔ Diastatic Power | **+0.45** | High-protein grain has a larger enzyme-producing aleurone layer |
| EBC Colour ↔ Diastatic Power | **−0.35** | Higher kilning temperature darkens malt but destroys amylase |
| Extract ↔ Diastatic Power | **−0.20** | Slight yield / enzyme activity trade-off |
| Protein ↔ pH | **−0.20** | Higher protein slightly buffers pH downward |
| Moisture ↔ Extract | **−0.15** | Wetter malt has marginally less dry-matter yield |

### Supplier archetypes

Each supplier is assigned one of three commercially realistic maltster profiles:

| Archetype | Style | Extract | Protein | DP (WK) | EBC |
|-----------|-------|---------|---------|---------|-----|
| A | Premium Continental (Weyermann, Crisp) | 82.5% | 10.2% | 305 | 3.8 |
| B | Standard Commercial (Malteurop, COFCO) | 81.8% | 10.85% | 335 | 4.0 |
| C | High Enzyme (North American, BBM) | 81.1% | 11.4% | 360 | 3.7 |

Notice the physical consistency: Archetype A (highest extract) has the lowest protein — exactly the relationship captured by the −0.70 extract/protein correlation.

---

## Front-End Validation

All user inputs are validated client-side before any API call is made. Invalid fields are highlighted with a red border and an inline error message; valid fields turn green.

**Validated fields and ranges:**

| Field | Min | Max | Notes |
|-------|-----|-----|-------|
| Target moisture % | 3.0 | 12.0 | |
| Target fine extract db% | 78.0 | 85.0 | |
| Target wort pH | 5.5 | 6.2 | |
| Target diastatic power (WK) | 200 | 500 | |
| Target total protein % | 8.0 | 14.0 | |
| Target wort colour (EBC) | 2.0 | 8.0 | |
| Optimizer iterations | 1 | 2000 | Integer |
| Optimizer seed | 0 | 999999 | Integer |
| Schedule silos count | 1 | 10 | Integer |
| Schedule lots count | 1 | 1000 | Integer |
| Schedule lot size (kg) | 1 | 50000 | |
| Schedule brews count | 1 | 50 | Integer |

The payload JSON textarea is also validated before simulation — checks for valid JSON, presence of required keys (`silos`, `layers`, `suppliers`, `discharge`), and discharge fractions in [0, 1].

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  CLI (dem_sim.cli)          Web API (dem_sim.web)        │
│       │                           │                      │
│       └──────────┬────────────────┘                      │
│                  ▼                                       │
│         Service Layer (dem_sim.service)                  │
│              RunConfig dataclass                         │
│                  │                                       │
│                  ▼                                       │
│         Physics Engine (dem_sim.model)                   │
│    Beverloo + Gaussian kernel + 3 extensions             │
│                  │                                       │
│         ┌────────┴────────┐                              │
│         ▼                 ▼                              │
│    State (dem_sim.state)  Storage (dem_sim.storage)      │
│    in-memory silo state   PostgreSQL / NullStorage       │
└─────────────────────────────────────────────────────────┘
```

**Storage pattern:** `NullStorage` is returned when `DEM_SIM_DATABASE_URL` is absent; `PostgresStorage` (SQLAlchemy + psycopg3) when present. The `layers` table uses an append-only snapshot pattern — current state is always `WHERE snapshot_id = MAX(snapshot_id)`.

---

## Project Structure

```
src/dem_sim/
├── model.py          # Physics engine (Beverloo + Gaussian + 3 extensions)
├── service.py        # RunConfig dataclass + run_blend orchestrator
├── cli.py            # Command-line interface
├── web.py            # FastAPI app and all API endpoints
├── state.py          # In-memory silo state + lifecycle functions
├── charger.py        # Lot allocation to silos (charge logic)
├── storage.py        # PostgresStorage / NullStorage factory
├── schema.py         # DDL for operational tables
├── db.py             # psycopg3 connection helpers
├── db_models.py      # SQLAlchemy ORM models
├── io.py             # CSV input loading and output helpers
├── reporting.py      # Output artifact writers + COA validation (validate_supplier_coa)
├── sample_data.py    # Built-in sample dataset (hardcoded CSVs — 3 real suppliers)
├── synthetic.py      # Synthetic dataset generator (correlated multivariate normal COA)
└── ui/
    ├── index.html    # Web app shell
    ├── styles.css    # Responsive UI styling
    └── app.js        # Frontend behaviour and API integration

tests/
├── conftest.py                    # Lifecycle test infrastructure + 7-invariant checker
├── test_lifecycle_silos.py        # 10 lifecycle DB scenario tests
├── test_physics_improvements.py   # 27 physics unit tests (3 extensions)
├── test_model_validation.py       # Model input validation tests
├── test_model_equivalence.py      # Regression equivalence
├── test_model_performance.py      # Performance guard
├── test_process_run_simulation.py # Fill-only simulation endpoint tests
├── test_web_api.py                # Web API smoke tests
└── test_smoke.py                  # End-to-end smoke test

scripts/
├── run_example.py      # Quick run using in-code example dataframes
└── db_sanity_check.py  # DB schema/data sanity checks
```

---

## Installation

```bash
python -m pip install -e .
```

Or install dependencies only:

```bash
python -m pip install -r requirements.txt
```

Requires Python ≥ 3.10.

---

## CLI Usage

**Generate sample input files:**
```bash
dem-sim init-sample --out data/sample
```

**Generate synthetic input files:**
```bash
dem-sim init-synthetic --out data/synthetic --seed 42 --silos 3 --suppliers 3 --lots 8
```

**Validate inputs:**
```bash
dem-sim validate --in data/sample
```

**Run simulation (basic):**
```bash
dem-sim run --in data/sample --out outputs/latest --auto-adjust
```

**Run with brewery physics extensions:**
```bash
dem-sim run --in data/sample --out outputs/latest \
  --auto-adjust \
  --moisture-beta 0.05 \
  --sigma-alpha 0.4 \
  --skew-alpha -2.0
```

**Physics extension flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--moisture-beta` | `0.0` | Cohesion correction — higher β = wetter malt flows slower |
| `--sigma-alpha` | `0.0` | Sigma height-scaling — mixing narrows as silo empties |
| `--skew-alpha` | `0.0` | Asymmetric kernel — negative values bias toward sub-front layers |

**`--steps` guidance:**

| Range | Use case |
|-------|----------|
| 400–800 | Quick exploratory runs |
| 1200–2000 | Default production planning |
| 3000+ | High-fidelity sensitivity checks |

**Without installing as a package:**
```bash
PYTHONPATH=src python -m dem_sim run --in data/sample --out outputs/latest --auto-adjust
```

---

## Web UI / API

**Start the server:**
```bash
dem-sim-web --host 127.0.0.1 --port 8000
```

Open:
- `http://127.0.0.1:8000/` — full web UI
- `http://127.0.0.1:8000/docs` — Swagger / OpenAPI

**Operator flow (UI):**
1. `Load Sample`
2. `Validate Inputs`
3. `Run Simulation`
4. Configure optimization target + preset / iterations / seed
5. `Optimize Blend`
6. Review KPI strip, top candidates, scenario compare, explainability snapshot

**Keyboard shortcuts:**
- `Ctrl/Cmd + Enter` — run simulation
- `Ctrl/Cmd + Shift + O` — optimize blend

**Key endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/api/sample` | Load sample data |
| `POST` | `/api/validate` | Validate inputs |
| `POST` | `/api/run` | Run simulation |
| `POST` | `/api/optimize` | Optimize discharge fractions toward target COA |
| `POST` | `/api/process/run_simulation` | Fill-only simulation (charge queue into silos) |
| `POST` | `/api/process/apply_discharge` | Apply discharge plan, update silo state |

The optimizer uses a **brew-master-weighted normalised L2 objective** (6 parameters weighted by brewing importance, scaled by EBC/ASBC reference ranges) with stratified exploration + simulated-annealing local refinement. Returns best plan, top-5 diversity-selected candidates, inventory feasibility warnings, and supplier COA warnings. See the [Optimizer](#optimizer) section for full details.

---

## PostgreSQL Persistence

The engine can persist silo state snapshots, discharge results, and the full event timeline to PostgreSQL 16.

**Setup:**
```bash
export DEM_SIM_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/dem_sim
dem-sim-web
```

Schema is auto-created on first startup.

**Tables created automatically:**

| Table | Contents |
|-------|---------|
| `silos` | Silo physical parameters |
| `suppliers` | Supplier COA parameters (moisture, extract, pH, etc.) |
| `layers` | Append-only lot layer snapshots (current = max snapshot_id) |
| `incoming_queue` | Lots waiting to be charged into silos |
| `results_run` | Discharge simulation results |
| `sim_snapshots` | Full state + summary per key event (ORM) |
| `sim_stages` | Deduped stage timeline (ORM) |
| `sim_history` | Deduped state history (ORM) |
| `sim_results` | Run / optimize / discharge prediction payloads (ORM) |

If `DEM_SIM_DATABASE_URL` is not set, the app runs fully in-memory with no change in behaviour.

---

## Lifecycle DB Testing

The lifecycle tests validate that the full silo cycle — charge → discharge → recharge → discharge — maintains **seven physical invariants** at every single step, checked against both the database and the in-memory state.

**Seven Invariants:**

| ID | Invariant |
|----|-----------|
| INV-1 | **Mass conservation** — `db_remaining + cumulative_discharged == total_charged` (±0.01 kg) |
| INV-2 | **Layer index integrity** — per silo, `layer_index` is contiguous 1..N with no gaps |
| INV-3 | **Supplier consistency** — a `lot_id` always maps to the same supplier, never changes |
| INV-4 | **No negative mass** — every `layers.loaded_mass >= 0` |
| INV-5 | **Capacity never exceeded** — `SUM(loaded_mass per silo) <= capacity_kg` |
| INV-6 | **Lot mass accounting** — `silo_mass + queue_mass <= original_charged_mass` per lot |
| INV-7 | **DB matches memory** — DB total per silo == in-memory total per silo (±0.01 kg) |

**Ten scenario tests:**

| Scenario | What it tests |
|----------|--------------|
| Single charge + discharge | Baseline correctness |
| Repeated discharges | 4 × 20% discharges, conservation at each step |
| Charge → discharge → recharge → discharge | Core brewery cycle; new lots stack on top |
| Lot split across silos | 8000 kg lot distributes across two 5000 kg silos |
| Capacity overflow | Excess stays in queue, never enters silos |
| 20 random cycles | Mass conservation + snapshot_id monotonicity |
| Supplier consistency | Exhaustive DB check of INV-3 across all history |
| DB ↔ memory agreement | Explicit INV-7 check at every single step |
| Zero-mass layer cleanup | Fully discharged layers do not corrupt future ops |
| Full brewery week | 6 lots initial fill, 5 brews, mid-week delivery |

---

## Running Tests

**All tests (physics + lifecycle + API):**
```bash
DEM_SIM_TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/dem_sim_test" \
  python -m pytest -v
```

**Physics tests only (no DB required):**
```bash
python -m pytest tests/test_physics_improvements.py -v
```

**Lifecycle DB tests only:**
```bash
DEM_SIM_TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/dem_sim_test" \
  python -m pytest tests/test_lifecycle_silos.py -v
```

Lifecycle tests are **auto-skipped** when `DEM_SIM_TEST_DATABASE_URL` is not set.

**Current test coverage: 44 tests, all passing.**

---

## Input Files

Place these CSV files in the input directory:

| File | Required columns |
|------|-----------------|
| `silos.csv` | `silo_id`, `capacity_kg`, `body_diameter_m`, `outlet_diameter_m` |
| `layers.csv` | `silo_id`, `layer_index`, `lot_id`, `supplier`, `segment_mass_kg` |
| `suppliers.csv` | `supplier`, `moisture_pct`, `fine_extract_db_pct`, `wort_pH`, `diastatic_power_WK`, `total_protein_pct`, `wort_colour_EBC` |
| `discharge.csv` | `silo_id`, `discharge_fraction` or `discharge_mass_kg` |

---

## Output Artifacts

Generated in the output directory after a run:

| File | Contents |
|------|---------|
| `segment_contributions.csv` | Per-segment lot contribution at each timestep |
| `lot_contributions.csv` | Aggregated discharged mass per lot |
| `segment_state_ledger.csv` | Initial / discharged / remaining per segment |
| `lot_state_ledger.csv` | Initial / discharged / remaining per lot per silo |
| `silo_state_ledger.csv` | Initial / discharged / remaining per silo |
| `summary.json` | Total discharged mass, blended COA parameters, per-silo results |

---

## Docker

**Start the database only:**
```bash
docker compose up -d db
```

**Start the full stack (app + db):**
```bash
docker compose up -d
```

The app is available at `http://localhost:8000`.

**Environment variables:**

| Variable | Description |
|----------|-------------|
| `DEM_SIM_DATABASE_URL` | PostgreSQL connection string for the app |
| `DEM_SIM_TEST_DATABASE_URL` | Test database URL (lifecycle tests only) |

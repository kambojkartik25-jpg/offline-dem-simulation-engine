# Inputs and Outputs Overview

## 1) Main Inputs Used by the Codebase

### A. File/CSV Inputs (CLI + batch runs)
- `silos.csv`
  - Required columns: `silo_id`, `capacity_kg`, `body_diameter_m`, `outlet_diameter_m`
  - Optional: `initial_mass_kg`
- `layers.csv`
  - Required: `silo_id`, `layer_index`, `lot_id`, `supplier`, `segment_mass_kg`
- `suppliers.csv`
  - Required: `supplier`
  - Typical quality params: `moisture_pct`, `fine_extract_db_pct`, `wort_pH`, `diastatic_power_WK`, `total_protein_pct`, `wort_colour_EBC`
- `discharge.csv`
  - Required: `silo_id`
  - At least one per row: `discharge_mass_kg` or `discharge_fraction`

### B. API Inputs (Web UI / external clients)
- `POST /api/validate` -> payload with `silos`, `layers`, `suppliers`, `discharge`, `config`
- `POST /api/run` -> same payload as validate
- `POST /api/optimize` -> run payload + `target_params`, `iterations`, `seed`
- `POST /api/process/run_simulation` -> fill simulation input (state-oriented)
- `POST /api/process/apply_discharge` -> discharge plan (`silo_id` + mass/fraction)

### C. Database Inputs (runtime persistence path)
- Connection via env var: `DEM_SIM_DATABASE_URL`
- Operational tables read/written during runtime:
  - `silos`, `layers`, `incoming_queue`, `suppliers`
  - Event/result tracking: `sim_events`, `discharge_results`, `results_optimize`
  - Legacy/result history: `sim_snapshots`, `sim_stages`, `sim_history`, `sim_results`

## 2) Main Outputs Generated

### A. API Outputs
- `POST /api/run`
  - `total_discharged_mass_kg`, `total_remaining_mass_kg`, `total_blended_params`
  - `per_silo` (discharged mass, flow, time, sigma, blended params)
  - `silo_state_ledger`
- `POST /api/optimize`
  - `objective_score`, `recommended_discharge`, `best_run`
  - `top_candidates`, `target_params`, method/range metadata
- Process endpoints return updated `state` and `summary` (silos/layers/incoming queue metrics)

### B. File Outputs (CLI reporting)
- `segment_contributions.csv`
- `lot_contributions.csv`
- `silo_state_ledger.csv`
- `lot_state_ledger.csv`
- `segment_state_ledger.csv`
- `summary.json`

### C. Database Outputs
- `sim_events`: consolidated event log (`state_before`, `state_after`, discharge/objective metrics)
- `layers`: append-only snapshots of silo layer state (`snapshot_id`, `event_type`, `sim_event_id`, `loaded_mass`)
- `discharge_results`: discharge action results (`discharge_by_silo`, predicted run, before/after summaries, `sim_event_id`)
- `results_optimize`: optimization result bundles (`objective_score`, recommended discharge, candidates, best run, `sim_event_id`)
- Optional/legacy persistence (`sim_snapshots`, `sim_stages`, `sim_history`, `sim_results`) depending on active flow

## 3) Validation/Constraint Notes
- Validation checks ensure required columns exist, numeric bounds are valid, and references are consistent (e.g., layer supplier exists).
- Discharge fractions must be within `[0,1]`; discharge mass cannot be negative.
- Some flows normalize discharge to a fixed target mass, so requested plans may be adjusted internally.


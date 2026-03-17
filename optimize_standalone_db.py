# scripts/optimize_standalone_db.py
from __future__ import annotations
import random
import numpy as np
import pandas as pd

from dem_sim.db import fetchall
from dem_sim.service import RunConfig, run_blend

DISCHARGE_FRACTION_MIN = 0.2
DISCHARGE_FRACTION_MAX = 0.8
FIXED_DISCHARGE_TARGET_KG = 7000.0
FIXED_DISCHARGE_TOL_KG = 1e-3

DEFAULT_PARAM_RANGES = {
    "moisture_pct": 5.0 - 0.0,
    "fine_extract_db_pct": 83.0 - 81.0,
    "wort_pH": 6.0 - 5.8,
    "diastatic_power_WK": 360.0 - 300.0,
    "total_protein_pct": 11.2 - 10.2,
    "wort_colour_EBC": 4.7 - 4.3,
}
PARAM_KEYS = list(DEFAULT_PARAM_RANGES.keys())
PARAM_WEIGHTS = {
    "moisture_pct": 0.07,
    "fine_extract_db_pct": 0.25,
    "wort_pH": 0.20,
    "diastatic_power_WK": 0.30,
    "total_protein_pct": 0.15,
    "wort_colour_EBC": 0.03,
}

def _suppliers_from_queue_rows(rows: list[dict]) -> list[dict]:
    seen: dict[str, dict] = {}
    for r in rows:
        supplier = str(r.get("supplier", "")).strip()
        if not supplier or supplier in seen:
            continue
        seen[supplier] = {
            "supplier": supplier,
            "moisture_pct": float(r.get("moisture_pct", 0.0) or 0.0),
            "fine_extract_db_pct": float(r.get("fine_extract_db_pct", 0.0) or 0.0),
            "wort_pH": float(r.get("wort_pH", 0.0) or 0.0),
            "diastatic_power_WK": float(r.get("diastatic_power_WK", 0.0) or 0.0),
            "total_protein_pct": float(r.get("total_protein_pct", 0.0) or 0.0),
            "wort_colour_EBC": float(r.get("wort_colour_EBC", 0.0) or 0.0),
        }
    return list(seen.values())

def clip_fraction(v: float) -> float:
    return max(DISCHARGE_FRACTION_MIN, min(DISCHARGE_FRACTION_MAX, float(v)))

def available_mass_by_silo(layers_df: pd.DataFrame) -> dict[str, float]:
    if layers_df.empty:
        return {}
    grouped = layers_df.groupby(layers_df["silo_id"].astype(str))["segment_mass_kg"].sum().astype(float)
    return {str(k): float(v) for k, v in grouped.to_dict().items()}

def normalize_discharge_to_target(rows, available_by_silo, target_total_kg):
    cleaned = []
    for r in rows:
        sid = str(r["silo_id"])
        m = max(0.0, float(r.get("discharge_mass_kg", 0.0)))
        m = min(m, float(available_by_silo.get(sid, 0.0)))
        cleaned.append({"silo_id": sid, "discharge_mass_kg": m})

    total = sum(x["discharge_mass_kg"] for x in cleaned)
    if total <= 1e-12:
        return [{"silo_id": sid, "discharge_mass_kg": 0.0, "discharge_fraction": 0.0} for sid in available_by_silo]

    scale = min(1.0, target_total_kg / total)
    out = []
    for x in cleaned:
        sid = x["silo_id"]
        m = x["discharge_mass_kg"] * scale
        avail = float(available_by_silo.get(sid, 0.0))
        frac = (m / avail) if avail > 1e-12 else 0.0
        out.append({"silo_id": sid, "discharge_mass_kg": m, "discharge_fraction": clip_fraction(frac)})
    return out

def score_vec(actual: dict, target: dict, param_ranges: dict) -> float:
    active_keys = [k for k in PARAM_KEYS if k in target and target.get(k) is not None]
    if not active_keys:
        return float("inf")
    a = np.array([actual.get(k, 0.0) for k in active_keys], dtype=np.float64)
    t = np.array([target.get(k, 0.0) for k in active_keys], dtype=np.float64)
    r = np.array([param_ranges.get(k, 1.0) for k in active_keys], dtype=np.float64)
    w = np.array([PARAM_WEIGHTS.get(k, 1.0 / len(active_keys)) for k in active_keys], dtype=np.float64)
    r = np.where(r == 0.0, 1.0, r)
    return float(np.sqrt(np.sum(w * ((a - t) / r) ** 2)))

def load_inputs_from_db() -> dict:
    silos = fetchall("""
        SELECT silo_id, capacity_kg, body_diameter_m, outlet_diameter_m, initial_mass_kg
        FROM silos
        ORDER BY silo_id
    """)
    layers = fetchall("""
        SELECT silo_id, layer_index, lot_id, supplier, loaded_mass
        FROM layers
        WHERE snapshot_id = (SELECT COALESCE(MAX(snapshot_id), 0) FROM layers)
        ORDER BY silo_id, layer_index
    """)
    suppliers = fetchall("""
        SELECT name AS supplier, moisture_pct, fine_extract_db_pct, wort_pH,
               diastatic_power_WK, total_protein_pct, wort_colour_EBC
        FROM suppliers
        ORDER BY name
    """)

    if not silos:
        raise RuntimeError("No silos in DB.")
    if not layers:
        raise RuntimeError("No layers in latest snapshot.")

    if not suppliers:
        queue_rows = fetchall("SELECT * FROM incoming_queue ORDER BY id")
        suppliers = _suppliers_from_queue_rows(queue_rows)

    if not suppliers:
        raise RuntimeError("No suppliers in DB or incoming_queue.")

    silos_df = pd.DataFrame(silos)
    layers_df = pd.DataFrame(layers).rename(columns={"loaded_mass": "segment_mass_kg"})
    layers_df["segment_mass_kg"] = layers_df["segment_mass_kg"].astype(float)
    suppliers_df = pd.DataFrame(suppliers)

    return {
        "silos": silos_df,
        "layers": layers_df,
        "suppliers": suppliers_df,
        "discharge": pd.DataFrame([]),
    }

def optimize_once_db(target_params: dict, seed=42, iterations=120):
    inputs = load_inputs_from_db()
    silos_df = inputs["silos"].copy()
    layers_df = inputs["layers"].copy()
    available = available_mass_by_silo(layers_df)

    available_total = float(sum(available.values()))
    if available_total + 1e-12 < FIXED_DISCHARGE_TARGET_KG:
        raise RuntimeError(
            f"Insufficient mass for target {FIXED_DISCHARGE_TARGET_KG:.3f} kg. "
            f"Available: {available_total:.3f} kg"
        )

    silo_ids = silos_df["silo_id"].astype(str).tolist()
    rng = random.Random(seed)

    total_iter = max(1, int(iterations))
    explore_iters = max(1, int(total_iter * 0.6))
    exploit_iters = total_iter - explore_iters

    best_score = float("inf")
    best = None
    score_vector: list[float] = []
    candidates: list[dict] = []

    def eval_fracs(fracs: list[float]) -> None:
        nonlocal best_score, best
        rows = [
            {
                "silo_id": sid,
                "discharge_mass_kg": float(fr) * float(available.get(sid, 0.0)),
            }
            for sid, fr in zip(silo_ids, fracs)
        ]
        rows = normalize_discharge_to_target(rows, available, FIXED_DISCHARGE_TARGET_KG)

        candidate_inputs = dict(inputs)
        candidate_inputs["discharge"] = pd.DataFrame(rows)

        res = run_blend(candidate_inputs, RunConfig())
        discharged_total = float(res["total_discharged_mass_kg"])
        if abs(discharged_total - FIXED_DISCHARGE_TARGET_KG) > FIXED_DISCHARGE_TOL_KG:
            return

        sc = score_vec(res["total_blended_params"], target_params, DEFAULT_PARAM_RANGES)
        score_vector.append(sc)

        cand = {
            "score": sc,
            "fractions": [float(r["discharge_fraction"]) for r in rows],
            "blended_params": {k: float(v) for k, v in res["total_blended_params"].items()},
            "total_discharged_mass_kg": discharged_total,
        }
        candidates.append(cand)

        if sc < best_score:
            best_score = sc
            best = cand

    # Explore
    for i in range(explore_iters):
        band_lo = DISCHARGE_FRACTION_MIN + (DISCHARGE_FRACTION_MAX - DISCHARGE_FRACTION_MIN) * i / explore_iters
        band_hi = DISCHARGE_FRACTION_MIN + (DISCHARGE_FRACTION_MAX - DISCHARGE_FRACTION_MIN) * (i + 1) / explore_iters
        fracs = [rng.uniform(band_lo, band_hi) for _ in silo_ids]
        rng.shuffle(fracs)
        eval_fracs(fracs)

    # Exploit
    best_fracs = best["fractions"] if best else [0.5 for _ in silo_ids]
    for i in range(exploit_iters):
        anneal = 1.0 - (i / max(1, exploit_iters))
        step = 0.12 * anneal + 0.01
        trial = [clip_fraction(f + rng.uniform(-step, step)) for f in best_fracs]
        eval_fracs(trial)

    return {
        "best_score": best_score,
        "best": best,
        "num_feasible_candidates": len(score_vector),
        "score_vector": np.array(score_vector, dtype=np.float64),
    }

if __name__ == "__main__":
    target_params = {
        "moisture_pct": 4.35,
        "fine_extract_db_pct": 82.40,
        "wort_pH": 5.89,
        "diastatic_power_WK": 332.0,
        "total_protein_pct": 10.60,
        "wort_colour_EBC": 4.40,
    }

    out = optimize_once_db(target_params=target_params, seed=42, iterations=120)
    print("Best score:", out["best_score"])
    print("Best fractions:", out["best"]["fractions"] if out["best"] else None)
    print("Best blended_params:", out["best"]["blended_params"] if out["best"] else None)
    print("Feasible candidates:", out["num_feasible_candidates"])
    print("Score vector shape:", out["score_vector"].shape)
    print("First 10 scores:", out["score_vector"][:10])

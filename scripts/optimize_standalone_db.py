# optimize_standalone_db.py
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

def diverse_top_k(candidates: list[dict], k: int = 5) -> list[dict]:
    if len(candidates) <= k:
        return candidates

    pool = sorted(candidates, key=lambda x: float(x.get("score", float("inf"))))[: max(k * 6, 30)]

    def frac_vec(c: dict) -> np.ndarray:
        return np.array([float(v) for v in c.get("fractions", [])], dtype=np.float64)

    def param_distance(a: dict, b: dict) -> float:
        aa = a.get("blended_params", {}) or {}
        bb = b.get("blended_params", {}) or {}
        keys = [k for k in PARAM_KEYS if k in aa and k in bb]
        if not keys:
            return 0.0
        vals: list[float] = []
        for key in keys:
            scale = float(DEFAULT_PARAM_RANGES.get(key, 1.0))
            if scale <= 0:
                scale = 1.0
            vals.append(abs(float(aa.get(key, 0.0)) - float(bb.get(key, 0.0))) / scale)
        return float(np.linalg.norm(np.array(vals, dtype=np.float64), ord=1))

    pool_vecs = [frac_vec(c) for c in pool]
    selected_indices: list[int] = [0]
    selected: list[dict] = [pool[0]]

    min_frac_dist = 0.22
    min_param_dist = 0.18
    relax = 0.75

    def select_pass(frac_thresh: float, param_thresh: float) -> None:
        while len(selected) < k:
            best_idx = None
            best_rank = -1.0
            for i, cand in enumerate(pool):
                if i in selected_indices:
                    continue
                frac_min = min(np.linalg.norm(pool_vecs[i] - pool_vecs[j]) for j in selected_indices)
                param_min = min(param_distance(cand, s) for s in selected)
                if frac_min < frac_thresh and param_min < param_thresh:
                    continue
                rank = frac_min + 0.5 * param_min
                if rank > best_rank:
                    best_rank = rank
                    best_idx = i
            if best_idx is None:
                break
            selected_indices.append(best_idx)
            selected.append(pool[best_idx])

    select_pass(min_frac_dist, min_param_dist)
    if len(selected) < k:
        select_pass(min_frac_dist * relax, min_param_dist * relax)
    if len(selected) < k:
        select_pass(min_frac_dist * relax * relax, min_param_dist * relax * relax)
    if len(selected) < k:
        for i, cand in enumerate(pool):
            if len(selected) >= k:
                break
            if i in selected_indices:
                continue
            selected_indices.append(i)
            selected.append(cand)
    return selected[:k]

def _suppliers_from_queue_rows(rows: list[dict]) -> list[dict]:
    def _f(row: dict, *keys: str) -> float:
        for key in keys:
            if key in row and row.get(key) is not None:
                try:
                    return float(row.get(key))
                except Exception:
                    continue
        return 0.0

    seen: dict[str, dict] = {}
    for r in rows:
        supplier = str(r.get("supplier", "")).strip()
        if not supplier or supplier in seen:
            continue
        seen[supplier] = {
            "supplier": supplier,
            "moisture_pct": _f(r, "moisture_pct"),
            "fine_extract_db_pct": _f(r, "fine_extract_db_pct"),
            "wort_pH": _f(r, "wort_pH", "wort_ph"),
            "diastatic_power_WK": _f(r, "diastatic_power_WK", "diastatic_power_wk"),
            "total_protein_pct": _f(r, "total_protein_pct"),
            "wort_colour_EBC": _f(r, "wort_colour_EBC", "wort_colour_ebc"),
        }
    return list(seen.values())

def clip_fraction(v: float) -> float:
    return max(DISCHARGE_FRACTION_MIN, min(DISCHARGE_FRACTION_MAX, float(v)))

def available_mass_by_silo(layers_df: pd.DataFrame) -> dict[str, float]:
    if layers_df.empty:
        return {}
    g = layers_df.groupby(layers_df["silo_id"].astype(str))["segment_mass_kg"].sum().astype(float)
    return {str(k): float(v) for k, v in g.to_dict().items()}

def normalize_discharge_to_target(rows, available_by_silo, target_total_kg):
    cleaned = []
    for r in rows:
        sid = str(r["silo_id"])
        m = max(0.0, float(r.get("discharge_mass_kg", 0.0)))
        m = min(m, float(available_by_silo.get(sid, 0.0)))
        cleaned.append({"silo_id": sid, "discharge_mass_kg": m})
    s = sum(x["discharge_mass_kg"] for x in cleaned)
    if s <= 1e-12:
        return [{"silo_id": sid, "discharge_mass_kg": 0.0, "discharge_fraction": 0.0} for sid in available_by_silo]
    scale = min(1.0, target_total_kg / s)
    out = []
    for x in cleaned:
        sid = x["silo_id"]
        m = x["discharge_mass_kg"] * scale
        avail = float(available_by_silo.get(sid, 0.0))
        frac = (m / avail) if avail > 1e-12 else 0.0
        out.append({"silo_id": sid, "discharge_mass_kg": m, "discharge_fraction": clip_fraction(frac)})
    return out

def score_vec(actual: dict, target: dict, param_ranges: dict) -> float:
    active = [k for k in PARAM_KEYS if k in target and target.get(k) is not None]
    if not active:
        return float("inf")
    a = np.array([actual.get(k, 0.0) for k in active], dtype=np.float64)
    t = np.array([target.get(k, 0.0) for k in active], dtype=np.float64)
    r = np.array([param_ranges.get(k, 1.0) for k in active], dtype=np.float64)
    w = np.array([PARAM_WEIGHTS.get(k, 1.0 / len(active)) for k in active], dtype=np.float64)
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
    queue_rows = fetchall("SELECT * FROM incoming_queue ORDER BY id")
    suppliers = _suppliers_from_queue_rows(queue_rows)

    if not silos:
        raise RuntimeError("No silos in DB.")
    if not layers:
        raise RuntimeError("No layers in latest snapshot.")
    if not suppliers:
        raise RuntimeError("No supplier-like specs found in incoming_queue.")

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

    if sum(available.values()) + 1e-12 < FIXED_DISCHARGE_TARGET_KG:
        raise RuntimeError(
            f"Insufficient mass in DB layers for target {FIXED_DISCHARGE_TARGET_KG} kg. "
            f"Available: {sum(available.values()):.3f} kg"
        )

    silo_ids = silos_df["silo_id"].astype(str).tolist()
    rng = random.Random(seed)
    explore = max(1, int(iterations * 0.6))
    exploit = iterations - explore

    best_score = float("inf")
    best = None
    score_history = []
    candidates = []

    def eval_fracs(fracs):
        nonlocal best_score, best
        rows = [{"silo_id": sid, "discharge_mass_kg": float(f) * float(available.get(sid, 0.0))}
                for sid, f in zip(silo_ids, fracs)]
        rows = normalize_discharge_to_target(rows, available, FIXED_DISCHARGE_TARGET_KG)

        candidate_inputs = dict(inputs)
        candidate_inputs["discharge"] = pd.DataFrame(rows)

        res = run_blend(candidate_inputs, RunConfig())
        total = float(res["total_discharged_mass_kg"])
        if abs(total - FIXED_DISCHARGE_TARGET_KG) > FIXED_DISCHARGE_TOL_KG:
            return

        sc = score_vec(res["total_blended_params"], target_params, DEFAULT_PARAM_RANGES)
        active = [k for k in PARAM_KEYS if k in target_params and target_params.get(k) is not None]
        a_vec = np.array([res["total_blended_params"].get(k, 0.0) for k in active], dtype=np.float64)
        t_vec = np.array([target_params.get(k, 0.0) for k in active], dtype=np.float64)
        r_vec = np.array([DEFAULT_PARAM_RANGES.get(k, 1.0) for k in active], dtype=np.float64)
        r_safe = np.where(r_vec == 0.0, 1.0, r_vec)
        w_vec = np.array([PARAM_WEIGHTS.get(k, 1.0 / len(active)) for k in active], dtype=np.float64)
        n_vec = (a_vec - t_vec) / r_safe
        wsq_vec = w_vec * (n_vec ** 2)
        score_history.append(sc)
        cand = {
            "score": sc,
            "fractions": [float(r["discharge_fraction"]) for r in rows],
            "blended_params": dict(res["total_blended_params"]),
            "total_discharged_mass_kg": total,
            "vector_space": {
                "keys": active,
                "actual_vec": a_vec.tolist(),
                "target_vec": t_vec.tolist(),
                "range_vec": r_vec.tolist(),
                "weight_vec": w_vec.tolist(),
                "normalized_delta_vec": n_vec.tolist(),
                "weighted_sq_vec": wsq_vec.tolist(),
            },
        }
        candidates.append(cand)
        if sc < best_score:
            best_score = sc
            best = cand

    # Explore
    for i in range(explore):
        lo = DISCHARGE_FRACTION_MIN + (DISCHARGE_FRACTION_MAX - DISCHARGE_FRACTION_MIN) * i / explore
        hi = DISCHARGE_FRACTION_MIN + (DISCHARGE_FRACTION_MAX - DISCHARGE_FRACTION_MIN) * (i + 1) / explore
        fr = [rng.uniform(lo, hi) for _ in silo_ids]
        rng.shuffle(fr)
        eval_fracs(fr)

    # Exploit
    best_fr = best["fractions"] if best else [0.5] * len(silo_ids)
    for i in range(exploit):
        anneal = 1.0 - (i / max(1, exploit))
        step = 0.12 * anneal + 0.01
        trial = [clip_fraction(f + rng.uniform(-step, step)) for f in best_fr]
        eval_fracs(trial)

    return {
        "best_score": best_score,
        "best": best,
        "num_feasible": len(score_history),
        "score_vector": np.array(score_history, dtype=np.float64),  # vector output
        "candidates": candidates,
        "inputs": inputs,
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
    inputs = out["inputs"]
    print("Input summary:")
    print(f"  silos={len(inputs['silos'])}, layers={len(inputs['layers'])}, inferred_suppliers={len(inputs['suppliers'])}")
    print("  target_params=", target_params)
    silos_df = inputs["silos"].copy()
    layers_df = inputs["layers"].copy()
    suppliers_df = inputs["suppliers"].copy()
    print("\nSilo details:")
    for _, s in silos_df.iterrows():
        print(
            "  "
            f"silo_id={s.get('silo_id')} "
            f"capacity_kg={float(s.get('capacity_kg', 0.0)):.3f} "
            f"body_diameter_m={float(s.get('body_diameter_m', 0.0)):.3f} "
            f"outlet_diameter_m={float(s.get('outlet_diameter_m', 0.0)):.3f} "
            f"initial_mass_kg={float(s.get('initial_mass_kg', 0.0) or 0.0):.3f}"
        )
    if not layers_df.empty:
        print("\nLayer mass by silo:")
        layer_mass_by_silo = (
            layers_df.groupby(layers_df["silo_id"].astype(str))["segment_mass_kg"]
            .sum()
            .astype(float)
            .to_dict()
        )
        for sid, m in layer_mass_by_silo.items():
            print(f"  silo_id={sid} total_layer_mass_kg={float(m):.3f}")
        print("\nLayer details:")
        view_cols = [c for c in ["silo_id", "layer_index", "lot_id", "supplier", "segment_mass_kg"] if c in layers_df.columns]
        view_df = layers_df[view_cols].copy().sort_values(["silo_id", "layer_index"], kind="mergesort")
        for _, row in view_df.iterrows():
            print(
                "  "
                f"silo_id={row.get('silo_id')} "
                f"layer_index={int(row.get('layer_index', 0) or 0)} "
                f"lot_id={row.get('lot_id')} "
                f"supplier={row.get('supplier')} "
                f"segment_mass_kg={float(row.get('segment_mass_kg', 0.0) or 0.0):.3f}"
            )
    if not suppliers_df.empty:
        print("\nInput parameter specifications (inferred from incoming_queue):")
        spec_cols = [
            "supplier",
            "moisture_pct",
            "fine_extract_db_pct",
            "wort_pH",
            "diastatic_power_WK",
            "total_protein_pct",
            "wort_colour_EBC",
        ]
        spec_cols = [c for c in spec_cols if c in suppliers_df.columns]
        spec_view = suppliers_df[spec_cols].copy().sort_values(["supplier"], kind="mergesort")
        for _, row in spec_view.iterrows():
            print(
                "  "
                f"supplier={row.get('supplier')} "
                f"moisture_pct={float(row.get('moisture_pct', 0.0) or 0.0):.3f} "
                f"fine_extract_db_pct={float(row.get('fine_extract_db_pct', 0.0) or 0.0):.3f} "
                f"wort_pH={float(row.get('wort_pH', 0.0) or 0.0):.3f} "
                f"diastatic_power_WK={float(row.get('diastatic_power_WK', 0.0) or 0.0):.3f} "
                f"total_protein_pct={float(row.get('total_protein_pct', 0.0) or 0.0):.3f} "
                f"wort_colour_EBC={float(row.get('wort_colour_EBC', 0.0) or 0.0):.3f}"
            )
    print("Best score:", out["best_score"])
    print("Best fractions:", out["best"]["fractions"] if out["best"] else None)
    print("Best blended_params:", out["best"]["blended_params"] if out["best"] else None)
    print("Feasible candidate count:", out["num_feasible"])
    print("Score vector shape:", out["score_vector"].shape)
    print("First 10 scores:", out["score_vector"][:10])
    if out["score_vector"].size > 0:
        scores = out["score_vector"]
        print("Score min/max:", float(scores.min()), float(scores.max()))
        print("Score spread (max-min):", float(scores.max() - scores.min()))
        print("Score std:", float(scores.std()))
    print("\nTop 5 blended results:")
    top5 = diverse_top_k(out["candidates"], k=5)
    for i, cand in enumerate(top5, start=1):
        print(f"{i}. score={cand['score']:.6f}, total_discharged={cand['total_discharged_mass_kg']:.3f} kg")
        print(f"   fractions={cand['fractions']}")
        print(f"   blended_params={cand['blended_params']}")
        vs = cand.get("vector_space", {})
        if vs:
            print(f"   vector.keys={vs.get('keys')}")
            print(f"   vector.actual={vs.get('actual_vec')}")
            print(f"   vector.target={vs.get('target_vec')}")
            print(f"   vector.range={vs.get('range_vec')}")
            print(f"   vector.weight={vs.get('weight_vec')}")
            print(f"   vector.normalized_delta={vs.get('normalized_delta_vec')}")
            print(f"   vector.weighted_sq={vs.get('weighted_sq_vec')}")
    if len(top5) >= 2:
        keys = [
            "moisture_pct",
            "fine_extract_db_pct",
            "wort_pH",
            "diastatic_power_WK",
            "total_protein_pct",
            "wort_colour_EBC",
        ]
        print("\nPairwise differences among top 5:")
        for i in range(len(top5)):
            for j in range(i + 1, len(top5)):
                score_diff = abs(float(top5[i]["score"]) - float(top5[j]["score"]))
                param_diff = {
                    k: round(
                        abs(
                            float(top5[i]["blended_params"].get(k, 0.0))
                            - float(top5[j]["blended_params"].get(k, 0.0))
                        ),
                        4,
                    )
                    for k in keys
                }
                print(f"{i+1} vs {j+1}: score_diff={score_diff:.6f}, param_diff={param_diff}")

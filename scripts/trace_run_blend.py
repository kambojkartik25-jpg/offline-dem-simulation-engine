from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dem_sim.io import load_inputs
from dem_sim.service import RunConfig, run_blend
import dem_sim.model as model


_orig_simulate_for_sigma = model._simulate_for_sigma


def _simulate_for_sigma_traced(
    silo,
    intervals_df: pd.DataFrame,
    total_height_m: float,
    discharge_mass_kg: float,
    m_dot_kg_s: float,
    material,
    sigma_m: float,
    steps: int,
    moisture_beta: float = 0.0,
    sigma_alpha: float = 0.0,
    skew_alpha: float = 0.0,
    layer_moisture=None,
) -> pd.DataFrame:
    layer_masses = intervals_df["segment_mass_kg"].astype(float).to_list()
    print("\n_simulate_for_sigma input")
    print(f"  silo_id: {silo.silo_id}")
    print(f"  discharge_mass_kg: {discharge_mass_kg}")
    print(f"  m_dot_kg_s: {m_dot_kg_s}")
    print(f"  sigma_m: {sigma_m}")
    print(f"  steps: {steps}")
    print(f"  layer_masses: {layer_masses}")

    out = _orig_simulate_for_sigma(
        silo=silo,
        intervals_df=intervals_df,
        total_height_m=total_height_m,
        discharge_mass_kg=discharge_mass_kg,
        m_dot_kg_s=m_dot_kg_s,
        material=material,
        sigma_m=sigma_m,
        steps=steps,
        moisture_beta=moisture_beta,
        sigma_alpha=sigma_alpha,
        skew_alpha=skew_alpha,
        layer_moisture=layer_moisture,
    )

    print("_simulate_for_sigma output")
    cols = ["silo_id", "layer_index", "discharged_mass_kg"]
    print(out[cols].to_string(index=False))
    return out


model._simulate_for_sigma = _simulate_for_sigma_traced


def main() -> int:
    input_dir = sys.argv[1] if len(sys.argv) > 1 else "data/synthetic"
    inputs = load_inputs(input_dir)

    # Filter to silos with exactly 4 layers (required by RF model).
    counts = inputs["layers"].groupby("silo_id").size()
    valid_silos = counts[counts == 4].index.astype(str)
    skipped = counts[counts != 4].index.astype(str)
    if len(skipped) > 0:
        print(f"Skipping silos without 4 layers: {list(skipped)}")

    if len(valid_silos) == 0:
        raise SystemExit("No silos with exactly 4 layers found.")

    for key in ["silos", "layers", "discharge"]:
        df = inputs[key]
        inputs[key] = df[df["silo_id"].astype(str).isin(valid_silos)].copy()

    cfg = RunConfig()
    result = run_blend(inputs, cfg)

    print("\nrun_blend summary")
    print(f"  silos: {len(valid_silos)}")
    print(f"  keys: {list(result.keys())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

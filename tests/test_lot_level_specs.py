from __future__ import annotations

import pandas as pd

from dem_sim.model import BeverlooParams, Material, run_multi_silo_blend


def test_run_multi_silo_blend_prefers_lot_level_specs_over_supplier_lookup() -> None:
    silos = pd.DataFrame(
        [
            {"silo_id": "S1", "capacity_kg": 8000.0, "body_diameter_m": 3.0, "outlet_diameter_m": 0.2},
            {"silo_id": "S2", "capacity_kg": 8000.0, "body_diameter_m": 3.1, "outlet_diameter_m": 0.2},
            {"silo_id": "S3", "capacity_kg": 8000.0, "body_diameter_m": 3.2, "outlet_diameter_m": 0.21},
        ]
    )
    layers = pd.DataFrame(
        [
            {"silo_id": "S1", "layer_index": 1, "lot_id": "L1", "supplier": "X", "segment_mass_kg": 2000.0, "moisture_pct": 4.0},
            {"silo_id": "S2", "layer_index": 1, "lot_id": "L2", "supplier": "Y", "segment_mass_kg": 2000.0, "moisture_pct": 5.0},
            {"silo_id": "S3", "layer_index": 1, "lot_id": "L3", "supplier": "Z", "segment_mass_kg": 2000.0, "moisture_pct": 6.0},
        ]
    )
    # Intentionally contradictory supplier table; run should still use lot-level moisture_pct.
    suppliers = pd.DataFrame(
        [
            {"supplier": "X", "moisture_pct": 20.0},
            {"supplier": "Y", "moisture_pct": 20.0},
            {"supplier": "Z", "moisture_pct": 20.0},
        ]
    )
    discharge = pd.DataFrame(
        [
            {"silo_id": "S1", "discharge_mass_kg": 1000.0},
            {"silo_id": "S2", "discharge_mass_kg": 1000.0},
            {"silo_id": "S3", "discharge_mass_kg": 1000.0},
        ]
    )
    out = run_multi_silo_blend(
        df_silos=silos,
        df_layers=layers,
        df_suppliers=suppliers,
        df_discharge=discharge,
        material=Material(rho_bulk_kg_m3=610.0, grain_diameter_m=0.004),
        bev=BeverlooParams(C=0.58, k=1.4, g_m_s2=9.81),
        sigma_m=0.12,
        steps=100,
        auto_adjust=False,
    )
    # Equal discharged mass from each silo -> average(4,5,6)=5.0 if lot-level path is used.
    assert abs(float(out["total_blended_params"]["moisture_pct"]) - 5.0) < 1e-6

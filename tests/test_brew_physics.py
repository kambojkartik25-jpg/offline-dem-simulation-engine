from __future__ import annotations

import pandas as pd

from dem_sim.brew_physics import (
    beverloo_intermittent,
    janssen_effective_density,
    layer_discharge_composition,
    moisture_flow_factor,
)
from dem_sim.service import RunConfig, run_blend


def test_janssen_reduces_density_for_tall_silo() -> None:
    rho_bulk = 610.0
    rho_eff_tall = janssen_effective_density(
        rho_bulk=rho_bulk,
        fill_height=6.0,
        silo_radius=1.5,
    )
    rho_eff_empty = janssen_effective_density(
        rho_bulk=rho_bulk,
        fill_height=0.0,
        silo_radius=1.5,
    )
    assert rho_eff_tall < rho_bulk
    assert rho_eff_empty == rho_bulk


def test_moisture_factor_is_one_below_threshold() -> None:
    assert moisture_flow_factor(10.0) == 1.0
    assert moisture_flow_factor(15.0) < 1.0
    assert moisture_flow_factor(15.0) >= 0.5


def test_beverloo_intermittent_returns_valid_quantiles() -> None:
    out = beverloo_intermittent(q_beverloo=100.0)
    assert out.p10_rate_kg_s <= out.mean_rate_kg_s <= out.p90_rate_kg_s
    assert out.std_rate_kg_s > 0.0
    assert out.mean_rate_kg_s > 0.0


def test_layer_composition_fifo_order() -> None:
    layers_df = pd.DataFrame(
        [
            {
                "silo_id": "S1",
                "layer_position": 1,
                "segment_mass_kg": 1000.0,
                "moisture_pct": 4.1,
                "total_protein_pct": 10.0,
            },
            {
                "silo_id": "S1",
                "layer_position": 2,
                "segment_mass_kg": 1000.0,
                "moisture_pct": 5.0,
                "total_protein_pct": 11.0,
            },
        ]
    )
    comp = layer_discharge_composition(
        layers_df=layers_df,
        silo_id="S1",
        discharged_mass_kg=1000.0,
        param_keys=["moisture_pct", "total_protein_pct"],
    )
    assert comp["moisture_pct"] == 4.1
    assert comp["total_protein_pct"] == 10.0


def test_run_blend_with_janssen_differs_from_without() -> None:
    inputs = {
        "silos": pd.DataFrame(
            [
                {
                    "silo_id": "S1",
                    "capacity_kg": 12000.0,
                    "body_diameter_m": 3.0,
                    "outlet_diameter_m": 0.2,
                },
                {
                    "silo_id": "S2",
                    "capacity_kg": 12000.0,
                    "body_diameter_m": 3.2,
                    "outlet_diameter_m": 0.2,
                },
                {
                    "silo_id": "S3",
                    "capacity_kg": 12000.0,
                    "body_diameter_m": 3.1,
                    "outlet_diameter_m": 0.21,
                },
            ]
        ),
        "layers": pd.DataFrame(
            [
                {"silo_id": "S1", "layer_index": 1, "lot_id": "L1", "supplier": "BBM", "segment_mass_kg": 2000.0, "moisture_pct": 4.2},
                {"silo_id": "S1", "layer_index": 2, "lot_id": "L2", "supplier": "COFCO", "segment_mass_kg": 2000.0, "moisture_pct": 4.4},
                {"silo_id": "S2", "layer_index": 1, "lot_id": "L3", "supplier": "Malteurop", "segment_mass_kg": 2000.0, "moisture_pct": 4.3},
                {"silo_id": "S2", "layer_index": 2, "lot_id": "L4", "supplier": "BBM", "segment_mass_kg": 2000.0, "moisture_pct": 4.2},
                {"silo_id": "S3", "layer_index": 1, "lot_id": "L5", "supplier": "COFCO", "segment_mass_kg": 2000.0, "moisture_pct": 4.4},
                {"silo_id": "S3", "layer_index": 2, "lot_id": "L6", "supplier": "Malteurop", "segment_mass_kg": 2000.0, "moisture_pct": 4.3},
            ]
        ),
        "suppliers": pd.DataFrame(
            [
                {"supplier": "BBM", "moisture_pct": 4.2, "fine_extract_db_pct": 82.0, "wort_pH": 5.98, "diastatic_power_WK": 342.1, "total_protein_pct": 10.12, "wort_colour_EBC": 3.8},
                {"supplier": "COFCO", "moisture_pct": 4.4, "fine_extract_db_pct": 81.8, "wort_pH": 5.93, "diastatic_power_WK": 317.4, "total_protein_pct": 11.1, "wort_colour_EBC": 4.0},
                {"supplier": "Malteurop", "moisture_pct": 4.3, "fine_extract_db_pct": 81.2, "wort_pH": 5.97, "diastatic_power_WK": 336.9, "total_protein_pct": 10.5, "wort_colour_EBC": 3.8},
            ]
        ),
        "discharge": pd.DataFrame(
            [
                {"silo_id": "S1", "discharge_mass_kg": 1200.0},
                {"silo_id": "S2", "discharge_mass_kg": 1200.0},
                {"silo_id": "S3", "discharge_mass_kg": 1200.0},
            ]
        ),
    }
    with_janssen = run_blend(
        inputs,
        RunConfig(use_janssen=True, use_moisture_correction=False, use_layer_composition=False),
    )
    without_janssen = run_blend(
        inputs,
        RunConfig(use_janssen=False, use_moisture_correction=False, use_layer_composition=False),
    )
    flow_with = sum(
        float(r.get("mass_flow_rate_kg_s", 0.0))
        for r in with_janssen.get("per_silo", {}).values()
    )
    flow_without = sum(
        float(r.get("mass_flow_rate_kg_s", 0.0))
        for r in without_janssen.get("per_silo", {}).values()
    )
    assert flow_with != flow_without

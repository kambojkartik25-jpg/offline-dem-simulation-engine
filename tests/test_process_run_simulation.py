from __future__ import annotations

from fastapi.testclient import TestClient

from dem_sim.state import reset_state, set_state
from dem_sim.web import create_app


def _base_silos() -> list[dict]:
    return [
        {"silo_id": "S1", "capacity_kg": 8000.0, "body_diameter_m": 3.0, "outlet_diameter_m": 0.2},
        {"silo_id": "S2", "capacity_kg": 8000.0, "body_diameter_m": 3.2, "outlet_diameter_m": 0.2},
        {"silo_id": "S3", "capacity_kg": 8000.0, "body_diameter_m": 3.1, "outlet_diameter_m": 0.21},
    ]


def _suppliers() -> list[dict]:
    return [
        {"supplier": "BBM", "moisture_pct": 4.2, "fine_extract_db_pct": 82.0, "wort_pH": 5.98, "diastatic_power_WK": 342.1, "total_protein_pct": 10.12, "wort_colour_EBC": 3.8},
        {"supplier": "COFCO", "moisture_pct": 4.4, "fine_extract_db_pct": 81.8, "wort_pH": 5.93, "diastatic_power_WK": 317.4, "total_protein_pct": 11.1, "wort_colour_EBC": 4.0},
        {"supplier": "Malteurop", "moisture_pct": 4.3, "fine_extract_db_pct": 81.2, "wort_pH": 5.97, "diastatic_power_WK": 336.9, "total_protein_pct": 10.5, "wort_colour_EBC": 3.8},
    ]


def _lots(n: int, start: int = 1) -> list[dict]:
    out = []
    sup = ["BBM", "COFCO", "Malteurop"]
    for i in range(start, start + n):
        out.append({"lot_id": f"LOT{i:03d}", "supplier": sup[(i - 1) % 3], "mass_kg": 2000.0})
    return out


def test_run_simulation_fills_only_and_leaves_queue() -> None:
    reset_state()
    set_state(silos=_base_silos(), layers=[], suppliers=_suppliers(), incoming_queue=_lots(15))
    client = TestClient(create_app())
    res = client.post("/api/process/run_simulation", json={})
    assert res.status_code == 200
    data = res.json()
    summary = data["summary"]
    assert summary["incoming_queue"]["count"] == 3
    for s in summary["silos"]:
        assert abs(float(s["used_kg"]) - 8000.0) < 1e-9
        assert abs(float(s["remaining_kg"])) < 1e-9
    assert abs(float(summary["cumulative_discharged_kg"])) < 1e-9


def test_second_fill_only_uses_remaining_capacity_no_discharge() -> None:
    reset_state()
    # Pre-fill each silo with 3 lots (6000kg), keep queue with 6 lots.
    layers = []
    for sid in ["S1", "S2", "S3"]:
        for idx in range(1, 4):
            layers.append(
                {"silo_id": sid, "layer_index": idx, "lot_id": f"{sid}_L{idx}", "supplier": "BBM", "segment_mass_kg": 2000.0}
            )
    set_state(silos=_base_silos(), layers=layers, suppliers=_suppliers(), incoming_queue=_lots(6, 50))
    client = TestClient(create_app())
    res = client.post("/api/process/run_simulation", json={})
    assert res.status_code == 200
    summary = res.json()["summary"]
    # Exactly one lot per silo can be added => 3 queued remain.
    assert summary["incoming_queue"]["count"] == 3
    for s in summary["silos"]:
        assert abs(float(s["used_kg"]) - 8000.0) < 1e-9
    assert abs(float(summary["cumulative_discharged_kg"])) < 1e-9


def test_only_apply_discharge_changes_cumulative_discharged() -> None:
    reset_state()
    set_state(silos=_base_silos(), layers=[], suppliers=_suppliers(), incoming_queue=_lots(12))
    client = TestClient(create_app())
    fill = client.post("/api/process/run_simulation", json={})
    assert fill.status_code == 200
    assert float(fill.json()["summary"]["cumulative_discharged_kg"]) == 0.0
    apply_res = client.post(
        "/api/process/apply_discharge",
        json={
            "discharge": [
                {"silo_id": "S1", "discharge_mass_kg": 1000.0},
                {"silo_id": "S2", "discharge_mass_kg": 1000.0},
                {"silo_id": "S3", "discharge_mass_kg": 1000.0},
            ],
            "config": {},
        },
    )
    assert apply_res.status_code == 200
    assert float(apply_res.json()["summary"]["cumulative_discharged_kg"]) >= 3000.0

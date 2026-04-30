from __future__ import annotations

import math

from fastapi.testclient import TestClient

from dem_sim.db import fetchall
from dem_sim.web import _json_sanitize, _visible_blended_params, create_app


def test_health_endpoint() -> None:
    client = TestClient(create_app())
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_sample_and_run_endpoint() -> None:
    client = TestClient(create_app())
    sample = client.get("/api/sample")
    assert sample.status_code == 200
    payload = sample.json()

    validate = client.post("/api/validate", json=payload)
    assert validate.status_code == 200
    assert validate.json()["valid"] is True

    run = client.post("/api/run", json=payload)
    assert run.status_code == 200
    data = run.json()
    assert data["total_discharged_mass_kg"] > 0


def test_optimize_endpoint() -> None:
    client = TestClient(create_app())
    payload = client.get("/api/sample").json()
    payload["target_params"] = {
        "moisture_pct": 4.5,
        "fine_extract_db_pct": 81.8,
    }
    payload["iterations"] = 5
    payload["seed"] = 7

    res = client.post("/api/optimize", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert "recommended_discharge" in data
    assert data["objective_method"] == "normalized_weighted_l2_hybrid_search"
    assert len(data["top_candidates"]) >= 1
    assert data["best_run"]["total_discharged_mass_kg"] > 0


def test_optimize_payload_sanitizes_nan_values_for_json() -> None:
    params = _visible_blended_params(
        {
            "moisture_pct": math.nan,
            "fine_extract_db_pct": 81.5,
            "viscosity_mpas": math.nan,
        }
    )
    assert params == {
        "moisture_pct": None,
        "fine_extract_db_pct": 81.5,
    }

    payload = _json_sanitize(
        {
            "best_run": {
                "per_silo": {
                    "S1": {
                        "blended_params_per_silo": params,
                    }
                }
            }
        }
    )
    assert payload["best_run"]["per_silo"]["S1"]["blended_params_per_silo"]["moisture_pct"] is None


def test_production_plan_apply_tracks_reason_and_scenarios() -> None:
    client = TestClient(create_app())
    load_res = client.post(
        "/api/production-plans/load",
        json={
            "plan_run_id": "test_plan_apply_tracks_reason",
            "seed": 21,
            "silos_count": 3,
            "lots_count": 30,
            "lot_size_kg": 25000.0,
            "schedule_id": "test_schedule_apply_tracks_reason",
            "name": "Tracked Schedule",
            "brews_count": 2,
            "target_params": {
                "moisture_pct": 4.5,
                "fine_extract_db_pct": 81.0,
            },
            "optimize_iterations": 5,
            "optimize_seed": 7,
            "config": {"steps": 200},
            "include_all_candidates": False,
        },
    )
    assert load_res.status_code == 200
    snapshot = load_res.json()
    current_brew = snapshot["current_brew"]
    assert current_brew["brew_id"]
    assert len(snapshot["scenarios"]) >= 1

    missing_reason = client.post(
        f"/api/production-plans/{snapshot['plan_run']['plan_run_id']}/brews/{current_brew['brew_id']}/apply",
        json={
            "candidate_index": 0,
            "config": {"steps": 200},
            "expected_last_event_id": snapshot["plan_run"]["last_event_id"],
        },
    )
    assert missing_reason.status_code == 422

    apply_res = client.post(
        f"/api/production-plans/{snapshot['plan_run']['plan_run_id']}/brews/{current_brew['brew_id']}/apply",
        json={
            "candidate_index": 0,
            "reason": "operator selected best fit",
            "config": {"steps": 200},
            "expected_last_event_id": snapshot["plan_run"]["last_event_id"],
        },
    )
    assert apply_res.status_code == 200

    schedule_rows = fetchall(
        "SELECT schedule_id, name FROM brew_schedules WHERE schedule_id = %s",
        ("test_schedule_apply_tracks_reason",),
    )
    assert schedule_rows

    brew_rows = fetchall(
        """
        SELECT status, selected_candidate_index, applied_event_id
        FROM brew_schedule_items
        WHERE schedule_id = %s AND brew_id = %s
        """,
        ("test_schedule_apply_tracks_reason", current_brew["brew_id"]),
    )
    assert brew_rows
    assert brew_rows[0]["status"] == "applied"
    assert int(brew_rows[0]["selected_candidate_index"]) == 0
    assert brew_rows[0]["applied_event_id"] is not None

    plan_run_rows = fetchall(
        """
        SELECT plan_run_id, schedule_id, last_event_id
        FROM production_plan_runs
        WHERE plan_run_id = %s
        """,
        ("test_plan_apply_tracks_reason",),
    )
    assert plan_run_rows
    assert plan_run_rows[0]["schedule_id"] == "test_schedule_apply_tracks_reason"
    assert plan_run_rows[0]["last_event_id"] is not None

    apply_event_rows = fetchall(
        """
        SELECT id, event_type, plan_run_id, meta
        FROM sim_events
        WHERE plan_run_id = %s AND event_type = 'apply_discharge'
        ORDER BY id DESC
        LIMIT 1
        """,
        ("test_plan_apply_tracks_reason",),
    )
    assert apply_event_rows
    assert apply_event_rows[0]["event_type"] == "apply_discharge"
    assert apply_event_rows[0]["plan_run_id"] == "test_plan_apply_tracks_reason"

from __future__ import annotations

from fastapi.testclient import TestClient

from dem_sim.db import get_conn
from dem_sim.web import create_app


def main() -> None:
    app = create_app()
    client = TestClient(app)

    state_resp = client.get("/api/state")
    state_resp.raise_for_status()
    state = state_resp.json()["state"]

    run_resp = client.post(
        "/api/process/run_simulation",
        json={
            "silos": state.get("silos", []),
            "layers": state.get("layers", []),
            "suppliers": state.get("suppliers", []),
            "incoming_queue": state.get("incoming_queue", []),
        },
    )
    run_resp.raise_for_status()

    opt_resp = client.post(
        "/api/process/optimize",
        json={
            "target_params": {
                "moisture_pct": 4.3,
                "fine_extract_db_pct": 81.8,
                "wort_pH": 5.95,
                "diastatic_power_WK": 330.0,
                "total_protein_pct": 10.6,
                "wort_colour_EBC": 3.9,
            },
            "iterations": 20,
            "seed": 42,
            "config": {},
        },
    )
    opt_resp.raise_for_status()
    rec = opt_resp.json().get("recommended_discharge", [])

    discharge_resp = client.post(
        "/api/process/apply_discharge",
        json={"discharge": rec, "config": {}},
    )
    discharge_resp.raise_for_status()

    with get_conn() as conn:
        layers_count = conn.execute("SELECT COUNT(*) AS n FROM layers").fetchone()["n"]
        queue_count = conn.execute("SELECT COUNT(*) AS n FROM incoming_queue").fetchone()["n"]
        discharged_sum = conn.execute(
            "SELECT COALESCE(SUM(discharged_kg), 0) AS s FROM lot_ledger"
        ).fetchone()["s"]
        run_results = conn.execute("SELECT COUNT(*) AS n FROM results_run").fetchone()["n"]
        opt_results = conn.execute("SELECT COUNT(*) AS n FROM results_optimize").fetchone()["n"]

    print("layers_count=", layers_count)
    print("incoming_queue_count=", queue_count)
    print("lot_ledger_discharged_sum=", float(discharged_sum))
    print("results_run_rows=", run_results)
    print("results_optimize_rows=", opt_results)


if __name__ == "__main__":
    main()

"""Shared test infrastructure for silo lifecycle DB tests.

Requires:  DEM_SIM_TEST_DATABASE_URL=postgresql://user:pass@host/dbname
           (tests are auto-skipped when the env var is absent)

The SiloLifecycle class drives charge/discharge cycles and asserts all seven
physical invariants against BOTH the database AND the in-memory state after
every single operation.

Seven Invariants
----------------
INV-1  Mass conservation   : db_remaining + cumulative_discharged == total_charged  (±0.01 kg)
INV-2  Layer index integrity: per silo, layer_index is contiguous 1..N, no gaps/duplicates
INV-3  Supplier consistency : a lot_id always maps to the same supplier — never changes
INV-4  No negative mass    : every layer.loaded_mass >= 0
INV-5  Capacity never exceeded: SUM(loaded_mass per silo) <= capacity_kg
INV-6  Lot mass accounting : silo_mass + queue_mass <= original_charged_mass per lot
INV-7  DB matches memory   : db total per silo == in-memory total per silo  (±0.01 kg)
"""
from __future__ import annotations

import json
import os
from copy import deepcopy
from typing import Any
from unittest.mock import patch

import pandas as pd
import psycopg
import pytest
from psycopg.rows import dict_row

from dem_sim.charger import allocate_lots_append_to_existing
from dem_sim.service import RunConfig, run_blend
from dem_sim.state import apply_discharge_to_state, get_state, reset_state, set_state
from dem_sim.storage import PostgresStorage


# ---------------------------------------------------------------------------
# Session fixture — DB URL
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def lifecycle_db_url() -> str:
    url = os.getenv("DEM_SIM_TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("DEM_SIM_TEST_DATABASE_URL not set — skipping lifecycle DB tests")
    return url


# ---------------------------------------------------------------------------
# Function fixture — fresh schema per test
# ---------------------------------------------------------------------------

@pytest.fixture()
def fresh_db(lifecycle_db_url: str) -> str:
    """Drop and recreate the entire public schema before each lifecycle test.

    Also resets the in-memory STATE so tests never pollute each other.
    """
    from dem_sim.schema import ensure_schema

    # DDL requires autocommit — cannot run inside a transaction block.
    wipe_conn = psycopg.connect(lifecycle_db_url, autocommit=True)
    try:
        wipe_conn.execute("DROP SCHEMA public CASCADE")
        wipe_conn.execute("CREATE SCHEMA public")
    finally:
        wipe_conn.close()

    # Operational tables (13 tables created by ensure_schema).
    with patch.dict(os.environ, {"DEM_SIM_DATABASE_URL": lifecycle_db_url}):
        ensure_schema()

    # ORM tables (sim_snapshots, sim_stages, sim_history, sim_results).
    PostgresStorage(dsn=lifecycle_db_url).ensure_schema()

    # Clear the global in-memory STATE dict.
    reset_state()

    yield lifecycle_db_url
    # No teardown — next test's fresh_db will wipe everything.


# ---------------------------------------------------------------------------
# SiloLifecycle — the lifecycle driver and invariant checker
# ---------------------------------------------------------------------------

class SiloLifecycle:
    """Drives a silo system through charge/discharge cycles.

    Usage::

        lc = SiloLifecycle(db_url, silos, suppliers)
        lc.setup()
        lc.charge([{"lot_id": "L1", "supplier": "BBM", "mass_kg": 4000}])
        lc.assert_invariants("after charge 1")
        lc.discharge({"S1": 0.30, "S2": 0.30, "S3": 0.30})
        lc.assert_invariants("after discharge 1")
    """

    def __init__(self, db_url: str, silos: list[dict], suppliers: list[dict]) -> None:
        self.db_url = db_url
        self.silos_cfg = deepcopy(silos)
        self.suppliers_cfg = deepcopy(suppliers)

        # Running totals for INV-1
        self.total_charged_kg: float = 0.0
        self.cumulative_discharged_kg: float = 0.0

        # Tracking maps for INV-3 and INV-6
        self._lot_supplier_map: dict[str, str] = {}
        self._lot_original_mass: dict[str, float] = {}

        # Current snapshot_id in the layers table (INV-1 reads from this)
        self._current_snapshot_id: int = 0

    # ── Internal helpers ───────────────────────────────────────────────────

    def _conn(self) -> psycopg.Connection:
        return psycopg.connect(self.db_url, row_factory=dict_row)

    # ── Setup ──────────────────────────────────────────────────────────────

    def setup(self) -> None:
        """Insert silos + suppliers into DB; reset in-memory STATE."""
        with self._conn() as conn:
            with conn.transaction():
                for s in self.silos_cfg:
                    conn.execute(
                        """
                        INSERT INTO silos (silo_id, capacity_kg, body_diameter_m, outlet_diameter_m)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (silo_id) DO NOTHING
                        """,
                        (s["silo_id"], s["capacity_kg"], s["body_diameter_m"], s["outlet_diameter_m"]),
                    )
                for sup in self.suppliers_cfg:
                    conn.execute(
                        """
                        INSERT INTO suppliers (name, moisture_pct, wort_pH, total_protein_pct, wort_colour_EBC)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (name) DO NOTHING
                        """,
                        (
                            sup["supplier"],
                            sup.get("moisture_pct", 4.0),
                            sup.get("wort_pH", 5.6),
                            sup.get("total_protein_pct", 11.5),
                            sup.get("wort_colour_EBC", 3.5),
                        ),
                    )

        reset_state()
        set_state(
            silos=self.silos_cfg,
            suppliers=self.suppliers_cfg,
            layers=[],
            incoming_queue=[],
            action="setup",
        )

    # ── Charge ─────────────────────────────────────────────────────────────

    def charge(self, lots: list[dict]) -> dict:
        """Allocate lots to silos, update in-memory STATE, persist snapshot to DB.

        Returns allocation result (new_layers, queued, warnings).
        """
        current = get_state()

        alloc = allocate_lots_append_to_existing(
            lots=lots,
            silos=self.silos_cfg,
            existing_layers=current["layers"],
        )

        # Track original lot mass (for INV-6) and supplier (for INV-3).
        for lot in lots:
            lot_id = str(lot.get("lot_id", ""))
            supplier = str(lot.get("supplier", ""))
            mass = float(lot.get("mass_kg", 0.0))
            if lot_id:
                # INV-3: supplier must never change for a given lot_id.
                if lot_id in self._lot_supplier_map:
                    assert self._lot_supplier_map[lot_id] == supplier, (
                        f"Supplier changed for lot {lot_id}: "
                        f"was {self._lot_supplier_map[lot_id]}, now {supplier}"
                    )
                self._lot_supplier_map[lot_id] = supplier
                self._lot_original_mass[lot_id] = (
                    self._lot_original_mass.get(lot_id, 0.0) + mass
                )

        # Only count mass actually placed in silos (not overflow queued).
        charged = sum(float(l.get("segment_mass_kg", 0.0)) for l in alloc["new_layers"])
        self.total_charged_kg += charged

        # Update in-memory state.
        combined = current["layers"] + alloc["new_layers"]
        set_state(
            layers=combined,
            incoming_queue=alloc["incoming_queue"],
            action="charge",
        )

        self._persist_layers(event_type="charge")
        return {
            "new_layers": alloc["new_layers"],
            "queued": alloc["incoming_queue"],
            "warnings": alloc.get("warnings", []),
        }

    # ── Discharge ──────────────────────────────────────────────────────────

    def discharge(self, fractions_by_silo: dict[str, float]) -> dict:
        """Simulate discharge, apply FIFO mass removal to STATE, persist to DB.

        fractions_by_silo: {silo_id: fraction_of_current_mass_to_discharge}
        Returns the full run_blend result dict.
        """
        current = get_state()
        layers = current["layers"]

        # Ensure segment_mass_kg reflects the current remaining mass.
        for layer in layers:
            if "remaining_mass_kg" in layer:
                layer["segment_mass_kg"] = layer["remaining_mass_kg"]

        layers_df = pd.DataFrame(layers)
        if layers_df.empty:
            raise ValueError("No layers in state — cannot discharge an empty silo system.")

        # Only include silos that have at least one layer with positive remaining mass.
        silos_with_mass = {
            str(l.get("silo_id", ""))
            for l in layers
            if float(l.get("remaining_mass_kg", l.get("segment_mass_kg", 0.0))) > 1e-6
        }
        discharge_rows = [
            {
                "silo_id": s["silo_id"],
                "discharge_fraction": float(fractions_by_silo.get(s["silo_id"], 0.0)),
                "discharge_mass_kg": None,
            }
            for s in self.silos_cfg
            if s["silo_id"] in silos_with_mass
        ]

        silos_with_mass_cfg = [s for s in self.silos_cfg if s["silo_id"] in silos_with_mass]
        inputs = {
            "silos": pd.DataFrame(silos_with_mass_cfg),
            "layers": layers_df,
            "suppliers": pd.DataFrame(self.suppliers_cfg),
            "discharge": pd.DataFrame(discharge_rows),
        }

        result = run_blend(inputs, RunConfig(auto_adjust=True))

        # Apply FIFO mass removal to in-memory STATE.
        discharge_by_silo = {
            sid: float(res["discharged_mass_kg"])
            for sid, res in result["per_silo"].items()
        }
        apply_discharge_to_state(discharge_by_silo)
        self.cumulative_discharged_kg += float(result["total_discharged_mass_kg"])

        self._persist_layers(event_type="discharge")
        self._write_results_run(result)
        return result

    # ── DB helpers ─────────────────────────────────────────────────────────

    def _persist_layers(self, event_type: str) -> None:
        """Write the current in-memory layer state as a new append-only snapshot."""
        state = get_state()
        with self._conn() as conn:
            with conn.transaction():
                row = conn.execute(
                    "SELECT COALESCE(MAX(snapshot_id), 0) AS m FROM layers"
                ).fetchone()
                snapshot_id = int(row["m"]) + 1
                self._current_snapshot_id = snapshot_id

                for layer in state["layers"]:
                    sid = str(layer.get("silo_id", ""))
                    mass = float(
                        layer.get("remaining_mass_kg", layer.get("segment_mass_kg", 0.0))
                    )
                    mass = max(mass, 0.0)
                    conn.execute(
                        """
                        INSERT INTO layers
                            (silo_id, snapshot_id, event_type, layer_index, lot_id, supplier, loaded_mass)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            sid,
                            snapshot_id,
                            event_type,
                            int(layer.get("layer_index", 0)),
                            str(layer.get("lot_id", "")),
                            str(layer.get("supplier", "")),
                            round(mass, 6),
                        ),
                    )

    def _write_results_run(self, result: dict) -> None:
        """Persist a discharge result to the results_run table."""
        blended = result.get("total_blended_params", {})
        blended_safe = {
            k: (float(v) if not (isinstance(v, float) and v != v) else None)
            for k, v in blended.items()
        }
        with self._conn() as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO results_run
                        (action, total_discharged_mass_kg, total_remaining_mass_kg,
                         total_blended_params, per_silo, silo_state_ledger)
                    VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
                    """,
                    (
                        "discharge",
                        float(result["total_discharged_mass_kg"]),
                        float(result["total_remaining_mass_kg"]),
                        json.dumps(blended_safe),
                        json.dumps({}),
                        json.dumps([]),
                    ),
                )

    # ── Invariant checker ──────────────────────────────────────────────────

    def assert_invariants(self, label: str = "") -> None:
        """Assert all 7 physical invariants against both DB and in-memory state.

        Raises AssertionError with a descriptive message on the first failure.
        label is prepended to every error message for easy identification.
        """
        state = get_state()
        snap_id = self._current_snapshot_id
        pfx = f"[{label}] " if label else ""

        with self._conn() as conn:

            # ── INV-1: Mass conservation ───────────────────────────────────
            db_mass = float(
                conn.execute(
                    "SELECT COALESCE(SUM(loaded_mass), 0.0) AS t FROM layers WHERE snapshot_id = %s",
                    (snap_id,),
                ).fetchone()["t"]
            )
            balance = db_mass + self.cumulative_discharged_kg
            assert abs(balance - self.total_charged_kg) < 0.01, (
                f"{pfx}INV-1 MASS CONSERVATION: "
                f"db_remaining={db_mass:.4f} + discharged={self.cumulative_discharged_kg:.4f}"
                f" = {balance:.4f} ≠ charged={self.total_charged_kg:.4f}"
                f" (drift={abs(balance - self.total_charged_kg):.6f} kg)"
            )

            # ── INV-2: Layer index contiguity per silo ─────────────────────
            for silo in self.silos_cfg:
                sid = silo["silo_id"]
                rows = conn.execute(
                    "SELECT layer_index FROM layers WHERE silo_id = %s AND snapshot_id = %s"
                    " ORDER BY layer_index",
                    (sid, snap_id),
                ).fetchall()
                indexes = [r["layer_index"] for r in rows]
                expected = list(range(1, len(indexes) + 1))
                assert indexes == expected, (
                    f"{pfx}INV-2 LAYER INDEX INTEGRITY silo={sid}: "
                    f"got {indexes}, expected {expected}"
                )

            # ── INV-3: Supplier consistency across all historical snapshots ─
            conflicts = conn.execute(
                """
                SELECT lot_id, COUNT(DISTINCT supplier) AS cnt
                FROM layers
                WHERE lot_id != ''
                GROUP BY lot_id
                HAVING COUNT(DISTINCT supplier) > 1
                """
            ).fetchall()
            assert len(conflicts) == 0, (
                f"{pfx}INV-3 SUPPLIER CONSISTENCY: "
                f"lot_ids with multiple suppliers: {[r['lot_id'] for r in conflicts]}"
            )

            # ── INV-4: No negative mass ────────────────────────────────────
            neg = conn.execute(
                "SELECT COUNT(*) AS cnt FROM layers WHERE loaded_mass < -1e-9"
            ).fetchone()["cnt"]
            assert neg == 0, (
                f"{pfx}INV-4 NO NEGATIVE MASS: {neg} layer(s) with loaded_mass < 0"
            )

            # ── INV-5: Capacity never exceeded ─────────────────────────────
            for silo in self.silos_cfg:
                sid = silo["silo_id"]
                used = float(
                    conn.execute(
                        "SELECT COALESCE(SUM(loaded_mass), 0.0) AS t"
                        " FROM layers WHERE silo_id = %s AND snapshot_id = %s",
                        (sid, snap_id),
                    ).fetchone()["t"]
                )
                cap = float(silo["capacity_kg"])
                assert used <= cap + 0.01, (
                    f"{pfx}INV-5 CAPACITY EXCEEDED silo={sid}: "
                    f"used={used:.4f} > capacity={cap:.4f}"
                )

            # ── INV-6: Lot mass accounting — no mass created from thin air ─
            queue = state.get("incoming_queue", [])
            queue_by_lot: dict[str, float] = {}
            for q in queue:
                lid = str(q.get("lot_id", ""))
                if lid:
                    queue_by_lot[lid] = queue_by_lot.get(lid, 0.0) + float(q.get("mass_kg", 0.0))

            silo_rows = conn.execute(
                "SELECT lot_id, COALESCE(SUM(loaded_mass), 0.0) AS t"
                " FROM layers WHERE snapshot_id = %s AND lot_id != ''"
                " GROUP BY lot_id",
                (snap_id,),
            ).fetchall()
            silo_by_lot: dict[str, float] = {r["lot_id"]: float(r["t"]) for r in silo_rows}

            for lot_id, original in self._lot_original_mass.items():
                in_silos = silo_by_lot.get(lot_id, 0.0)
                in_queue = queue_by_lot.get(lot_id, 0.0)
                assert in_silos + in_queue <= original + 0.01, (
                    f"{pfx}INV-6 LOT MASS lot={lot_id}: "
                    f"silos({in_silos:.4f}) + queue({in_queue:.4f})"
                    f" = {in_silos+in_queue:.4f} > original({original:.4f})"
                )

            # ── INV-7: DB snapshot matches in-memory state ─────────────────
            for silo in self.silos_cfg:
                sid = silo["silo_id"]
                db_total = float(
                    conn.execute(
                        "SELECT COALESCE(SUM(loaded_mass), 0.0) AS t"
                        " FROM layers WHERE silo_id = %s AND snapshot_id = %s",
                        (sid, snap_id),
                    ).fetchone()["t"]
                )
                mem_total = sum(
                    float(l.get("remaining_mass_kg", l.get("segment_mass_kg", 0.0)))
                    for l in state["layers"]
                    if str(l.get("silo_id", "")) == sid
                )
                assert abs(db_total - mem_total) < 0.01, (
                    f"{pfx}INV-7 DB vs MEMORY silo={sid}: "
                    f"db={db_total:.4f} ≠ memory={mem_total:.4f}"
                )


# ---------------------------------------------------------------------------
# Fixture factory
# ---------------------------------------------------------------------------

@pytest.fixture()
def lifecycle(fresh_db: str):
    """Provide a SiloLifecycle factory bound to the test DB.

    Usage in tests::

        def test_something(lifecycle):
            lc = lifecycle(SILOS_3, SUPPLIERS_3)
            lc.charge(...)
            lc.assert_invariants("after charge")
    """
    def _make(silos: list[dict], suppliers: list[dict]) -> SiloLifecycle:
        lc = SiloLifecycle(db_url=fresh_db, silos=silos, suppliers=suppliers)
        lc.setup()
        return lc
    return _make

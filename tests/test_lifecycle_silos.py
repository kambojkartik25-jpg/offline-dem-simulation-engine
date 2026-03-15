"""Silo lifecycle tests: charge, discharge, recharge, repeat.

Every test runs a complete operational scenario and asserts all 7 physical
invariants after EVERY single operation — not just at the end.

Run with a live PostgreSQL test database:
    DEM_SIM_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/dem_sim_test \
    pytest tests/test_lifecycle_silos.py -v

Tests are auto-skipped when DEM_SIM_TEST_DATABASE_URL is not set.
"""
from __future__ import annotations

import random

import pytest

# ---------------------------------------------------------------------------
# Standard fixture data
# ---------------------------------------------------------------------------

SILOS_3 = [
    {"silo_id": "S1", "capacity_kg": 8000.0, "body_diameter_m": 3.0, "outlet_diameter_m": 0.25},
    {"silo_id": "S2", "capacity_kg": 8000.0, "body_diameter_m": 3.0, "outlet_diameter_m": 0.25},
    {"silo_id": "S3", "capacity_kg": 8000.0, "body_diameter_m": 3.0, "outlet_diameter_m": 0.25},
]

SILOS_1 = [
    {"silo_id": "S1", "capacity_kg": 6000.0, "body_diameter_m": 3.0, "outlet_diameter_m": 0.25},
]

SILOS_2 = [
    {"silo_id": "S1", "capacity_kg": 5000.0, "body_diameter_m": 3.0, "outlet_diameter_m": 0.25},
    {"silo_id": "S2", "capacity_kg": 5000.0, "body_diameter_m": 3.0, "outlet_diameter_m": 0.25},
]

SUPPLIERS_3 = [
    {
        "supplier": "BBM",
        "moisture_pct": 4.2, "wort_pH": 5.6,
        "total_protein_pct": 11.5, "wort_colour_EBC": 3.5,
        "fine_extract_db_pct": 81.0, "diastatic_power_WK": 250.0,
    },
    {
        "supplier": "COFCO",
        "moisture_pct": 4.5, "wort_pH": 5.7,
        "total_protein_pct": 12.0, "wort_colour_EBC": 3.8,
        "fine_extract_db_pct": 80.5, "diastatic_power_WK": 240.0,
    },
    {
        "supplier": "Malteurop",
        "moisture_pct": 4.8, "wort_pH": 5.8,
        "total_protein_pct": 11.8, "wort_colour_EBC": 4.0,
        "fine_extract_db_pct": 80.0, "diastatic_power_WK": 230.0,
    },
]

ALL_3_FRACS = {"S1": 0.30, "S2": 0.30, "S3": 0.30}


# ---------------------------------------------------------------------------
# Scenario 1: Single charge → single discharge (baseline correctness)
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_single_charge_single_discharge(lifecycle):
    """Simplest possible lifecycle — establishes the baseline for all invariants."""
    lc = lifecycle(SILOS_3, SUPPLIERS_3)

    lc.charge([
        {"lot_id": "L001", "supplier": "BBM",       "mass_kg": 4000.0},
        {"lot_id": "L002", "supplier": "COFCO",     "mass_kg": 4000.0},
        {"lot_id": "L003", "supplier": "Malteurop", "mass_kg": 4000.0},
        {"lot_id": "L004", "supplier": "BBM",       "mass_kg": 4000.0},
        {"lot_id": "L005", "supplier": "COFCO",     "mass_kg": 4000.0},
        {"lot_id": "L006", "supplier": "Malteurop", "mass_kg": 4000.0},
    ])
    lc.assert_invariants("after C1")

    result = lc.discharge(ALL_3_FRACS)
    lc.assert_invariants("after D1")

    assert result["total_discharged_mass_kg"] > 0
    assert result["total_remaining_mass_kg"] > 0
    assert abs(
        result["total_discharged_mass_kg"] + result["total_remaining_mass_kg"]
        - lc.total_charged_kg
    ) < 0.01


# ---------------------------------------------------------------------------
# Scenario 2: Repeated discharges drain silos (mass conservation across N ops)
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_repeated_discharges_conserve_mass(lifecycle):
    """Four successive 20% discharges — mass conservation must hold at every step."""
    lc = lifecycle(SILOS_1, SUPPLIERS_3[:2])

    lc.charge([
        {"lot_id": "L001", "supplier": "BBM",   "mass_kg": 2000.0},
        {"lot_id": "L002", "supplier": "COFCO", "mass_kg": 2000.0},
        {"lot_id": "L003", "supplier": "BBM",   "mass_kg": 2000.0},
    ])
    lc.assert_invariants("after C1")

    for i in range(4):
        lc.discharge({"S1": 0.20})
        lc.assert_invariants(f"after D{i + 1}")

    # Total discharged must be less than total charged (silo not empty).
    assert lc.cumulative_discharged_kg < lc.total_charged_kg
    assert lc.cumulative_discharged_kg > 0


# ---------------------------------------------------------------------------
# Scenario 3: Charge → discharge → recharge → discharge (core brewery cycle)
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_charge_discharge_recharge_discharge(lifecycle):
    """The defining brewery pattern: partial discharge then new lots on top of remaining."""
    lc = lifecycle(SILOS_3, SUPPLIERS_3)

    # First fill.
    lc.charge([
        {"lot_id": "L001", "supplier": "BBM",       "mass_kg": 3500.0},
        {"lot_id": "L002", "supplier": "COFCO",     "mass_kg": 3500.0},
        {"lot_id": "L003", "supplier": "Malteurop", "mass_kg": 3500.0},
        {"lot_id": "L004", "supplier": "BBM",       "mass_kg": 3500.0},
    ])
    lc.assert_invariants("after C1")
    total_after_c1 = lc.total_charged_kg

    # Two discharges.
    lc.discharge(ALL_3_FRACS)
    lc.assert_invariants("after D1")
    lc.discharge(ALL_3_FRACS)
    lc.assert_invariants("after D2")
    discharged_before_recharge = lc.cumulative_discharged_kg

    # New lots on top of remaining material.
    charge2 = lc.charge([
        {"lot_id": "L005", "supplier": "COFCO",     "mass_kg": 2000.0},
        {"lot_id": "L006", "supplier": "Malteurop", "mass_kg": 2000.0},
        {"lot_id": "L007", "supplier": "BBM",       "mass_kg": 2000.0},
    ])
    lc.assert_invariants("after C2")

    # New lots must sit on TOP of existing layers (higher layer_index values).
    with lc._conn() as conn:
        snap = lc._current_snapshot_id
        for silo in SILOS_3:
            sid = silo["silo_id"]
            rows = conn.execute(
                "SELECT lot_id, layer_index FROM layers WHERE silo_id = %s AND snapshot_id = %s ORDER BY layer_index",
                (sid, snap),
            ).fetchall()
            if len(rows) < 2:
                continue
            # New lot_ids (L005-L007) must have higher layer_index than old ones.
            new_lot_ids = {"L005", "L006", "L007"}
            old_rows = [r for r in rows if r["lot_id"] not in new_lot_ids]
            new_rows = [r for r in rows if r["lot_id"] in new_lot_ids]
            if old_rows and new_rows:
                max_old_idx = max(r["layer_index"] for r in old_rows)
                min_new_idx = min(r["layer_index"] for r in new_rows)
                assert min_new_idx > max_old_idx, (
                    f"Silo {sid}: new lots have layer_index {min_new_idx} "
                    f"≤ old max {max_old_idx} — new lots not on top!"
                )

    # Two more discharges after recharge (mixes old and new lots).
    lc.discharge(ALL_3_FRACS)
    lc.assert_invariants("after D3")
    lc.discharge(ALL_3_FRACS)
    lc.assert_invariants("after D4")

    # Total charged must be cumulative from both charge events.
    assert lc.total_charged_kg > total_after_c1
    assert lc.cumulative_discharged_kg > discharged_before_recharge


# ---------------------------------------------------------------------------
# Scenario 4: Lot split across silos when one silo fills up
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_lot_split_across_silos(lifecycle):
    """A lot too large for one silo splits across two — INV-3 must hold for both halves."""
    lc = lifecycle(SILOS_2, SUPPLIERS_3[:2])

    # Single lot of 8000 kg, two silos of 5000 kg each.
    # S1 gets 5000 kg (full), S2 gets the remaining 3000 kg.
    lc.charge([
        {"lot_id": "L001", "supplier": "BBM", "mass_kg": 8000.0},
    ])
    lc.assert_invariants("after C1 (split lot)")

    # Verify the lot appears in both silos with the same supplier.
    with lc._conn() as conn:
        snap = lc._current_snapshot_id
        rows = conn.execute(
            "SELECT silo_id, supplier, loaded_mass FROM layers WHERE lot_id = 'L001' AND snapshot_id = %s",
            (snap,),
        ).fetchall()
        assert len(rows) == 2, f"Expected L001 in 2 silos, found {len(rows)}"
        suppliers_found = {r["supplier"] for r in rows}
        assert suppliers_found == {"BBM"}, f"Supplier changed for split lot: {suppliers_found}"
        total_placed = sum(float(r["loaded_mass"]) for r in rows)
        assert abs(total_placed - 8000.0) < 0.01, (
            f"Split lot total {total_placed:.4f} ≠ 8000.0"
        )

    lc.discharge({"S1": 0.50, "S2": 0.50})
    lc.assert_invariants("after D1")

    # Second lot fills the freed space, also splits.
    lc.charge([
        {"lot_id": "L002", "supplier": "COFCO", "mass_kg": 6000.0},
    ])
    lc.assert_invariants("after C2")

    lc.discharge({"S1": 0.40, "S2": 0.40})
    lc.assert_invariants("after D2")


# ---------------------------------------------------------------------------
# Scenario 5: Capacity overflow — excess lots stay in queue, not in silos
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_capacity_overflow_queues_excess(lifecycle):
    """Charging beyond capacity must queue the overflow — silos must never exceed INV-5."""
    lc = lifecycle(SILOS_1, SUPPLIERS_3[:1])

    # Fill silo to exactly capacity.
    lc.charge([
        {"lot_id": "L001", "supplier": "BBM", "mass_kg": 3000.0},
        {"lot_id": "L002", "supplier": "BBM", "mass_kg": 3000.0},
    ])
    lc.assert_invariants("after C1 (silo full)")

    # Attempt to charge 4000 kg more into a 0-headroom silo.
    result2 = lc.charge([
        {"lot_id": "L003", "supplier": "BBM", "mass_kg": 4000.0},
    ])
    lc.assert_invariants("after C2 (overflow)")

    # The entire 4000 kg must be queued, nothing placed.
    assert len(result2["new_layers"]) == 0, "Overflow lot should not be placed in any silo"
    queued_mass = sum(float(q["mass_kg"]) for q in result2["queued"])
    assert abs(queued_mass - 4000.0) < 0.01, (
        f"Expected 4000 kg queued, got {queued_mass:.4f}"
    )
    # Capacity not exceeded.
    with lc._conn() as conn:
        snap = lc._current_snapshot_id
        used = float(conn.execute(
            "SELECT COALESCE(SUM(loaded_mass), 0) AS t FROM layers WHERE silo_id = 'S1' AND snapshot_id = %s",
            (snap,),
        ).fetchone()["t"])
        assert used <= 6000.0 + 0.01

    # Discharge makes room; queued lot can now be placed.
    lc.discharge({"S1": 0.40})
    lc.assert_invariants("after D1 (made room)")

    # Charge the queued lot from the incoming queue manually.
    from dem_sim.state import get_state
    state = get_state()
    queued = state.get("incoming_queue", [])
    if queued:
        lc.charge(queued)
        lc.assert_invariants("after C3 (queued lot placed)")


# ---------------------------------------------------------------------------
# Scenario 6: Many cycles — mass conservation under stress (20 charge/discharge cycles)
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_many_cycles_mass_conservation(lifecycle):
    """20 charge/discharge cycles — conservation must hold at every single step."""
    rng = random.Random(42)
    lc = lifecycle(SILOS_3, SUPPLIERS_3)

    suppliers = ["BBM", "COFCO", "Malteurop"]
    lot_counter = 1

    for cycle in range(20):
        # Charge 2-3 lots per cycle.
        n_lots = rng.randint(2, 3)
        lots = []
        for _ in range(n_lots):
            lots.append({
                "lot_id": f"L{lot_counter:04d}",
                "supplier": rng.choice(suppliers),
                "mass_kg": round(rng.uniform(500.0, 2500.0), 1),
            })
            lot_counter += 1
        lc.charge(lots)
        lc.assert_invariants(f"cycle={cycle + 1} after charge")

        # Two discharges per cycle with random fractions.
        for d in range(2):
            fracs = {
                "S1": round(rng.uniform(0.10, 0.40), 2),
                "S2": round(rng.uniform(0.10, 0.40), 2),
                "S3": round(rng.uniform(0.10, 0.40), 2),
            }
            lc.discharge(fracs)
            lc.assert_invariants(f"cycle={cycle + 1} discharge={d + 1}")

    # Final accounting.
    assert lc.total_charged_kg > 0
    assert lc.cumulative_discharged_kg > 0
    assert lc.cumulative_discharged_kg < lc.total_charged_kg  # something remains

    # DB: snapshot_ids must be monotonically increasing.
    with lc._conn() as conn:
        snap_ids = [
            r["snapshot_id"]
            for r in conn.execute(
                "SELECT DISTINCT snapshot_id FROM layers ORDER BY snapshot_id"
            ).fetchall()
        ]
        assert snap_ids == list(range(1, len(snap_ids) + 1)), (
            f"snapshot_ids not monotonically sequential: {snap_ids[:10]}..."
        )


# ---------------------------------------------------------------------------
# Scenario 7: Supplier consistency — lot_id always maps to same supplier
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_supplier_consistency_across_full_lifecycle(lifecycle):
    """INV-3 deep check: no lot_id ever appears with two different suppliers in any DB record."""
    lc = lifecycle(SILOS_3, SUPPLIERS_3)

    # Alternate suppliers across multiple charge events.
    lc.charge([
        {"lot_id": "L001", "supplier": "BBM",       "mass_kg": 3000.0},
        {"lot_id": "L002", "supplier": "COFCO",     "mass_kg": 3000.0},
        {"lot_id": "L003", "supplier": "Malteurop", "mass_kg": 3000.0},
    ])
    lc.assert_invariants("after C1")
    lc.discharge({"S1": 0.30, "S2": 0.30, "S3": 0.30})
    lc.assert_invariants("after D1")

    lc.charge([
        {"lot_id": "L004", "supplier": "BBM",       "mass_kg": 2000.0},
        {"lot_id": "L005", "supplier": "COFCO",     "mass_kg": 2000.0},
        {"lot_id": "L006", "supplier": "Malteurop", "mass_kg": 2000.0},
    ])
    lc.assert_invariants("after C2")
    lc.discharge({"S1": 0.25, "S2": 0.25, "S3": 0.25})
    lc.assert_invariants("after D2")

    lc.charge([
        {"lot_id": "L007", "supplier": "BBM",       "mass_kg": 1500.0},
        {"lot_id": "L008", "supplier": "Malteurop", "mass_kg": 1500.0},
    ])
    lc.assert_invariants("after C3")
    lc.discharge({"S1": 0.35, "S2": 0.35, "S3": 0.35})
    lc.assert_invariants("after D3")

    # Exhaustive DB check: every lot_id across ALL snapshots must have exactly one supplier.
    with lc._conn() as conn:
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
            f"Supplier drift detected in DB for lot_ids: {[r['lot_id'] for r in conflicts]}"
        )

        # Also verify expected supplier for each lot_id we know about.
        expected = {
            "L001": "BBM", "L002": "COFCO", "L003": "Malteurop",
            "L004": "BBM", "L005": "COFCO", "L006": "Malteurop",
            "L007": "BBM", "L008": "Malteurop",
        }
        for lot_id, expected_sup in expected.items():
            rows = conn.execute(
                "SELECT DISTINCT supplier FROM layers WHERE lot_id = %s",
                (lot_id,),
            ).fetchall()
            assert len(rows) == 1, f"lot_id={lot_id} has {len(rows)} distinct suppliers in DB"
            assert rows[0]["supplier"] == expected_sup, (
                f"lot_id={lot_id}: expected {expected_sup}, got {rows[0]['supplier']}"
            )


# ---------------------------------------------------------------------------
# Scenario 8: DB and in-memory state always agree (INV-7 deep focus)
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_db_and_memory_always_agree(lifecycle):
    """After every operation, DB snapshot and in-memory STATE must report identical totals."""
    lc = lifecycle(SILOS_3, SUPPLIERS_3)

    def check_agreement(label: str) -> None:
        """Verify DB snapshot == in-memory STATE for every silo."""
        from dem_sim.state import get_state
        state = get_state()
        with lc._conn() as conn:
            snap = lc._current_snapshot_id
            for silo in SILOS_3:
                sid = silo["silo_id"]
                db_total = float(
                    conn.execute(
                        "SELECT COALESCE(SUM(loaded_mass), 0) AS t FROM layers WHERE silo_id = %s AND snapshot_id = %s",
                        (sid, snap),
                    ).fetchone()["t"]
                )
                mem_total = sum(
                    float(l.get("remaining_mass_kg", l.get("segment_mass_kg", 0.0)))
                    for l in state["layers"]
                    if str(l.get("silo_id", "")) == sid
                )
                assert abs(db_total - mem_total) < 0.01, (
                    f"[{label}] silo={sid}: db={db_total:.4f} ≠ memory={mem_total:.4f}"
                )

    lc.charge([
        {"lot_id": "L001", "supplier": "BBM",       "mass_kg": 4000.0},
        {"lot_id": "L002", "supplier": "COFCO",     "mass_kg": 4000.0},
        {"lot_id": "L003", "supplier": "Malteurop", "mass_kg": 4000.0},
    ])
    check_agreement("after C1")
    lc.assert_invariants("after C1")

    lc.discharge({"S1": 0.40, "S2": 0.30, "S3": 0.20})
    check_agreement("after D1")
    lc.assert_invariants("after D1")

    lc.charge([
        {"lot_id": "L004", "supplier": "BBM",       "mass_kg": 3000.0},
        {"lot_id": "L005", "supplier": "Malteurop", "mass_kg": 3000.0},
    ])
    check_agreement("after C2")
    lc.assert_invariants("after C2")

    lc.discharge({"S1": 0.35, "S2": 0.35, "S3": 0.35})
    check_agreement("after D2")
    lc.assert_invariants("after D2")

    lc.discharge({"S1": 0.50, "S2": 0.50, "S3": 0.50})
    check_agreement("after D3")
    lc.assert_invariants("after D3")


# ---------------------------------------------------------------------------
# Scenario 9: Zero-mass layers do not pollute simulation or DB integrity
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_zero_mass_layers_do_not_pollute(lifecycle):
    """Fully-discharged lots leave zero-mass layers that must not corrupt future ops."""
    lc = lifecycle(SILOS_1, SUPPLIERS_3[:2])

    # Two equal lots.
    lc.charge([
        {"lot_id": "L001", "supplier": "BBM",   "mass_kg": 3000.0},
        {"lot_id": "L002", "supplier": "COFCO", "mass_kg": 3000.0},
    ])
    lc.assert_invariants("after C1")

    # Discharge exactly 50% — fully drains L001 (bottom layer).
    lc.discharge({"S1": 0.50})
    lc.assert_invariants("after D1 (L001 fully drained)")

    # INV-4: no negative masses.
    with lc._conn() as conn:
        snap = lc._current_snapshot_id
        neg = conn.execute(
            "SELECT COUNT(*) AS cnt FROM layers WHERE loaded_mass < -1e-9 AND snapshot_id = %s",
            (snap,),
        ).fetchone()["cnt"]
        assert neg == 0, f"Negative mass found after first discharge: {neg} layers"

    # New lot charged on top — must get a higher layer_index than existing layers.
    lc.charge([
        {"lot_id": "L003", "supplier": "BBM", "mass_kg": 2000.0},
    ])
    lc.assert_invariants("after C2 (new lot on top of zero-mass layer)")

    with lc._conn() as conn:
        snap = lc._current_snapshot_id
        rows = conn.execute(
            "SELECT lot_id, layer_index, loaded_mass FROM layers WHERE silo_id = 'S1' AND snapshot_id = %s ORDER BY layer_index",
            (snap,),
        ).fetchall()
        lot_ids = [r["lot_id"] for r in rows]
        layer_indexes = [r["layer_index"] for r in rows]

        # L003 must be above (higher index than) L001 and L002.
        assert "L003" in lot_ids, "L003 not found in silo layers"
        idx_l003 = next(r["layer_index"] for r in rows if r["lot_id"] == "L003")
        for r in rows:
            if r["lot_id"] in ("L001", "L002"):
                assert idx_l003 > r["layer_index"], (
                    f"L003 (index {idx_l003}) not above {r['lot_id']} (index {r['layer_index']})"
                )

        # Layer indexes must still be contiguous 1..N.
        assert layer_indexes == list(range(1, len(rows) + 1)), (
            f"Layer indexes not contiguous after zero-mass + recharge: {layer_indexes}"
        )

    # One more discharge — should not error with ghost zero-mass layers.
    lc.discharge({"S1": 0.30})
    lc.assert_invariants("after D2 (discharge through zero-mass layers)")


# ---------------------------------------------------------------------------
# Scenario 10: Full brewery week — 5 brews over a realistic schedule
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle_db
def test_full_brewery_week(lifecycle):
    """Realistic week: initial fill → 5 brews → mid-week delivery → invariants hold throughout."""
    lc = lifecycle(SILOS_3, SUPPLIERS_3)

    # Sunday: initial fill (3 silos, 4 lots each ≈ 90% capacity).
    lc.charge([
        {"lot_id": "L001", "supplier": "BBM",       "mass_kg": 3500.0},
        {"lot_id": "L002", "supplier": "COFCO",     "mass_kg": 3500.0},
        {"lot_id": "L003", "supplier": "Malteurop", "mass_kg": 3500.0},
        {"lot_id": "L004", "supplier": "BBM",       "mass_kg": 3500.0},
        {"lot_id": "L005", "supplier": "COFCO",     "mass_kg": 3500.0},
        {"lot_id": "L006", "supplier": "Malteurop", "mass_kg": 3500.0},
    ])
    lc.assert_invariants("Sunday initial fill")

    # Monday: brew 1 (≈30% of each silo).
    r1 = lc.discharge({"S1": 0.30, "S2": 0.30, "S3": 0.30})
    lc.assert_invariants("Monday brew 1")
    blend1 = r1["total_blended_params"]

    # Tuesday: brew 2.
    r2 = lc.discharge({"S1": 0.30, "S2": 0.30, "S3": 0.30})
    lc.assert_invariants("Tuesday brew 2")

    # Wednesday: mid-week delivery (new lots), then brew 3.
    lc.charge([
        {"lot_id": "L007", "supplier": "BBM",       "mass_kg": 2000.0},
        {"lot_id": "L008", "supplier": "COFCO",     "mass_kg": 2000.0},
        {"lot_id": "L009", "supplier": "Malteurop", "mass_kg": 2000.0},
    ])
    lc.assert_invariants("Wednesday delivery")

    r3 = lc.discharge({"S1": 0.30, "S2": 0.30, "S3": 0.30})
    lc.assert_invariants("Wednesday brew 3")

    # Thursday: brew 4.
    r4 = lc.discharge({"S1": 0.30, "S2": 0.30, "S3": 0.30})
    lc.assert_invariants("Thursday brew 4")

    # Friday: brew 5.
    r5 = lc.discharge({"S1": 0.30, "S2": 0.30, "S3": 0.30})
    lc.assert_invariants("Friday brew 5")

    # ── Final assertions ──────────────────────────────────────────────────

    total_brewed = sum(
        r["total_discharged_mass_kg"] for r in [r1, r2, r3, r4, r5]
    )
    assert abs(total_brewed - lc.cumulative_discharged_kg) < 0.01, (
        f"Brew totals {total_brewed:.2f} ≠ cumulative {lc.cumulative_discharged_kg:.2f}"
    )

    # DB: 6 results_run rows (one per discharge operation).
    with lc._conn() as conn:
        run_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM results_run"
        ).fetchone()["cnt"]
        assert run_count == 5, f"Expected 5 results_run rows, found {run_count}"

        # All historical snapshots have monotonically increasing IDs.
        snap_ids = [
            r["snapshot_id"]
            for r in conn.execute(
                "SELECT DISTINCT snapshot_id FROM layers ORDER BY snapshot_id"
            ).fetchall()
        ]
        assert snap_ids == list(range(1, len(snap_ids) + 1)), (
            f"snapshot_ids not monotonically sequential: {snap_ids}"
        )

        # INV-3 final sweep across all 9 lot_ids.
        conflicts = conn.execute(
            """
            SELECT lot_id, COUNT(DISTINCT supplier) AS cnt
            FROM layers WHERE lot_id != ''
            GROUP BY lot_id HAVING COUNT(DISTINCT supplier) > 1
            """
        ).fetchall()
        assert len(conflicts) == 0, (
            f"Supplier drift after full week: {[r['lot_id'] for r in conflicts]}"
        )

    # Blends from brews 3-5 should differ from brews 1-2 because new lots arrived.
    # (We only check that the simulation ran and produced non-nan values.)
    for brew_num, result in enumerate([r1, r2, r3, r4, r5], start=1):
        params = result["total_blended_params"]
        assert "moisture_pct" in params, f"Brew {brew_num} missing moisture_pct"
        assert params["moisture_pct"] == params["moisture_pct"], (  # nan check
            f"Brew {brew_num} moisture_pct is NaN"
        )

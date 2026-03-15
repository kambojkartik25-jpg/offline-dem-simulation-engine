from __future__ import annotations

import argparse
import json
import random
import time
from io import StringIO
from math import isnan
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .reporting import validate_inputs_shape
from .sample_data import (
    DISCHARGE_CSV,
    LAYERS_CSV,
    LOT_SIZE_KG,
    SILO_CAPACITY_KG,
    SILO_COUNT,
    SILO_SLOT_COUNT,
    SILOS_CSV,
    SUPPLIERS_CSV,
)
from .service import RunConfig, run_blend
from .db import execute, fetchall, get_conn
from .schema import ensure_schema as ensure_db_schema
from .state import (
    add_stage,
    apply_discharge_to_state,
    get_state,
    reset_state,
    run_fill_only_simulation,
    set_state,
    summarize_state,
)
from .storage import get_storage

_STORAGE = get_storage()
_STORAGE_READY = False


def _suppliers_from_incoming_queue_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build supplier specs from incoming_queue-like rows only."""
    def _alias_float(row: dict[str, Any], *keys: str) -> float:
        for key in keys:
            if key in row and row.get(key) is not None:
                try:
                    return float(row.get(key) or 0.0)
                except Exception:
                    continue
        return 0.0

    supplier_agg: dict[str, dict[str, Any]] = {}
    for r in rows:
        supplier_name = str(r.get("supplier", ""))
        if not supplier_name:
            continue
        if supplier_name in supplier_agg:
            continue
        supplier_agg[supplier_name] = {
            "supplier": supplier_name,
            "moisture_pct": float(r.get("moisture_pct", 0.0) or 0.0),
            "fine_extract_db_pct": float(r.get("fine_extract_db_pct", 0.0) or 0.0),
            "wort_pH": _alias_float(r, "wort_pH", "wort_ph"),
            "diastatic_power_WK": _alias_float(r, "diastatic_power_WK", "diastatic_power_wk"),
            "total_protein_pct": float(r.get("total_protein_pct", 0.0) or 0.0),
            "wort_colour_EBC": _alias_float(r, "wort_colour_EBC", "wort_colour_ebc"),
        }
    return list(supplier_agg.values())


def _ensure_storage_ready() -> None:
    global _STORAGE_READY
    if _STORAGE_READY:
        return
    try:
        ensure_db_schema()
        _STORAGE.ensure_schema()
        _STORAGE_READY = True
    except Exception:
        return


def _sync_incoming_queue_to_db(state_queue: list[dict[str, Any]]) -> None:
    # Persist per-lot queue state back to DB without creating new rows.
    lot_remaining: dict[str, float] = {}
    for row in state_queue:
        lot_id = str(row.get("lot_id", ""))
        if not lot_id:
            continue
        lot_remaining[lot_id] = round(max(0.0, float(row.get("mass_kg", 0.0))), 6)

    db_rows = fetchall("SELECT id, lot_id FROM incoming_queue ORDER BY id")
    for row in db_rows:
        row_id = int(row.get("id", 0))
        lot_id = str(row.get("lot_id", ""))
        remaining = float(lot_remaining.get(lot_id, 0.0))
        consumed = remaining <= 1e-9
        execute(
            """
            UPDATE incoming_queue
            SET remaining_mass_kg = %s, is_fully_consumed = %s
            WHERE id = %s
            """,
            (remaining, consumed, row_id),
        )


def _sync_layers_to_db(
    state: dict[str, Any], event_type: str, sim_event_id: int | None = None
) -> None:
    # Persist current fill-state layers as an append-only snapshot in `layers`.
    # Discharge sync is intentionally separate.
    silos = [str(s.get("silo_id", "")) for s in state.get("silos", []) if str(s.get("silo_id", ""))]
    by_silo: dict[str, list[dict[str, Any]]] = {sid: [] for sid in silos}
    for row in state.get("layers", []):
        sid = str(row.get("silo_id", ""))
        if sid in by_silo:
            by_silo[sid].append(dict(row))

    with get_conn() as conn:
        with conn.transaction():
            snap_row = conn.execute(
                "SELECT COALESCE(MAX(snapshot_id), 0) AS max_snapshot_id FROM layers"
            ).fetchone()
            snapshot_id = int(snap_row["max_snapshot_id"]) + 1 if snap_row else 1
            for sid in silos:
                silo_layers = by_silo.get(sid, [])
                silo_layers.sort(key=lambda r: int(r.get("layer_index", 0)))
                for idx, row in enumerate(silo_layers, start=1):
                    lot_id = str(row.get("lot_id", ""))
                    supplier = str(row.get("supplier", ""))
                    remaining_mass_kg = float(
                        row.get("remaining_mass_kg", row.get("segment_mass_kg", 0.0)) or 0.0
                    )
                    conn.execute(
                        """
                        INSERT INTO layers (
                            silo_id, sim_event_id, snapshot_id, event_type, layer_index, lot_id, supplier, loaded_mass
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            sid,
                            sim_event_id,
                            snapshot_id,
                            event_type,
                            idx,
                            lot_id,
                            supplier,
                            round(remaining_mass_kg, 6),
                        ),
                    )


def _persist_state_bundle(event_type: str, payload: dict[str, Any] | None = None) -> None:
    try:
        _ensure_storage_ready()
        state = get_state()
        summary = summarize_state()
        _STORAGE.write_snapshot(
            event_type=event_type,
            action=str(state.get("last_action", "")),
            state=state,
            summary=summary,
            payload=payload or {},
        )
        _STORAGE.write_stages(state.get("stages", []))
        _STORAGE.write_history(state.get("history", []))
    except Exception:
        return


def _persist_result(event_type: str, result: dict[str, Any], payload: dict[str, Any] | None = None) -> None:
    try:
        _ensure_storage_ready()
        _STORAGE.write_result(event_type=event_type, result=result, payload=payload or {})
    except Exception:
        return


def _write_sim_event(
    *,
    event_type: str,
    action: str,
    state_before: dict[str, Any] | None = None,
    state_after: dict[str, Any] | None = None,
    discharge_by_silo: dict[str, float] | None = None,
    total_discharged_mass_kg: float | None = None,
    total_remaining_mass_kg: float | None = None,
    incoming_queue_count: int | None = None,
    incoming_queue_mass_kg: float | None = None,
    objective_score: float | None = None,
    meta: dict[str, Any] | None = None,
) -> int | None:
    try:
        # Ensure consolidated tracking table exists before insert.
        ensure_db_schema()
        with get_conn() as conn:
            with conn.transaction():
                row = conn.execute(
                    """
                    INSERT INTO sim_events (
                        event_type,
                        action,
                        state_before,
                        state_after,
                        discharge_by_silo,
                        total_discharged_mass_kg,
                        total_remaining_mass_kg,
                        incoming_queue_count,
                        incoming_queue_mass_kg,
                        objective_score,
                        meta
                    )
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb)
                    RETURNING id
                    """,
                    (
                        event_type,
                        action,
                        json.dumps(state_before or {}),
                        json.dumps(state_after or {}),
                        json.dumps(discharge_by_silo or {}),
                        total_discharged_mass_kg,
                        total_remaining_mass_kg,
                        incoming_queue_count,
                        incoming_queue_mass_kg,
                        objective_score,
                        json.dumps(meta or {}),
                    ),
                ).fetchone()
                if row:
                    return int(row.get("id"))
        return None
    except Exception as e:
        # Keep request flow alive, but emit a visible diagnostic instead of silent drop.
        print(f"[sim_events] insert failed: {e}")
        return None


class RunRequest(BaseModel):
    silos: list[dict[str, Any]] = Field(default_factory=list)
    layers: list[dict[str, Any]] = Field(default_factory=list)
    suppliers: list[dict[str, Any]] = Field(default_factory=list)
    discharge: list[dict[str, Any]] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)


class OptimizeRequest(RunRequest):
    target_params: dict[str, float] = Field(default_factory=dict)
    iterations: int = 120
    seed: int = 42


class ProcessRunSimulationRequest(BaseModel):
    silos: list[dict[str, Any]] = Field(default_factory=list)
    layers: list[dict[str, Any]] = Field(default_factory=list)
    suppliers: list[dict[str, Any]] = Field(default_factory=list)
    incoming_queue: list[dict[str, Any]] = Field(default_factory=list)


class ProcessOptimizeRequest(BaseModel):
    target_params: dict[str, float] = Field(default_factory=dict)
    iterations: int = 120
    seed: int = 42
    config: dict[str, Any] = Field(default_factory=dict)


class ProcessApplyDischargeRequest(BaseModel):
    discharge: list[dict[str, Any]] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)


class GenerateRandomDataRequest(BaseModel):
    seed: int = 42
    silos_count: int = 3
    lots_count: int = 100
    lot_size_kg: float = 2000.0


class GenerateScheduleRequest(BaseModel):
    schedule_id: str | None = None
    name: str = "MVP Brew Schedule"
    brews_count: int = 5
    seed: int = 42
    target_params: dict[str, float] = Field(default_factory=dict)


class ScheduleOptimizeRequest(BaseModel):
    iterations: int = 120
    seed: int = 42
    config: dict[str, Any] = Field(default_factory=dict)


class ScheduleApplyRequest(BaseModel):
    candidate_index: int = 0
    config: dict[str, Any] = Field(default_factory=dict)


DEFAULT_SCHEDULE_TARGET_PARAMS = {
    "moisture_pct": 4.35,
    "fine_extract_db_pct": 82.40,
    "wort_pH": 5.89,
    "diastatic_power_WK": 332.0,
    "total_protein_pct": 10.60,
    "wort_colour_EBC": 4.40,
}


def _generate_random_payload(
    *, seed: int, silos_count: int, lots_count: int, lot_size_kg: float
) -> dict[str, Any]:
    rng = random.Random(seed)
    silos_count = max(1, int(silos_count))
    lots_count = max(1, int(lots_count))
    lot_size_kg = max(1.0, float(lot_size_kg))
    suppliers = ["BBM", "COFCO", "Malteurop"]

    silos: list[dict[str, Any]] = []
    for i in range(silos_count):
        silos.append(
            {
                "silo_id": f"S{i+1}",
                "capacity_kg": float(8000.0),
                "body_diameter_m": round(rng.uniform(2.8, 3.4), 3),
                "outlet_diameter_m": round(rng.uniform(0.18, 0.23), 3),
                "initial_mass_kg": 0.0,
            }
        )

    supplier_specs = {
        "BBM": {
            "supplier": "BBM",
            "moisture_pct": 4.20,
            "fine_extract_db_pct": 82.10,
            "wort_pH": 5.86,
            "diastatic_power_WK": 320.0,
            "total_protein_pct": 10.40,
            "wort_colour_EBC": 4.30,
        },
        "COFCO": {
            "supplier": "COFCO",
            "moisture_pct": 4.35,
            "fine_extract_db_pct": 82.40,
            "wort_pH": 5.89,
            "diastatic_power_WK": 332.0,
            "total_protein_pct": 10.60,
            "wort_colour_EBC": 4.40,
        },
        "Malteurop": {
            "supplier": "Malteurop",
            "moisture_pct": 4.50,
            "fine_extract_db_pct": 82.70,
            "wort_pH": 5.92,
            "diastatic_power_WK": 344.0,
            "total_protein_pct": 10.80,
            "wort_colour_EBC": 4.50,
        },
    }
    suppliers_rows = [supplier_specs[s] for s in suppliers]
    incoming_queue: list[dict[str, Any]] = []
    for i in range(lots_count):
        sup = suppliers[i % len(suppliers)]
        spec = supplier_specs[sup]
        incoming_queue.append(
            {
                "lot_id": f"LOT{i+1:03d}",
                "supplier": sup,
                "mass_kg": float(lot_size_kg),
                "moisture_pct": float(spec["moisture_pct"]),
                "fine_extract_db_pct": float(spec["fine_extract_db_pct"]),
                "wort_pH": float(spec["wort_pH"]),
                "diastatic_power_WK": float(spec["diastatic_power_WK"]),
                "total_protein_pct": float(spec["total_protein_pct"]),
                "wort_colour_EBC": float(spec["wort_colour_EBC"]),
            }
        )

    return {
        "silos": silos,
        "layers": [],
        "suppliers": suppliers_rows,
        "incoming_queue": incoming_queue,
        "discharge": [{"silo_id": s["silo_id"], "discharge_mass_kg": None, "discharge_fraction": 0.5} for s in silos],
        "config": {
            "rho_bulk_kg_m3": 610.0,
            "grain_diameter_m": 0.004,
            "beverloo_c": 0.58,
            "beverloo_k": 1.4,
            "gravity_m_s2": 9.81,
            "sigma_m": 0.12,
            "steps": 2000,
            "auto_adjust": True,
            "moisture_beta": 0.0,
            "sigma_alpha": 0.0,
            "skew_alpha": 0.0,
        },
    }


def _replace_db_seed_data(payload: dict[str, Any]) -> None:
    silos = payload.get("silos", [])
    suppliers = payload.get("suppliers", [])
    queue = payload.get("incoming_queue", [])
    with get_conn() as conn:
        with conn.transaction():
            conn.execute("DELETE FROM layers")
            conn.execute("DELETE FROM incoming_queue")
            conn.execute("DELETE FROM suppliers")
            conn.execute("DELETE FROM silos")

            for s in silos:
                conn.execute(
                    """
                    INSERT INTO silos (silo_id, capacity_kg, body_diameter_m, outlet_diameter_m, initial_mass_kg)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        str(s.get("silo_id", "")),
                        float(s.get("capacity_kg", 0.0) or 0.0),
                        float(s.get("body_diameter_m", 0.0) or 0.0),
                        float(s.get("outlet_diameter_m", 0.0) or 0.0),
                        float(s.get("initial_mass_kg", 0.0) or 0.0),
                    ),
                )
            for sp in suppliers:
                conn.execute(
                    """
                    INSERT INTO suppliers (name, moisture_pct, fine_extract_db_pct, wort_pH, diastatic_power_WK, total_protein_pct, wort_colour_EBC)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(sp.get("supplier", "")),
                        float(sp.get("moisture_pct", 0.0) or 0.0),
                        float(sp.get("fine_extract_db_pct", 0.0) or 0.0),
                        float(sp.get("wort_pH", 0.0) or 0.0),
                        float(sp.get("diastatic_power_WK", 0.0) or 0.0),
                        float(sp.get("total_protein_pct", 0.0) or 0.0),
                        float(sp.get("wort_colour_EBC", 0.0) or 0.0),
                    ),
                )
            for q in queue:
                mass = float(q.get("mass_kg", 0.0) or 0.0)
                conn.execute(
                    """
                    INSERT INTO incoming_queue (
                        lot_id,
                        supplier,
                        mass_kg,
                        remaining_mass_kg,
                        is_fully_consumed,
                        moisture_pct,
                        fine_extract_db_pct,
                        wort_pH,
                        diastatic_power_WK,
                        total_protein_pct,
                        wort_colour_EBC
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(q.get("lot_id", "")),
                        str(q.get("supplier", "")),
                        mass,
                        mass,
                        False,
                        float(q.get("moisture_pct", 0.0) or 0.0),
                        float(q.get("fine_extract_db_pct", 0.0) or 0.0),
                        float(q.get("wort_pH", 0.0) or 0.0),
                        float(q.get("diastatic_power_WK", 0.0) or 0.0),
                        float(q.get("total_protein_pct", 0.0) or 0.0),
                        float(q.get("wort_colour_EBC", 0.0) or 0.0),
                    ),
                )

def _records_json_safe(df: pd.DataFrame) -> list[dict[str, Any]]:
    records = df.to_dict(orient="records")
    out: list[dict[str, Any]] = []
    for row in records:
        cleaned: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, float) and isnan(value):
                cleaned[key] = None
            else:
                cleaned[key] = value
        out.append(cleaned)
    return out


def _sample_payload() -> dict[str, Any]:
    # Prefer consolidated event state from sim_events; fallback to tables/sample.
    try:
        rows = fetchall(
            """
            SELECT state_after
            FROM sim_events
            ORDER BY id DESC
            LIMIT 100
            """
        )
        for r in rows:
            state_after = r.get("state_after")
            if isinstance(state_after, str):
                try:
                    state_after = json.loads(state_after)
                except Exception:
                    state_after = None
            if not isinstance(state_after, dict):
                continue
            silos = state_after.get("silos")
            layers = state_after.get("layers")
            suppliers = state_after.get("suppliers")
            incoming_queue = state_after.get("incoming_queue")
            if not isinstance(silos, list) or not isinstance(layers, list):
                continue
            if suppliers is None:
                suppliers = []
            if incoming_queue is None:
                incoming_queue = []
            # Always source incoming lots from DB incoming_queue (latest), not event snapshot.
            queue_rows = fetchall(
                """
                SELECT *
                FROM incoming_queue
                ORDER BY id
                """
            )
            incoming_queue_live = []
            for qr in queue_rows:
                base_mass_kg = float(qr.get("mass_kg", 0.0) or 0.0)
                remaining_mass_kg = float(qr.get("remaining_mass_kg", base_mass_kg) or 0.0)
                is_fully_consumed = bool(qr.get("is_fully_consumed", False))
                if (remaining_mass_kg > 0) and (not is_fully_consumed):
                    incoming_queue_live.append(
                        {
                            "lot_id": str(qr.get("lot_id", "")),
                            "supplier": str(qr.get("supplier", "")),
                            "mass_kg": remaining_mass_kg,
                        }
                    )
            suppliers_from_queue = _suppliers_from_incoming_queue_rows(queue_rows)
            if suppliers_from_queue:
                suppliers = suppliers_from_queue
            return {
                "silos": silos,
                "layers": layers,
                "suppliers": suppliers,
                "discharge": [
                    {"silo_id": str(s.get("silo_id", "")), "discharge_mass_kg": None, "discharge_fraction": 0.5}
                    for s in silos
                    if str(s.get("silo_id", ""))
                ],
                "assumptions": {
                    "lot_size_kg": LOT_SIZE_KG,
                    "silo_slot_count": SILO_SLOT_COUNT,
                    "silo_count": len(silos),
                    "silo_capacity_kg": float(sum(float(s.get("capacity_kg", 0.0)) for s in silos)),
                    "charging_policy": "sim_events_state_after",
                },
                "incoming_queue": incoming_queue_live,
                "config": {
                    "rho_bulk_kg_m3": 610.0,
                    "grain_diameter_m": 0.004,
                    "beverloo_c": 0.58,
                    "beverloo_k": 1.4,
                    "gravity_m_s2": 9.81,
                    "sigma_m": 0.12,
                    "steps": 2000,
                    "auto_adjust": True,
                    "moisture_beta": 0.0,
                    "sigma_alpha": 0.0,
                    "skew_alpha": 0.0,
                },
            }
    except Exception:
        pass

    # Fallback: prefer on-prem Postgres input when available; fallback to bundled CSV sample.
    try:
        silos_rows = fetchall(
            """
            SELECT silo_id, capacity_kg, body_diameter_m, outlet_diameter_m, initial_mass_kg
            FROM silos
            ORDER BY silo_id
            """
        )
        queue_rows = fetchall(
            """
            SELECT *
            FROM incoming_queue
            ORDER BY id
            """
        )
        layers_rows = fetchall(
            """
            SELECT silo_id, layer_index, lot_id, supplier, loaded_mass
            FROM layers
            WHERE snapshot_id = (SELECT COALESCE(MAX(snapshot_id), 0) FROM layers)
            ORDER BY silo_id, layer_index
            """
        )
        if silos_rows:
            silos = [
                {
                    "silo_id": str(r.get("silo_id", "")),
                    "capacity_kg": float(r.get("capacity_kg", 0.0)),
                    "body_diameter_m": float(r.get("body_diameter_m", 0.0)),
                    "outlet_diameter_m": float(r.get("outlet_diameter_m", 0.0)),
                    "initial_mass_kg": float(r.get("initial_mass_kg", 0.0) or 0.0),
                }
                for r in silos_rows
            ]
            incoming_queue = []
            for r in queue_rows:
                supplier_name = str(r.get("supplier", ""))
                lot_id = str(r.get("lot_id", ""))
                base_mass_kg = float(r.get("mass_kg", 0.0) or 0.0)
                remaining_mass_kg = float(r.get("remaining_mass_kg", base_mass_kg) or 0.0)
                is_fully_consumed = bool(r.get("is_fully_consumed", False))
                if (remaining_mass_kg > 0) and (not is_fully_consumed):
                    incoming_queue.append(
                        {"lot_id": lot_id, "supplier": supplier_name, "mass_kg": remaining_mass_kg}
                    )
            suppliers = _suppliers_from_incoming_queue_rows(queue_rows)
            layers = [
                {
                    "silo_id": str(r.get("silo_id", "")),
                    "layer_index": int(r.get("layer_index", 0) or 0),
                    "lot_id": str(r.get("lot_id", "")),
                    "supplier": str(r.get("supplier", "")),
                    "segment_mass_kg": float(r.get("loaded_mass", 0.0) or 0.0),
                    "remaining_mass_kg": float(r.get("loaded_mass", 0.0) or 0.0),
                }
                for r in layers_rows
                if float(r.get("loaded_mass", 0.0) or 0.0) > 0
            ]
            return {
                "silos": silos,
                "layers": layers,
                "suppliers": suppliers,
                "discharge": [
                    {"silo_id": s["silo_id"], "discharge_mass_kg": None, "discharge_fraction": 0.5}
                    for s in silos
                ],
                "assumptions": {
                    "lot_size_kg": LOT_SIZE_KG,
                    "silo_slot_count": SILO_SLOT_COUNT,
                    "silo_count": len(silos),
                    "silo_capacity_kg": float(sum(float(s.get("capacity_kg", 0.0)) for s in silos)),
                    "charging_policy": "db_bootstrap_fill_only",
                },
                "incoming_queue": incoming_queue,
                "config": {
                    "rho_bulk_kg_m3": 610.0,
                    "grain_diameter_m": 0.004,
                    "beverloo_c": 0.58,
                    "beverloo_k": 1.4,
                    "gravity_m_s2": 9.81,
                    "sigma_m": 0.12,
                    "steps": 2000,
                    "auto_adjust": True,
                    "moisture_beta": 0.0,
                    "sigma_alpha": 0.0,
                    "skew_alpha": 0.0,
                },
            }
    except Exception:
        pass

    layers = _records_json_safe(pd.read_csv(StringIO(LAYERS_CSV)))
    placed_lot_ids = {str(x.get("lot_id", "")) for x in layers}
    queue: list[dict[str, Any]] = []
    # Keep deterministic queue extension for UI demonstration after initial 12 filled lots.
    for i in range(1013, 1021):
        lot_id = f"L{i}"
        if lot_id in placed_lot_ids:
            continue
        supplier = "BBM" if i % 3 == 1 else ("COFCO" if i % 3 == 2 else "Malteurop")
        queue.append({"lot_id": lot_id, "supplier": supplier, "mass_kg": float(LOT_SIZE_KG)})
    return {
        "silos": _records_json_safe(pd.read_csv(StringIO(SILOS_CSV))),
        "layers": layers,
        "suppliers": _records_json_safe(pd.read_csv(StringIO(SUPPLIERS_CSV))),
        "discharge": _records_json_safe(pd.read_csv(StringIO(DISCHARGE_CSV))),
        "assumptions": {
            "lot_size_kg": LOT_SIZE_KG,
            "silo_slot_count": SILO_SLOT_COUNT,
            "silo_count": SILO_COUNT,
            "silo_capacity_kg": SILO_CAPACITY_KG,
            "charging_policy": "strict_whole_lot_no_split_block_fill",
        },
        "incoming_queue": queue,
        "config": {
            "rho_bulk_kg_m3": 610.0,
            "grain_diameter_m": 0.004,
            "beverloo_c": 0.58,
            "beverloo_k": 1.4,
            "gravity_m_s2": 9.81,
            "sigma_m": 0.12,
            "steps": 2000,
            "auto_adjust": True,
            "moisture_beta": 0.0,
            "sigma_alpha": 0.0,
            "skew_alpha": 0.0,
        },
    }


def _load_incoming_queue_from_db() -> list[dict[str, Any]]:
    rows = fetchall(
        """
        SELECT lot_id, supplier, COALESCE(remaining_mass_kg, mass_kg) AS live_mass_kg
        FROM incoming_queue
        WHERE COALESCE(is_fully_consumed, FALSE) = FALSE
          AND COALESCE(remaining_mass_kg, mass_kg) > 0
        ORDER BY id
        """
    )
    return [
        {
            "lot_id": str(r.get("lot_id", "")),
            "supplier": str(r.get("supplier", "")),
            "mass_kg": float(r.get("live_mass_kg", 0.0) or 0.0),
        }
        for r in rows
    ]


def _ensure_state_initialized() -> None:
    # Only bootstrap from DB/sample when state has not yet been set (silos list is empty).
    # This preserves explicitly set state (e.g. from tests or prior API calls).
    if get_state()["silos"]:
        return
    payload = _sample_payload()
    set_state(
        silos=payload["silos"],
        layers=payload["layers"],
        suppliers=payload["suppliers"],
        incoming_queue=payload.get("incoming_queue", []),
        action="bootstrap_sample_state",
        meta={"source": "sample_payload"},
    )


def _result_to_api_payload(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "total_discharged_mass_kg": float(result["total_discharged_mass_kg"]),
        "total_remaining_mass_kg": float(result["total_remaining_mass_kg"]),
        "total_blended_params": {
            k: float(v) for k, v in result["total_blended_params"].items()
        },
        "silo_state_ledger": result["df_silo_state_ledger"].to_dict(orient="records"),
        "per_silo": {
            silo_id: {
                "discharged_mass_kg": float(r["discharged_mass_kg"]),
                "mass_flow_rate_kg_s": float(r["mass_flow_rate_kg_s"]),
                "discharge_time_s": float(r["discharge_time_s"]),
                "sigma_m": float(r["sigma_m"]),
                "blended_params_per_silo": {
                    k: float(v) for k, v in r["blended_params_per_silo"].items()
                },
            }
            for silo_id, r in result["per_silo"].items()
        },
    }


DEFAULT_PARAM_RANGES = {
    "moisture_pct": 5.0 - 0.0,
    "fine_extract_db_pct": 83.0 - 81.0,
    "wort_pH": 6.0 - 5.8,
    "diastatic_power_WK": 360.0 - 300.0,
    "total_protein_pct": 11.2 - 10.2,
    "wort_colour_EBC": 4.7 - 4.3,
}
DISCHARGE_FRACTION_MIN = 0.2
DISCHARGE_FRACTION_MAX = 0.8
FIXED_DISCHARGE_TARGET_KG = 12000.0
FIXED_DISCHARGE_TOL_KG = 1e-3


def _score_blend(
    actual: dict[str, float], target: dict[str, float], param_ranges: dict[str, float]
) -> float:
    if not target:
        return float("inf")
    score = 0.0
    for key, t in target.items():
        a = float(actual.get(key, 0.0))
        scale = float(param_ranges.get(key, 1.0))
        if scale <= 0:
            scale = 1.0
        score += ((a - float(t)) / scale) ** 2
    return score


# Built once at import time — shared by all hot-path scoring functions.
PARAM_KEYS: list[str] = list(DEFAULT_PARAM_RANGES.keys())


def _score_blend_vectorised(
    actual: dict[str, Any],
    target: dict[str, Any],
    param_ranges: dict[str, float],
) -> float:
    """Normalised weighted L2 error using numpy — replaces the Python loop in _score_blend.

    ~10-20x faster in the hot path. Equal weights (1/N) across all parameters.
    """
    a = np.array([actual.get(k, 0.0)       for k in PARAM_KEYS], dtype=np.float64)
    t = np.array([target.get(k, 0.0)       for k in PARAM_KEYS], dtype=np.float64)
    r = np.array([param_ranges.get(k, 1.0) for k in PARAM_KEYS], dtype=np.float64)
    w = np.ones(len(PARAM_KEYS), dtype=np.float64) / len(PARAM_KEYS)
    r = np.where(r == 0.0, 1.0, r)
    return float(np.sqrt(np.sum(w * ((a - t) / r) ** 2)))


def _score_batch(
    candidates: list[dict[str, Any]],
    target: dict[str, Any],
    param_ranges: dict[str, float],
) -> np.ndarray:
    """Score an entire candidate list in one matrix operation.

    Each candidate must have a 'blended_params' key.
    Returns a 1-D float64 array of length len(candidates).
    """
    if not candidates:
        return np.array([], dtype=np.float64)
    t = np.array([target.get(k, 0.0)       for k in PARAM_KEYS], dtype=np.float64)
    r = np.array([param_ranges.get(k, 1.0)  for k in PARAM_KEYS], dtype=np.float64)
    w = np.ones(len(PARAM_KEYS), dtype=np.float64) / len(PARAM_KEYS)
    r = np.where(r == 0.0, 1.0, r)
    A = np.array(
        [[c["blended_params"].get(k, 0.0) for k in PARAM_KEYS] for c in candidates],
        dtype=np.float64,
    )
    return np.sqrt(np.sum(w * ((A - t) / r) ** 2, axis=1))


def _diverse_top_k(
    candidates: list[dict[str, Any]],
    k: int = 5,
) -> list[dict[str, Any]]:
    """Maximin diversity selection from the candidate pool.

    Picks k candidates that are both high-quality (low score) AND spread across
    the discharge-fraction space, so the brewer sees genuinely different options.

    Algorithm:
      1. Sort by objective_score; keep top min(len, k*6, 30) as pool.
      2. Seed selection with the best-scoring candidate.
      3. Greedily add the candidate whose minimum distance to the already-selected
         set is largest (Maximin criterion).
    """
    if len(candidates) <= k:
        return candidates

    pool = sorted(candidates, key=lambda x: x["objective_score"])[: max(k * 6, 30)]

    def _frac_vec(c: dict) -> np.ndarray:
        return np.array(
            [float(r["discharge_fraction"]) for r in c["recommended_discharge"]],
            dtype=np.float64,
        )

    selected: list[dict[str, Any]] = [pool[0]]
    selected_vecs: list[np.ndarray] = [_frac_vec(pool[0])]
    pool_vecs = [_frac_vec(c) for c in pool]

    for _ in range(k - 1):
        best_cand = None
        best_dist = -1.0
        for i, c in enumerate(pool):
            if c in selected:
                continue
            min_dist = min(np.linalg.norm(pool_vecs[i] - sv) for sv in selected_vecs)
            if min_dist > best_dist:
                best_dist = min_dist
                best_cand = (i, c)
        if best_cand is None:
            break
        selected.append(best_cand[1])
        selected_vecs.append(pool_vecs[best_cand[0]])

    return selected


def _clip_fraction(v: float) -> float:
    return max(DISCHARGE_FRACTION_MIN, min(DISCHARGE_FRACTION_MAX, float(v)))


def _candidate_rows_from_fractions(
    silo_ids: list[str], fractions: list[float]
) -> list[dict[str, Any]]:
    return [
        {
            "silo_id": silo_id,
            "discharge_mass_kg": None,
            "discharge_fraction": round(_clip_fraction(frac), 4),
        }
        for silo_id, frac in zip(silo_ids, fractions)
    ]


def _ensure_discharge_has_silo_ids(inputs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    discharge_df = inputs.get("discharge", pd.DataFrame())
    if not discharge_df.empty and "silo_id" in discharge_df.columns:
        return inputs
    silos_df = inputs["silos"]
    inputs["discharge"] = pd.DataFrame({"silo_id": silos_df["silo_id"].astype(str).tolist()})
    return inputs


def _available_mass_by_silo(layers_df: pd.DataFrame) -> dict[str, float]:
    if layers_df.empty:
        return {}
    grouped = (
        layers_df.groupby(layers_df["silo_id"].astype(str))["segment_mass_kg"]
        .sum()
        .astype(float)
    )
    return {str(k): float(v) for k, v in grouped.to_dict().items()}


def _normalize_discharge_to_target(
    rows: list[dict[str, Any]],
    available_by_silo: dict[str, float],
    target_total_kg: float,
) -> list[dict[str, Any]]:
    available_total = float(sum(available_by_silo.values()))
    masses = {str(r["silo_id"]): max(0.0, float(r.get("discharge_mass_kg", 0.0))) for r in rows}
    if available_total + 1e-12 < target_total_kg:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Insufficient available mass for fixed discharge target {target_total_kg:.3f} kg. "
                f"Currently available: {available_total:.3f} kg."
            ),
        )
    total = sum(masses.values())
    if total <= 1e-12:
        # deterministic equal split over silos with available mass
        active = [sid for sid, m in available_by_silo.items() if m > 1e-12]
        if not active:
            raise HTTPException(status_code=422, detail="No available mass in silos.")
        share = target_total_kg / len(active)
        masses = {sid: share if sid in active else 0.0 for sid in available_by_silo}
    else:
        scale = target_total_kg / total
        masses = {sid: m * scale for sid, m in masses.items()}

    # Respect silo caps and redistribute overflow iteratively.
    capped = {sid: min(masses.get(sid, 0.0), available_by_silo.get(sid, 0.0)) for sid in available_by_silo}
    deficit = target_total_kg - sum(capped.values())
    for _ in range(10):
        if deficit <= 1e-9:
            break
        room = {sid: available_by_silo[sid] - capped[sid] for sid in capped}
        total_room = sum(max(0.0, v) for v in room.values())
        if total_room <= 1e-12:
            break
        for sid in capped:
            r = max(0.0, room[sid])
            if r <= 0:
                continue
            add = deficit * (r / total_room)
            capped[sid] += min(add, r)
        deficit = target_total_kg - sum(capped.values())

    if abs(target_total_kg - sum(capped.values())) > 1e-6:
        raise HTTPException(
            status_code=422,
            detail=f"Could not satisfy exact fixed discharge target {target_total_kg:.3f} kg.",
        )

    out: list[dict[str, Any]] = []
    for sid in sorted(available_by_silo.keys()):
        avail = available_by_silo[sid]
        mass = capped.get(sid, 0.0)
        out.append(
            {
                "silo_id": sid,
                "discharge_mass_kg": round(mass, 6),
                "discharge_fraction": round((mass / avail) if avail > 1e-12 else 0.0, 6),
            }
        )
    return out


def create_app() -> FastAPI:
    app = FastAPI(title="DEM Simulation API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    ui_dir = Path(__file__).resolve().parent / "ui"
    app.mount("/ui", StaticFiles(directory=ui_dir, html=True), name="ui")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        # Avoid noisy 404 logs when browsers auto-request favicon.
        return Response(status_code=204)

    @app.get("/api/sample")
    def sample() -> dict[str, Any]:
        return _sample_payload()

    @app.post("/api/data/generate-random")
    def generate_random_data(req: GenerateRandomDataRequest) -> dict[str, Any]:
        ensure_db_schema()
        payload = _generate_random_payload(
            seed=req.seed,
            silos_count=req.silos_count,
            lots_count=req.lots_count,
            lot_size_kg=req.lot_size_kg,
        )
        _replace_db_seed_data(payload)
        reset_state()
        set_state(
            silos=payload.get("silos", []),
            layers=payload.get("layers", []),
            suppliers=payload.get("suppliers", []),
            incoming_queue=payload.get("incoming_queue", []),
            action="generate_random_data",
            meta={"seed": req.seed},
        )
        summary = summarize_state()
        sim_event_id = _write_sim_event(
            event_type="generate_random_data",
            action="generate_random_data",
            state_after=get_state(),
            incoming_queue_count=int(summary.get("incoming_queue", {}).get("count", 0)),
            incoming_queue_mass_kg=float(summary.get("incoming_queue", {}).get("total_mass_kg", 0.0)),
            meta={"seed": req.seed, "silos_count": req.silos_count, "lots_count": req.lots_count},
        )
        _sync_layers_to_db(get_state(), event_type="generate_random_data", sim_event_id=sim_event_id)
        return {"status": "ok", "payload": payload, "summary": summary, "sim_event_id": sim_event_id}

    @app.get("/api/state")
    def state() -> dict[str, Any]:
        _ensure_state_initialized()
        return {"state": get_state(), "summary": summarize_state()}

    @app.post("/api/state/reset")
    def state_reset() -> dict[str, Any]:
        payload = _sample_payload()
        reset_state()
        set_state(
            silos=payload["silos"],
            layers=payload["layers"],
            suppliers=payload["suppliers"],
            incoming_queue=payload.get("incoming_queue", []),
            action="state_reset_to_sample",
            meta={},
        )
        out = {"state": get_state(), "summary": summarize_state()}
        try:
            _sync_incoming_queue_to_db(out["state"].get("incoming_queue", []))
        except Exception as e:
            print(f"[state_reset] incoming_queue sync failed: {e}")
        sim_event_id = _write_sim_event(
            event_type="state_reset",
            action="state_reset_to_sample",
            state_after=out.get("state", {}),
            incoming_queue_count=int(out.get("summary", {}).get("incoming_queue", {}).get("count", 0)),
            incoming_queue_mass_kg=float(out.get("summary", {}).get("incoming_queue", {}).get("total_mass_kg", 0.0)),
            meta={"source": "state_reset"},
        )
        try:
            _sync_layers_to_db(out["state"], event_type="state_reset", sim_event_id=sim_event_id)
        except Exception as e:
            print(f"[state_reset] layers sync failed: {e}")
        return out

    @app.post("/api/process/run_simulation")
    def process_run_simulation(req: ProcessRunSimulationRequest) -> dict[str, Any]:
        _ensure_state_initialized()
        before_state = get_state()
        # DB is the source of truth for simulation state; ignore UI-provided state for mutation.
        _ = req
        out = run_fill_only_simulation()
        after_state = out.get("state", {})
        after_summary = out.get("summary", {})
        try:
            _sync_incoming_queue_to_db(out["state"].get("incoming_queue", []))
        except Exception as e:
            print(f"[run_simulation_fill_only] incoming_queue sync failed: {e}")
        sim_event_id = _write_sim_event(
            event_type="run_simulation_fill_only",
            action="run_simulation_fill_only",
            state_before=before_state,
            state_after=after_state,
            total_discharged_mass_kg=0.0,
            total_remaining_mass_kg=None,
            incoming_queue_count=int(after_summary.get("incoming_queue", {}).get("count", 0)),
            incoming_queue_mass_kg=float(after_summary.get("incoming_queue", {}).get("total_mass_kg", 0.0)),
            meta={"source": "process_run_simulation"},
        )
        try:
            _sync_layers_to_db(
                out["state"],
                event_type="run_simulation_fill_only",
                sim_event_id=sim_event_id,
            )
        except Exception as e:
            print(f"[run_simulation_fill_only] layers sync failed: {e}")
        # Intentionally do not call _persist_state_bundle here; use sim_events only.
        return out

    @app.get("/api/process/stages")
    def process_stages() -> dict[str, Any]:
        _ensure_state_initialized()
        return {"stages": get_state().get("stages", [])}

    @app.post("/api/process/optimize")
    def process_optimize(req: ProcessOptimizeRequest) -> dict[str, Any]:
        # DB is the source of truth for optimization input state.
        _ensure_state_initialized()
        state = get_state()
        opt_req = OptimizeRequest(
            silos=state.get("silos", []),
            layers=state.get("layers", []),
            suppliers=state.get("suppliers", []),
            discharge=[],
            config=req.config,
            target_params=req.target_params,
            iterations=req.iterations,
            seed=req.seed,
        )
        return optimize(opt_req)

    @app.post("/api/process/apply_discharge")
    def process_apply_discharge(req: ProcessApplyDischargeRequest) -> dict[str, Any]:
        _ensure_state_initialized()
        state = get_state()
        before_state = state
        if not req.discharge:
            raise HTTPException(status_code=422, detail="discharge plan is required.")
        discharge_df = pd.DataFrame(req.discharge)
        if "silo_id" not in discharge_df.columns:
            raise HTTPException(status_code=422, detail="discharge rows need silo_id.")
        discharge_by_silo: dict[str, float] = {}
        for _, row in discharge_df.iterrows():
            sid = str(row["silo_id"])
            if pd.notna(row.get("discharge_mass_kg")):
                discharge_by_silo[sid] = max(0.0, float(row["discharge_mass_kg"]))
            elif pd.notna(row.get("discharge_fraction")):
                frac = float(row["discharge_fraction"])
                if frac < 0 or frac > 1:
                    raise HTTPException(status_code=422, detail=f"{sid} discharge_fraction must be in [0,1]")
                mass_total = sum(
                    float(x.get("remaining_mass_kg", x.get("segment_mass_kg", 0.0)))
                    for x in state.get("layers", [])
                    if str(x.get("silo_id", "")) == sid
                )
                discharge_by_silo[sid] = frac * mass_total
            else:
                discharge_by_silo[sid] = 0.0
        available_by_silo = {
            str(s["silo_id"]): sum(
                float(x.get("remaining_mass_kg", x.get("segment_mass_kg", 0.0)))
                for x in state.get("layers", [])
                if str(x.get("silo_id", "")) == str(s["silo_id"])
            )
            for s in state.get("silos", [])
        }
        normalized_rows = _normalize_discharge_to_target(
            rows=[{"silo_id": sid, "discharge_mass_kg": m} for sid, m in discharge_by_silo.items()],
            available_by_silo=available_by_silo,
            target_total_kg=FIXED_DISCHARGE_TARGET_KG,
        )
        discharge_by_silo = {str(r["silo_id"]): float(r["discharge_mass_kg"]) for r in normalized_rows}

        # Predict blend using existing physics core before mutation.
        run_req = RunRequest(
            silos=state.get("silos", []),
            layers=state.get("layers", []),
            suppliers=state.get("suppliers", []),
            discharge=[{"silo_id": k, "discharge_mass_kg": v} for k, v in discharge_by_silo.items()],
            config=req.config,
        )
        predicted = run(run_req)
        predicted_total = float(predicted.get("total_discharged_mass_kg", 0.0))
        if abs(predicted_total - FIXED_DISCHARGE_TARGET_KG) > FIXED_DISCHARGE_TOL_KG:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Predicted discharge is {predicted_total:.3f} kg, expected fixed target "
                    f"{FIXED_DISCHARGE_TARGET_KG:.3f} kg. Adjust config (steps/auto_adjust) and retry."
                ),
            )
        before = summarize_state()
        updated = apply_discharge_to_state(discharge_by_silo)
        after = summarize_state()
        add_stage(
            action="apply_discharge",
            before=before,
            after=after,
            meta={"discharge_by_silo": discharge_by_silo},
        )
        out = {"state": updated, "summary": after, "predicted_run": predicted}
        sim_event_id = _write_sim_event(
            event_type="apply_discharge",
            action="apply_discharge",
            state_before=before_state,
            state_after=updated,
            discharge_by_silo=discharge_by_silo,
            total_discharged_mass_kg=float(predicted.get("total_discharged_mass_kg", 0.0)),
            total_remaining_mass_kg=float(predicted.get("total_remaining_mass_kg", 0.0)),
            incoming_queue_count=int(after.get("incoming_queue", {}).get("count", 0)),
            incoming_queue_mass_kg=float(after.get("incoming_queue", {}).get("total_mass_kg", 0.0)),
            meta={"source": "process_apply_discharge"},
        )
        try:
            execute(
                """
                INSERT INTO discharge_results (
                    sim_event_id,
                    discharge_by_silo,
                    predicted_run,
                    summary_before,
                    summary_after
                )
                VALUES (%s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
                """,
                (
                    sim_event_id,
                    json.dumps(discharge_by_silo),
                    json.dumps(predicted),
                    json.dumps(before),
                    json.dumps(after),
                ),
            )
        except Exception as e:
            print(f"[apply_discharge] discharge_results insert failed: {e}")
        try:
            _sync_layers_to_db(updated, event_type="apply_discharge", sim_event_id=sim_event_id)
        except Exception as e:
            print(f"[apply_discharge] layers sync failed: {e}")
        _persist_result("apply_discharge_predicted", predicted, payload={"discharge_by_silo": discharge_by_silo})
        # Intentionally do not call _persist_state_bundle here; use sim_events/discharge tables.
        return out

    @app.post("/api/schedules/generate")
    def generate_schedule(req: GenerateScheduleRequest) -> dict[str, Any]:
        ensure_db_schema()
        count = max(1, min(50, int(req.brews_count)))
        schedule_id = (req.schedule_id or f"sched_{req.seed}_{count}").strip()
        if not schedule_id:
            raise HTTPException(status_code=422, detail="schedule_id cannot be empty")
        target_fixed = dict(DEFAULT_SCHEDULE_TARGET_PARAMS)
        for k, v in (req.target_params or {}).items():
            target_fixed[str(k)] = float(v)
        items: list[dict[str, Any]] = []
        with get_conn() as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO brew_schedules (schedule_id, name, status)
                    VALUES (%s, %s, 'active')
                    ON CONFLICT (schedule_id)
                    DO UPDATE SET name = EXCLUDED.name, status = 'active', updated_at = NOW()
                    """,
                    (schedule_id, req.name),
                )
                conn.execute("DELETE FROM brew_schedule_items WHERE schedule_id = %s", (schedule_id,))
                for i in range(count):
                    brew_id = f"BREW{i+1:03d}"
                    conn.execute(
                        """
                        INSERT INTO brew_schedule_items (
                            schedule_id, brew_id, brew_index, target_params, target_discharge_kg, status
                        )
                        VALUES (%s, %s, %s, %s::jsonb, %s, 'pending')
                        """,
                        (schedule_id, brew_id, i + 1, json.dumps(target_fixed), FIXED_DISCHARGE_TARGET_KG),
                    )
                    items.append(
                        {
                            "brew_id": brew_id,
                            "brew_index": i + 1,
                            "target_params": target_fixed,
                            "status": "pending",
                        }
                    )
        return {"schedule_id": schedule_id, "name": req.name, "items": items}

    @app.get("/api/schedules/{schedule_id}")
    def get_schedule(schedule_id: str) -> dict[str, Any]:
        ensure_db_schema()
        head = fetchall(
            "SELECT schedule_id, name, status, created_at, updated_at FROM brew_schedules WHERE schedule_id = %s",
            (schedule_id,),
        )
        if not head:
            raise HTTPException(status_code=404, detail="schedule not found")
        rows = fetchall(
            """
            SELECT id, brew_id, brew_index, target_params, target_discharge_kg, status, selected_candidate_index, applied_event_id
            FROM brew_schedule_items
            WHERE schedule_id = %s
            ORDER BY brew_index
            """,
            (schedule_id,),
        )
        return {"schedule": head[0], "items": rows}

    @app.post("/api/schedules/{schedule_id}/items/{brew_id}/optimize")
    def optimize_schedule_item(schedule_id: str, brew_id: str, req: ScheduleOptimizeRequest) -> dict[str, Any]:
        ensure_db_schema()
        rows = fetchall(
            """
            SELECT id, target_params
            FROM brew_schedule_items
            WHERE schedule_id = %s AND brew_id = %s
            """,
            (schedule_id, brew_id),
        )
        if not rows:
            raise HTTPException(status_code=404, detail="schedule item not found")
        target_params = rows[0].get("target_params", {}) or {}
        _ensure_state_initialized()
        state = get_state()
        opt_req = OptimizeRequest(
            silos=state.get("silos", []),
            layers=state.get("layers", []),
            suppliers=state.get("suppliers", []),
            discharge=[],
            config=req.config,
            target_params=target_params,
            iterations=req.iterations,
            seed=req.seed,
        )
        out = optimize(opt_req)
        execute(
            """
            UPDATE brew_schedule_items
            SET status = 'optimized', optimize_result = %s::jsonb, updated_at = NOW()
            WHERE schedule_id = %s AND brew_id = %s
            """,
            (json.dumps(out), schedule_id, brew_id),
        )
        return out

    @app.post("/api/schedules/{schedule_id}/items/{brew_id}/apply")
    def apply_schedule_item(schedule_id: str, brew_id: str, req: ScheduleApplyRequest) -> dict[str, Any]:
        ensure_db_schema()
        rows = fetchall(
            """
            SELECT optimize_result
            FROM brew_schedule_items
            WHERE schedule_id = %s AND brew_id = %s
            """,
            (schedule_id, brew_id),
        )
        if not rows:
            raise HTTPException(status_code=404, detail="schedule item not found")
        opt_result = rows[0].get("optimize_result", {}) or {}
        top_candidates = opt_result.get("top_candidates", []) or []
        idx = int(req.candidate_index)
        if idx < 0 or idx >= len(top_candidates):
            raise HTTPException(status_code=422, detail="invalid candidate_index for schedule item")
        discharge_plan = top_candidates[idx].get("recommended_discharge", []) or []
        if not discharge_plan:
            raise HTTPException(status_code=422, detail="selected candidate has empty recommended_discharge")

        before_id_rows = fetchall("SELECT COALESCE(MAX(id), 0) AS id FROM sim_events")
        before_id = int(before_id_rows[0].get("id", 0)) if before_id_rows else 0
        out = process_apply_discharge(
            ProcessApplyDischargeRequest(discharge=discharge_plan, config=req.config)
        )
        after_id_rows = fetchall("SELECT COALESCE(MAX(id), 0) AS id FROM sim_events")
        after_id = int(after_id_rows[0].get("id", 0)) if after_id_rows else before_id
        applied_event_id = after_id if after_id > before_id else None
        execute(
            """
            UPDATE brew_schedule_items
            SET status = 'applied',
                selected_candidate_index = %s,
                applied_event_id = %s,
                updated_at = NOW()
            WHERE schedule_id = %s AND brew_id = %s
            """,
            (idx, applied_event_id, schedule_id, brew_id),
        )
        return {"applied": True, "candidate_index": idx, "applied_event_id": applied_event_id, "result": out}

    @app.post("/api/validate")
    def validate(req: RunRequest) -> dict[str, Any]:
        layers_df = pd.DataFrame(req.layers)
        # Fill-first mode can legitimately start with no layers.
        # Provide required columns so schema validation focuses on provided data.
        if layers_df.empty:
            layers_df = pd.DataFrame(
                columns=["silo_id", "layer_index", "lot_id", "supplier", "segment_mass_kg"]
            )
        inputs = {
            "silos": pd.DataFrame(req.silos),
            "layers": layers_df,
            "suppliers": pd.DataFrame(req.suppliers),
            "discharge": pd.DataFrame(req.discharge),
        }
        errors = validate_inputs_shape(inputs)
        return {"valid": len(errors) == 0, "errors": errors}

    @app.post("/api/run")
    def run(req: RunRequest) -> dict[str, Any]:
        inputs = {
            "silos": pd.DataFrame(req.silos),
            "layers": pd.DataFrame(req.layers),
            "suppliers": pd.DataFrame(req.suppliers),
            "discharge": pd.DataFrame(req.discharge),
        }
        errors = validate_inputs_shape(inputs)
        if errors:
            raise HTTPException(status_code=422, detail=errors)

        cfg = RunConfig(**req.config)
        result = run_blend(inputs, cfg)
        out = _result_to_api_payload(result)
        sim_event_id = _write_sim_event(
            event_type="run",
            action="run",
            state_before={},
            state_after={},
            total_discharged_mass_kg=float(out.get("total_discharged_mass_kg", 0.0)),
            total_remaining_mass_kg=float(out.get("total_remaining_mass_kg", 0.0)),
            incoming_queue_count=None,
            incoming_queue_mass_kg=None,
            meta={"source": "api_run"},
        )
        try:
            seg = result.get("df_segment_state_ledger")
            if seg is not None and not seg.empty:
                run_layers = [
                    {
                        "silo_id": str(r.get("silo_id", "")),
                        "layer_index": int(r.get("layer_index", 0) or 0),
                        "lot_id": str(r.get("lot_id", "")),
                        "supplier": str(r.get("supplier", "")),
                        "remaining_mass_kg": float(r.get("remaining_mass_kg", 0.0) or 0.0),
                    }
                    for r in seg.to_dict(orient="records")
                ]
                _sync_layers_to_db(
                    state={"silos": req.silos, "layers": run_layers},
                    event_type="run_simulation",
                    sim_event_id=sim_event_id,
                )
        except Exception:
            pass
        _persist_result("run", out, payload=req.model_dump())
        return out

    @app.post("/api/optimize")
    def optimize(req: OptimizeRequest) -> dict[str, Any]:
        started_at = time.perf_counter()
        inputs = {
            "silos": pd.DataFrame(req.silos),
            "layers": pd.DataFrame(req.layers),
            "suppliers": pd.DataFrame(req.suppliers),
            "discharge": pd.DataFrame(req.discharge),
        }
        inputs = _ensure_discharge_has_silo_ids(inputs)
        errors = validate_inputs_shape(inputs)
        if errors:
            raise HTTPException(status_code=422, detail=errors)
        if not req.target_params:
            raise HTTPException(status_code=422, detail="target_params must be provided.")

        cfg = RunConfig(**req.config)
        silos_df = inputs["silos"].copy()
        layers_df = inputs["layers"].copy()
        available_by_silo = _available_mass_by_silo(layers_df)
        available_total = float(sum(available_by_silo.values()))
        if available_total + 1e-12 < FIXED_DISCHARGE_TARGET_KG:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Insufficient available mass for fixed optimization target {FIXED_DISCHARGE_TARGET_KG:.3f} kg. "
                    f"Currently available: {available_total:.3f} kg."
                ),
            )
        silo_ids = silos_df["silo_id"].astype(str).tolist()
        rng = random.Random(req.seed)
        total_iter = max(1, req.iterations)
        explore_iters = max(1, int(total_iter * 0.6))
        exploit_iters = total_iter - explore_iters
        best_score = float("inf")
        best_result: dict[str, Any] | None = None
        best_discharge: list[dict[str, Any]] = []
        top_candidates: list[dict[str, Any]] = []
        best_fractions: list[float] = []

        def evaluate_fractions(fracs: list[float]) -> None:
            nonlocal best_score, best_result, best_discharge, best_fractions
            candidate_rows = _candidate_rows_from_fractions(silo_ids, fracs)
            candidate_rows = _normalize_discharge_to_target(
                rows=[
                    {
                        "silo_id": str(r["silo_id"]),
                        "discharge_mass_kg": float(r["discharge_fraction"]) * float(available_by_silo.get(str(r["silo_id"]), 0.0)),
                    }
                    for r in candidate_rows
                ],
                available_by_silo=available_by_silo,
                target_total_kg=FIXED_DISCHARGE_TARGET_KG,
            )
            candidate_inputs = dict(inputs)
            candidate_inputs["discharge"] = pd.DataFrame(candidate_rows)
            result = run_blend(candidate_inputs, cfg)
            discharged_total = float(result["total_discharged_mass_kg"])
            if abs(discharged_total - FIXED_DISCHARGE_TARGET_KG) > FIXED_DISCHARGE_TOL_KG:
                # Reject candidates that cannot physically meet the fixed-target discharge.
                return
            score = _score_blend_vectorised(
                actual=result["total_blended_params"],
                target=req.target_params,
                param_ranges=DEFAULT_PARAM_RANGES,
            )
            candidate_record = {
                "objective_score": score,
                "recommended_discharge": candidate_rows,
                "blended_params": {
                    k: float(v) for k, v in result["total_blended_params"].items()
                },
                "total_discharged_mass_kg": discharged_total,
            }
            top_candidates.append(candidate_record)
            if score < best_score:
                best_score = score
                best_result = result
                best_discharge = candidate_rows
                best_fractions = [float(c["discharge_fraction"]) for c in candidate_rows]

        # Explore: stratified random sampling in [0.2, 0.8] to improve coverage.
        for i in range(explore_iters):
            band_lo = DISCHARGE_FRACTION_MIN + (
                (DISCHARGE_FRACTION_MAX - DISCHARGE_FRACTION_MIN) * i / explore_iters
            )
            band_hi = DISCHARGE_FRACTION_MIN + (
                (DISCHARGE_FRACTION_MAX - DISCHARGE_FRACTION_MIN) * (i + 1) / explore_iters
            )
            fractions = [rng.uniform(band_lo, band_hi) for _ in silo_ids]
            rng.shuffle(fractions)
            evaluate_fractions(fractions)

        # Exploit: local perturbation around the current best solution.
        if not best_fractions:
            best_fractions = [0.5 for _ in silo_ids]
        for i in range(exploit_iters):
            anneal = 1.0 - (i / max(1, exploit_iters))
            step = 0.12 * anneal + 0.01
            trial = [_clip_fraction(f + rng.uniform(-step, step)) for f in best_fractions]
            evaluate_fractions(trial)

        if best_result is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"No feasible optimization candidate can achieve fixed discharge target "
                    f"{FIXED_DISCHARGE_TARGET_KG:.3f} kg with current state/config."
                ),
            )

        if top_candidates:
            batch_scores = _score_batch(
                candidates=top_candidates,
                target=req.target_params,
                param_ranges=DEFAULT_PARAM_RANGES,
            )
            for cand, sc in zip(top_candidates, batch_scores):
                cand["objective_score"] = float(sc)
        top_candidates = _diverse_top_k(top_candidates, k=5)
        out = {
            "objective_score": best_score,
            "recommended_discharge": best_discharge,
            "best_run": _result_to_api_payload(best_result),
            "target_params": req.target_params,
            "fixed_discharge_target_kg": FIXED_DISCHARGE_TARGET_KG,
            "iterations": req.iterations,
            "iterations_effective": total_iter,
            "explore_iterations": explore_iters,
            "exploit_iterations": exploit_iters,
            "objective_method": "normalized_weighted_l2_hybrid_search",
            "param_ranges": DEFAULT_PARAM_RANGES,
            "top_candidates": top_candidates,
            "config_used": {
                "rho_bulk_kg_m3": float(cfg.rho_bulk_kg_m3),
                "grain_diameter_m": float(cfg.grain_diameter_m),
                "beverloo_c": float(cfg.beverloo_c),
                "beverloo_k": float(cfg.beverloo_k),
                "gravity_m_s2": float(cfg.gravity_m_s2),
                "sigma_m": float(cfg.sigma_m),
                "steps": int(cfg.steps),
                "auto_adjust": bool(cfg.auto_adjust),
            },
        }
        out["elapsed_ms"] = round((time.perf_counter() - started_at) * 1000.0, 2)
        sim_event_id = _write_sim_event(
            event_type="optimize",
            action="optimize",
            total_discharged_mass_kg=float(out.get("best_run", {}).get("total_discharged_mass_kg", 0.0)),
            total_remaining_mass_kg=float(out.get("best_run", {}).get("total_remaining_mass_kg", 0.0)),
            objective_score=float(out.get("objective_score", 0.0)),
            meta={
                "source": "api_optimize",
                "elapsed_ms": out.get("elapsed_ms"),
                "iterations_effective": total_iter,
                "explore_iterations": explore_iters,
                "exploit_iterations": exploit_iters,
                "steps": int(cfg.steps),
            },
        )
        try:
            execute(
                """
                INSERT INTO results_optimize (
                    sim_event_id,
                    objective_score,
                    recommended_discharge,
                    target_params,
                    top_candidates,
                    best_run
                )
                VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
                """,
                (
                    sim_event_id,
                    float(out["objective_score"]),
                    json.dumps(out["recommended_discharge"]),
                    json.dumps(out["target_params"]),
                    json.dumps(out["top_candidates"]),
                    json.dumps(out["best_run"]),
                ),
            )
        except Exception:
            pass
        _persist_result("optimize", out, payload=req.model_dump())
        return out

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(ui_dir / "index.html")

    return app


def run() -> None:
    parser = argparse.ArgumentParser(description="Run DEM simulation FastAPI server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true", default=False)
    args = parser.parse_args()

    uvicorn.run(
        "dem_sim.web:create_app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        factory=True,
    )


if __name__ == "__main__":
    run()

from __future__ import annotations

import argparse
import csv
import json
import random
import time
from copy import deepcopy
from io import StringIO
from math import isfinite, isnan
from pathlib import Path
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

from .reporting import validate_inputs_shape, validate_supplier_coa
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
    plan_run_id: str | None = None,
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
                        plan_run_id,
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
                    VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb)
                    RETURNING id
                    """,
                    (
                        plan_run_id,
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
    iterations: int = 80
    seed: int = 42
    use_latest_state: bool = False
    include_all_candidates: bool = False
    plan_run_id: str | None = None


class ProcessRunSimulationRequest(BaseModel):
    silos: list[dict[str, Any]] = Field(default_factory=list)
    layers: list[dict[str, Any]] = Field(default_factory=list)
    suppliers: list[dict[str, Any]] = Field(default_factory=list)
    incoming_queue: list[dict[str, Any]] = Field(default_factory=list)


class ProcessOptimizeRequest(BaseModel):
    target_params: dict[str, float] = Field(default_factory=dict)
    iterations: int = 80
    seed: int = 42
    config: dict[str, Any] = Field(default_factory=dict)
    include_all_candidates: bool = False


class ProcessApplyDischargeRequest(BaseModel):
    discharge: list[dict[str, Any]] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)
    plan_run_id: str | None = None


class GenerateRandomDataRequest(BaseModel):
    seed: int = 42
    silos_count: int = 6
    lots_count: int = 100
    lot_size_kg: float = 25000.0


class GenerateScheduleRequest(BaseModel):
    schedule_id: str | None = None
    name: str = "MVP Brew Schedule"
    brews_count: int = 7
    seed: int = 42
    target_params: dict[str, float] = Field(default_factory=dict)


class ScheduleOptimizeRequest(BaseModel):
    iterations: int = 80
    seed: int = 42
    config: dict[str, Any] = Field(default_factory=dict)
    include_all_candidates: bool = False


class ScheduleApplyRequest(BaseModel):
    candidate_index: int = 0
    config: dict[str, Any] = Field(default_factory=dict)


class ProductionPlanLoadRequest(BaseModel):
    plan_run_id: str | None = None
    seed: int = 42
    silos_count: int = 3
    lots_count: int = 100
    lot_size_kg: float = 25000.0
    schedule_id: str | None = None
    name: str = "MVP Brew Schedule"
    brews_count: int = 7
    target_params: dict[str, float] = Field(default_factory=dict)
    optimize_iterations: int = 80
    optimize_seed: int = 42
    config: dict[str, Any] = Field(default_factory=lambda: {"steps": 800})
    include_all_candidates: bool = False


class ProductionPlanOptimizeRequest(BaseModel):
    iterations: int = 80
    seed: int = 42
    config: dict[str, Any] = Field(default_factory=dict)
    include_all_candidates: bool = False
    expected_last_event_id: int | None = None


class ProductionPlanApplyRequest(BaseModel):
    candidate_index: int = 0
    config: dict[str, Any] = Field(default_factory=dict)
    expected_last_event_id: int | None = None


DEFAULT_SCHEDULE_TARGET_PARAMS = {
    "moisture_pct": 4.50,
    "fine_extract_db_pct": 81.00,
    "wort_pH": 5.80,
    "diastatic_power_WK": 250.0,
    "total_protein_pct": 10.80,
    "wort_colour_EBC": 3.50,
}

ACTIVE_RUN_STATUSES = {"active", "awaiting_apply", "awaiting_optimization", "in_progress"}


def _new_plan_run_id(schedule_id: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(schedule_id or "plan"))
    slug = slug.strip("_") or "plan"
    return f"run_{slug}_{int(time.time() * 1000)}"


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _preferred_brewmaster_candidate(optimize_result: dict[str, Any]) -> dict[str, Any] | None:
    recommendation = _json_obj(optimize_result.get("brewmaster_recommendation"))
    if not recommendation:
        return None
    if recommendation.get("prob_selected") is None:
        return None
    return recommendation


def _json_sanitize_non_finite(value: Any) -> Any:
    """Recursively replace NaN/Inf values so payloads are valid json/jsonb."""
    if isinstance(value, dict):
        return {k: _json_sanitize_non_finite(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_sanitize_non_finite(v) for v in value]
    if isinstance(value, float):
        return value if isfinite(value) else None
    return value


def _json_dumps_safe(value: Any) -> str:
    """JSON dump compatible with Postgres jsonb (no NaN/Inf tokens)."""
    return json.dumps(_json_sanitize_non_finite(value), allow_nan=False)


def _summarize_state_snapshot(state: dict[str, Any]) -> dict[str, Any]:
    silos = deepcopy(state.get("silos", []))
    layers = deepcopy(state.get("layers", []))
    by_silo: dict[str, dict[str, Any]] = {}
    for s in silos:
        sid = str(s.get("silo_id", ""))
        if not sid:
            continue
        cap = float(s.get("capacity_kg", 0.0) or 0.0)
        by_silo[sid] = {
            "silo_id": sid,
            "capacity_kg": cap,
            "used_kg": 0.0,
            "remaining_kg": cap,
            "remaining_pct": 100.0 if cap > 0 else 0.0,
            "lots": [],
        }
    for row in sorted(layers, key=lambda r: (str(r.get("silo_id", "")), int(r.get("layer_index", 0) or 0))):
        sid = str(row.get("silo_id", ""))
        if sid not in by_silo:
            continue
        mass = float(row.get("remaining_mass_kg", row.get("segment_mass_kg", 0.0)) or 0.0)
        by_silo[sid]["used_kg"] += mass
        by_silo[sid]["lots"].append(
            {
                "layer_index": int(row.get("layer_index", 0) or 0),
                "lot_id": str(row.get("lot_id", "")),
                "supplier": str(row.get("supplier", "")),
                "remaining_mass_kg": mass,
            }
        )
    for rec in by_silo.values():
        cap = float(rec["capacity_kg"])
        remaining = cap - float(rec["used_kg"])
        rec["remaining_kg"] = max(0.0, 0.0 if abs(remaining) <= 1e-6 else remaining)
        rec["remaining_pct"] = max(0.0, min(100.0, (rec["remaining_kg"] / cap * 100.0) if cap > 0 else 0.0))
        active_lots = [lot for lot in rec["lots"] if float(lot.get("remaining_mass_kg", 0.0)) > 1e-9]
        active_lots.sort(key=lambda lot: int(lot.get("layer_index", 0)))
        for idx, lot in enumerate(active_lots, start=1):
            lot["current_layer_index"] = idx
        rec["lots"] = active_lots
    queue = deepcopy(state.get("incoming_queue", []))
    return {
        "silos": list(by_silo.values()),
        "incoming_queue": {
            "count": len(queue),
            "total_mass_kg": float(sum(float(x.get("mass_kg", 0.0) or 0.0) for x in queue)),
        },
        "cumulative_discharged_kg": float(state.get("cumulative_discharged_kg", 0.0) or 0.0),
    }


def _latest_plan_state(plan_run_id: str) -> dict[str, Any] | None:
    rows = fetchall(
        """
        SELECT state_after
        FROM sim_events
        WHERE plan_run_id = %s
        ORDER BY id DESC
        LIMIT 100
        """,
        (plan_run_id,),
    )
    for row in rows:
        state_after = _json_obj(row.get("state_after"))
        if isinstance(state_after.get("silos"), list) and isinstance(state_after.get("layers"), list):
            return state_after
    return None


def _activate_plan_run_state(plan_run_id: str) -> dict[str, Any]:
    state = _latest_plan_state(plan_run_id)
    if not state:
        raise HTTPException(status_code=404, detail="No persisted state found for plan run.")
    reset_state()
    set_state(
        silos=state.get("silos", []),
        layers=state.get("layers", []),
        suppliers=state.get("suppliers", []),
        incoming_queue=state.get("incoming_queue", []),
        action="activate_plan_run_state",
        meta={"plan_run_id": plan_run_id},
    )
    live = get_state()
    live["cumulative_discharged_kg"] = float(state.get("cumulative_discharged_kg", live.get("cumulative_discharged_kg", 0.0)) or 0.0)
    return live


def _plan_run_head(plan_run_id: str) -> dict[str, Any]:
    rows = fetchall(
        """
        SELECT plan_run_id, schedule_id, name, status, current_stage, current_message, progress_pct,
               current_brew_id, current_brew_index,
               last_event_id, created_at, updated_at, completed_at, meta
        FROM production_plan_runs
        WHERE plan_run_id = %s
        """,
        (plan_run_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="production plan run not found")
    row = dict(rows[0])
    row["meta"] = _json_obj(row.get("meta"))
    return row


def _schedule_items(schedule_id: str) -> list[dict[str, Any]]:
    rows = fetchall(
        """
        SELECT id, brew_id, brew_index, target_params, target_discharge_kg, status,
               optimize_result, selected_candidate_index, applied_event_id, created_at, updated_at
        FROM brew_schedule_items
        WHERE schedule_id = %s
        ORDER BY brew_index
        """,
        (schedule_id,),
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["target_params"] = _json_obj(item.get("target_params"))
        item["optimize_result"] = _json_obj(item.get("optimize_result"))
        out.append(item)
    return out


def _pick_current_brew(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    for item in items:
        if str(item.get("status", "")) == "optimized":
            return item
    for item in items:
        if str(item.get("status", "")) != "applied":
            return item
    return None


def _update_plan_run(
    plan_run_id: str,
    *,
    status: str | None = None,
    current_stage: str | None = None,
    current_message: str | None = None,
    progress_pct: float | None = None,
    current_brew_id: str | None = None,
    current_brew_index: int | None = None,
    last_event_id: int | None = None,
    completed: bool = False,
) -> None:
    existing = _plan_run_head(plan_run_id)
    execute(
        """
        UPDATE production_plan_runs
        SET status = %s,
            current_stage = %s,
            current_message = %s,
            progress_pct = %s,
            current_brew_id = %s,
            current_brew_index = %s,
            last_event_id = %s,
            updated_at = NOW(),
            completed_at = CASE WHEN %s THEN NOW() ELSE completed_at END
        WHERE plan_run_id = %s
        """,
        (
            status or existing.get("status"),
            current_stage or existing.get("current_stage"),
            current_message if current_message is not None else existing.get("current_message"),
            float(progress_pct) if progress_pct is not None else float(existing.get("progress_pct", 0.0) or 0.0),
            current_brew_id if current_brew_id is not None else existing.get("current_brew_id"),
            current_brew_index if current_brew_index is not None else existing.get("current_brew_index"),
            last_event_id if last_event_id is not None else existing.get("last_event_id"),
            completed,
            plan_run_id,
        ),
    )


def _assert_expected_last_event(plan_run_id: str, expected_last_event_id: int | None) -> None:
    if expected_last_event_id is None:
        return
    head = _plan_run_head(plan_run_id)
    current = head.get("last_event_id")
    current_num = int(current) if current is not None else None
    if current_num != int(expected_last_event_id):
        raise HTTPException(
            status_code=409,
            detail=f"Production plan state advanced from event {expected_last_event_id} to {current_num}. Refresh and retry.",
        )


def _plan_run_response(plan_run_id: str) -> dict[str, Any]:
    head = _plan_run_head(plan_run_id)
    items = _schedule_items(str(head.get("schedule_id", "")))
    current_brew = _pick_current_brew(items)
    state = _latest_plan_state(plan_run_id) or {"silos": [], "layers": [], "suppliers": [], "incoming_queue": []}
    summary = _summarize_state_snapshot(state)
    scenarios: list[dict[str, Any]] = []
    target_params: dict[str, Any] = {}
    preferred_candidate: dict[str, Any] | None = None
    if current_brew:
        target_params = _json_obj(current_brew.get("target_params"))
        optimize_result = _json_obj(current_brew.get("optimize_result"))
        scenarios = _json_list(optimize_result.get("top_candidates"))
        preferred_candidate = _preferred_brewmaster_candidate(optimize_result)
    return {
        "plan_run": head,
        "schedule": {
            "schedule_id": head.get("schedule_id"),
            "name": head.get("name"),
        },
        "brews": [
            {
                "brew_id": item.get("brew_id"),
                "brew_index": item.get("brew_index"),
                "status": item.get("status"),
                "selected_candidate_index": item.get("selected_candidate_index"),
                "applied_event_id": item.get("applied_event_id"),
                "target_params": _json_obj(item.get("target_params")),
            }
            for item in items
        ],
        "current_brew": (
            {
                "brew_id": current_brew.get("brew_id"),
                "brew_index": current_brew.get("brew_index"),
                "status": current_brew.get("status"),
                "target_params": target_params,
                "selected_candidate_index": current_brew.get("selected_candidate_index"),
            }
            if current_brew
            else None
        ),
        "preferred_candidate": preferred_candidate,
        "scenarios": scenarios,
        "inventory": {"state": state, "summary": summary},
    }


def _workflow_progress(schedule_id: str) -> float:
    items = _schedule_items(schedule_id)
    total = len(items)
    if total <= 0:
        return 0.0
    completed = sum(1 for item in items if str(item.get("status", "")) == "applied")
    return round((completed / total) * 100.0, 2)


def _stream_signature(plan_run_id: str) -> str:
    rows = fetchall(
        """
        SELECT updated_at, current_stage, current_message, progress_pct, current_brew_id, current_brew_index, last_event_id, status
        FROM production_plan_runs
        WHERE plan_run_id = %s
        """,
        (plan_run_id,),
    )
    if not rows:
        return "missing"
    row = rows[0]
    return json.dumps(
        {
            "updated_at": str(row.get("updated_at")),
            "current_stage": row.get("current_stage"),
            "current_message": row.get("current_message"),
            "progress_pct": row.get("progress_pct"),
            "current_brew_id": row.get("current_brew_id"),
            "current_brew_index": row.get("current_brew_index"),
            "last_event_id": row.get("last_event_id"),
            "status": row.get("status"),
        },
        sort_keys=True,
    )


def _optimize_brew_for_plan_run(
    *,
    optimize_fn: Any,
    plan_run_id: str,
    schedule_id: str,
    brew_id: str,
    target_params: dict[str, Any],
    iterations: int,
    seed: int,
    config: dict[str, Any],
    include_all_candidates: bool,
) -> tuple[dict[str, Any], int | None]:
    opt_out = optimize_fn(
        OptimizeRequest(
            silos=get_state().get("silos", []),
            layers=get_state().get("layers", []),
            suppliers=get_state().get("suppliers", []),
            discharge=[],
            config=config,
            target_params={str(k): float(v) for k, v in (target_params or {}).items()},
            iterations=iterations,
            seed=seed,
            use_latest_state=False,
            include_all_candidates=include_all_candidates,
            plan_run_id=plan_run_id,
        )
    )
    execute(
        """
        UPDATE brew_schedule_items
        SET status = 'optimized', optimize_result = %s::jsonb, updated_at = NOW()
        WHERE schedule_id = %s AND brew_id = %s
        """,
        (_json_dumps_safe(opt_out), schedule_id, brew_id),
    )
    optimize_rows = fetchall(
        """
        SELECT id
        FROM sim_events
        WHERE plan_run_id = %s AND event_type = 'optimize'
        ORDER BY id DESC
        LIMIT 1
        """,
        (plan_run_id,),
    )
    optimize_event_id = int(optimize_rows[0].get("id")) if optimize_rows else None
    return opt_out, optimize_event_id


def _generate_random_payload(
    *, seed: int, silos_count: int, lots_count: int, lot_size_kg: float
) -> dict[str, Any]:
    rng = random.Random(seed)
    silos_count = max(1, int(silos_count))
    lots_count = max(1, int(lots_count))
    lot_size_kg = max(1.0, float(lot_size_kg))

    silos: list[dict[str, Any]] = []
    fixed_body_diameter_m = 3.1
    fixed_outlet_diameter_m = 0.2
    for i in range(silos_count):
        silos.append(
            {
                "silo_id": f"S{i+1}",
                "capacity_kg": float(100000.0),
                "body_diameter_m": float(fixed_body_diameter_m),
                "outlet_diameter_m": float(fixed_outlet_diameter_m),
                "initial_mass_kg": 0.0,
            }
        )

    spec_ranges = {
        # One-sided spec style inputs with broader bands for visible optimization diversity.
        # We intentionally keep variability on both sides of the threshold.
        "moisture_pct": {"low": 2.0, "high": 5.0},
        "fine_extract_db_pct": {"low": 79.0, "high": 87.0},
        "wort_pH": {"low": 5.5, "high": 6.0},
        "diastatic_power_WK": {"low": 200.0, "high": 325.0},
        "total_protein_pct": {"low": 9.5, "high": 11.5},
        "wort_colour_EBC": {"low": 2.7, "high": 4.5},
    }

    def _sample_param(key: str) -> float:
        spec = spec_ranges[key]
        low = float(spec["low"])
        high = float(spec["high"])
        return float(rng.uniform(low, high))

    suppliers_rows: list[dict[str, Any]] = []
    incoming_queue: list[dict[str, Any]] = []
    for i in range(lots_count):
        lot_id = f"LOT{i+1:03d}"
        sup = f"SPEC_{lot_id}"
        lot_spec = {
            "moisture_pct": round(_sample_param("moisture_pct"), 3),
            "fine_extract_db_pct": round(_sample_param("fine_extract_db_pct"), 3),
            "wort_pH": round(_sample_param("wort_pH"), 3),
            "diastatic_power_WK": round(_sample_param("diastatic_power_WK"), 3),
            "total_protein_pct": round(_sample_param("total_protein_pct"), 3),
            "wort_colour_EBC": round(_sample_param("wort_colour_EBC"), 3),
        }
        incoming_queue.append(
            {
                "lot_id": lot_id,
                "supplier": sup,
                "mass_kg": float(lot_size_kg),
                "moisture_pct": float(lot_spec["moisture_pct"]),
                "fine_extract_db_pct": float(lot_spec["fine_extract_db_pct"]),
                "wort_pH": float(lot_spec["wort_pH"]),
                "diastatic_power_WK": float(lot_spec["diastatic_power_WK"]),
                "total_protein_pct": float(lot_spec["total_protein_pct"]),
                "wort_colour_EBC": float(lot_spec["wort_colour_EBC"]),
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
            "steps": 800,
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
                    INSERT INTO suppliers (
                        name, moisture_pct, fine_extract_db_pct, wort_pH, diastatic_power_WK, total_protein_pct,
                        soluble_n_mg_100g, free_amino_n_mg_100g, kolbach_index_pct, beta_glucan_65c_mg_100g, viscosity_mpas,
                        wort_colour_EBC
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(sp.get("supplier", "")),
                        float(sp.get("moisture_pct", 0.0) or 0.0),
                        float(sp.get("fine_extract_db_pct", 0.0) or 0.0),
                        float(sp.get("wort_pH", 0.0) or 0.0),
                        float(sp.get("diastatic_power_WK", 0.0) or 0.0),
                        float(sp.get("total_protein_pct", 0.0) or 0.0),
                        float(sp.get("soluble_n_mg_100g", 0.0) or 0.0),
                        float(sp.get("free_amino_n_mg_100g", 0.0) or 0.0),
                        float(sp.get("kolbach_index_pct", 0.0) or 0.0),
                        float(sp.get("beta_glucan_65c_mg_100g", 0.0) or 0.0),
                        float(sp.get("viscosity_mpas", 0.0) or 0.0),
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
                        soluble_n_mg_100g,
                        free_amino_n_mg_100g,
                        kolbach_index_pct,
                        beta_glucan_65c_mg_100g,
                        viscosity_mpas,
                        wort_colour_EBC
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        float(q.get("soluble_n_mg_100g", 0.0) or 0.0),
                        float(q.get("free_amino_n_mg_100g", 0.0) or 0.0),
                        float(q.get("kolbach_index_pct", 0.0) or 0.0),
                        float(q.get("beta_glucan_65c_mg_100g", 0.0) or 0.0),
                        float(q.get("viscosity_mpas", 0.0) or 0.0),
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


def _write_lot_coas_csv(
    *,
    layers_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
    incoming_queue: list[dict[str, Any]] | None = None,
    output_dir: Path | None = None,
) -> Path | None:
    """Write per-lot COA rows to outputs CSV for the current optimize run."""
    try:
        incoming_queue = incoming_queue or []
        output_dir = output_dir or Path("outputs")
        output_dir.mkdir(parents=True, exist_ok=True)

        coa_cols = [
            "moisture_pct",
            "fine_extract_db_pct",
            "wort_pH",
            "diastatic_power_WK",
            "total_protein_pct",
            "wort_colour_EBC",
        ]
        supplier_map: dict[str, dict[str, Any]] = {}
        if not suppliers_df.empty:
            for r in suppliers_df.to_dict(orient="records"):
                key = str(r.get("supplier", "") or r.get("name", ""))
                if not key:
                    continue
                supplier_map[key] = {c: r.get(c) for c in coa_cols}

        # Incoming queue can carry explicit lot-level COA specs.
        lot_map: dict[str, dict[str, Any]] = {}
        for row in incoming_queue:
            lot_id = str(row.get("lot_id", ""))
            supplier = str(row.get("supplier", ""))
            if not lot_id:
                continue
            lot_map[lot_id] = {
                "lot_id": lot_id,
                "supplier": supplier,
                "source": "lot",
                **{c: row.get(c) for c in coa_cols},
            }

        # Layers list the lot ids but typically do not store lot-level COA.
        if not layers_df.empty and "lot_id" in layers_df.columns:
            for r in layers_df.to_dict(orient="records"):
                lot_id = str(r.get("lot_id", ""))
                if not lot_id:
                    continue
                if lot_id in lot_map:
                    continue
                supplier = str(r.get("supplier", ""))
                sup_coa = supplier_map.get(supplier, {})
                lot_map[lot_id] = {
                    "lot_id": lot_id,
                    "supplier": supplier,
                    "source": "supplier_fallback",
                    **{c: sup_coa.get(c) for c in coa_cols},
                }

        if not lot_map:
            return None

        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out_path = output_dir / f"lot_coas_optimize_{ts}.csv"
        rows = list(lot_map.values())
        fieldnames = ["lot_id", "supplier", "source"] + coa_cols
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return out_path
    except Exception:
        return None


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
                            "moisture_pct": float(qr.get("moisture_pct", 0.0) or 0.0),
                            "fine_extract_db_pct": float(qr.get("fine_extract_db_pct", 0.0) or 0.0),
                            "wort_pH": float(qr.get("wort_pH", 0.0) or 0.0),
                            "diastatic_power_WK": float(qr.get("diastatic_power_WK", 0.0) or 0.0),
                            "total_protein_pct": float(qr.get("total_protein_pct", 0.0) or 0.0),
                            "wort_colour_EBC": float(qr.get("wort_colour_EBC", 0.0) or 0.0),
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
                    "steps": 800,
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
                        {
                            "lot_id": lot_id,
                            "supplier": supplier_name,
                            "mass_kg": remaining_mass_kg,
                            "moisture_pct": float(r.get("moisture_pct", 0.0) or 0.0),
                            "fine_extract_db_pct": float(r.get("fine_extract_db_pct", 0.0) or 0.0),
                            "wort_pH": float(r.get("wort_pH", 0.0) or 0.0),
                            "diastatic_power_WK": float(r.get("diastatic_power_WK", 0.0) or 0.0),
                            "total_protein_pct": float(r.get("total_protein_pct", 0.0) or 0.0),
                            "wort_colour_EBC": float(r.get("wort_colour_EBC", 0.0) or 0.0),
                        }
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
                    "steps": 800,
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
            "steps": 800,
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
             , moisture_pct, fine_extract_db_pct, wort_pH, diastatic_power_WK, total_protein_pct
             , soluble_n_mg_100g, free_amino_n_mg_100g, kolbach_index_pct, beta_glucan_65c_mg_100g, viscosity_mpas
             , wort_colour_EBC
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
            "moisture_pct": float(r.get("moisture_pct", 0.0) or 0.0),
            "fine_extract_db_pct": float(r.get("fine_extract_db_pct", 0.0) or 0.0),
            "wort_pH": float(r.get("wort_pH", 0.0) or 0.0),
            "diastatic_power_WK": float(r.get("diastatic_power_WK", 0.0) or 0.0),
            "total_protein_pct": float(r.get("total_protein_pct", 0.0) or 0.0),
            "wort_colour_EBC": float(r.get("wort_colour_EBC", 0.0) or 0.0),
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
        "total_blended_params": _visible_blended_params(result["total_blended_params"]),
        "silo_state_ledger": result["df_silo_state_ledger"].to_dict(orient="records"),
        "per_silo": {
            silo_id: {
                "discharged_mass_kg": float(r["discharged_mass_kg"]),
                "mass_flow_rate_kg_s": float(r["mass_flow_rate_kg_s"]),
                "discharge_time_s": float(r["discharge_time_s"]),
                "sigma_m": float(r["sigma_m"]),
                "blended_params_per_silo": _visible_blended_params(
                    r["blended_params_per_silo"]
                ),
            }
            for silo_id, r in result["per_silo"].items()
        },
    }


def _ensure_suppliers_dataframe(inputs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    suppliers_df = inputs.get("suppliers", pd.DataFrame())
    if not suppliers_df.empty and ("supplier" in suppliers_df.columns or "name" in suppliers_df.columns):
        return inputs
    inferred_rows: list[dict[str, Any]] = []
    try:
        queue_rows = fetchall("SELECT * FROM incoming_queue ORDER BY id")
        inferred_rows = _suppliers_from_incoming_queue_rows(queue_rows)
    except Exception:
        inferred_rows = []
    if inferred_rows:
        inputs["suppliers"] = pd.DataFrame(inferred_rows)
    return inputs


DEFAULT_PARAM_RANGES = {
    "moisture_pct": 5.0 - 0.0,
    "fine_extract_db_pct": 83.0 - 81.0,
    "wort_pH": 6.0 - 5.8,
    "diastatic_power_WK": 360.0 - 300.0,
    "total_protein_pct": 11.2 - 10.2,
    "wort_colour_EBC": 4.7 - 4.3,
}
DISCHARGE_FRACTION_MIN = 0.0
DISCHARGE_FRACTION_MAX = 1.0
FIXED_DISCHARGE_TARGET_KG = 9000.0
FIXED_DISCHARGE_TOL_KG = 1e-3
MIN_TOTAL_DISCHARGE_SHARE = 0.05
MAX_TOTAL_DISCHARGE_SHARE = 0.90
MIN_CANDIDATE_POOL_DISTANCE = 0.15
TARGET_OBJECTIVE_MODE: dict[str, str] = {
    # One-sided objective rules:
    # - "max": penalty only when actual > target
    # - "min": penalty only when actual < target
    # Any missing key defaults to "exact".
    "moisture_pct": "max",
    "fine_extract_db_pct": "min",
    "diastatic_power_WK": "min",
}
HIDDEN_BLEND_PARAM_KEYS: set[str] = {
    "soluble_n_mg_100g",
    "free_amino_n_mg_100g",
    "kolbach_index_pct",
    "beta_glucan_65c_mg_100g",
    "viscosity_mpas",
}


def _json_sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_sanitize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_sanitize(v) for v in value]
    if isinstance(value, tuple):
        return [_json_sanitize(v) for v in value]
    if isinstance(value, float):
        return value if isfinite(value) else None
    return value


def _visible_blended_params(params: dict[str, Any]) -> dict[str, float | None]:
    visible: dict[str, float | None] = {}
    for k, v in (params or {}).items():
        key = str(k)
        if key in HIDDEN_BLEND_PARAM_KEYS:
            continue
        try:
            numeric = float(v)
        except (TypeError, ValueError):
            continue
        visible[key] = numeric if isfinite(numeric) else None
    return visible


# ---------------------------------------------------------------------------
# Brewmaster ML — predict which of the top candidates the brewmaster selects
# ---------------------------------------------------------------------------

_BREWMASTER_FEATURE_COLUMNS = [
    "seed", "candidate_num", "objective_score", "total_discharged_mass_kg",
    "S1_discharge_fraction", "S1_discharge_mass_kg",
    "S2_discharge_fraction", "S2_discharge_mass_kg",
    "S3_discharge_fraction", "S3_discharge_mass_kg",
    "diastatic_power_WK", "fine_extract_db_pct", "moisture_pct",
    "total_protein_pct", "wort_colour_EBC", "wort_pH",
]


def _brewmaster_score_candidates(
    candidates: list[dict[str, Any]],
    seed: int,
) -> list[dict[str, Any]]:
    """
    Score the top candidates against the Azure ML brewmaster endpoint.

    Each candidate is annotated with:
      brewmaster_prob_selected  — P(this candidate is chosen) from the model
      brewmaster_prediction     — 1 if model predicts selected, 0 otherwise

    The candidate with the highest brewmaster_prob_selected is also flagged
    with  brewmaster_top_pick: True.

    Silently skips if BREWMASTER_ENDPOINT_URL / BREWMASTER_API_KEY are unset
    or if the endpoint call fails, so optimization is never blocked.
    """
    import os
    import requests as _requests

    url = os.getenv(
        "BREWMASTER_ENDPOINT_URL",
        "https://bq-brewmaster-endpoint.germanywestcentral.inference.ml.azure.com/score",
    ).strip()
    key = os.getenv("BREWMASTER_API_KEY", "").strip()
    if not url or not key:
        print(
            "[brewmaster] skipping endpoint scoring: "
            f"url_set={bool(url)} api_key_set={bool(key)}"
        )
        return candidates

    rows = []
    for i, cand in enumerate(candidates, start=1):
        bp   = cand.get("blended_params") or {}
        silo = {str(r["silo_id"]): r for r in (cand.get("recommended_discharge") or [])}
        rows.append([
            int(seed),
            i,
            float(cand.get("objective_score", 0.0)),
            float(cand.get("total_discharged_mass_kg", 0.0)),
            float(silo.get("S1", {}).get("discharge_fraction", 0.0)),
            float(silo.get("S1", {}).get("discharge_mass_kg",  0.0)),
            float(silo.get("S2", {}).get("discharge_fraction", 0.0)),
            float(silo.get("S2", {}).get("discharge_mass_kg",  0.0)),
            float(silo.get("S3", {}).get("discharge_fraction", 0.0)),
            float(silo.get("S3", {}).get("discharge_mass_kg",  0.0)),
            float(bp.get("diastatic_power_WK",  0.0)),
            float(bp.get("fine_extract_db_pct", 0.0)),
            float(bp.get("moisture_pct",        0.0)),
            float(bp.get("total_protein_pct",   0.0)),
            float(bp.get("wort_colour_EBC",     0.0)),
            float(bp.get("wort_pH",             0.0)),
        ])

    try:
        print(
            "[brewmaster] scoring candidates via endpoint: "
            f"url={url} candidate_count={len(rows)} seed={seed} tls_verify=False"
        )
        resp = _requests.post(
            url,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"input_data": {"columns": _BREWMASTER_FEATURE_COLUMNS, "data": rows}},
            verify=False,
            timeout=10,
        )
        resp.raise_for_status()
        body = resp.json()
        if isinstance(body, str):
            body = json.loads(body)

        predictions   = body.get("predictions",   [])
        probabilities = body.get("probabilities", [])

        best_idx  = -1
        best_prob = -1.0
        for i, cand in enumerate(candidates):
            prob = float(probabilities[i][1]) if i < len(probabilities) else None
            pred = int(predictions[i])        if i < len(predictions)   else None
            cand["candidate_num"]            = i + 1   # 1-based, stable before any sort
            cand["brewmaster_prob_selected"] = prob
            cand["brewmaster_prediction"]    = pred
            cand["brewmaster_top_pick"]      = False
            if prob is not None and prob > best_prob:
                best_prob = prob
                best_idx  = i

        if best_idx >= 0:
            candidates[best_idx]["brewmaster_top_pick"] = True
        print(
            "[brewmaster] endpoint scoring succeeded: "
            f"predictions={len(predictions)} probabilities={len(probabilities)} "
            f"top_pick_candidate_num={best_idx + 1 if best_idx >= 0 else 'none'}"
        )

    except Exception as exc:
        print(f"[brewmaster] endpoint call failed (non-fatal): {exc}")

    return candidates


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
        mode = str(TARGET_OBJECTIVE_MODE.get(str(key), "exact"))
        if mode == "max":
            diff = max(0.0, a - float(t))
        elif mode == "min":
            diff = max(0.0, float(t) - a)
        else:
            diff = a - float(t)
        score += (diff / scale) ** 2
    return score


# Built once at import time — shared by all hot-path scoring functions.
PARAM_KEYS: list[str] = list(DEFAULT_PARAM_RANGES.keys())

# Brew-master importance weights for the normalised L2 objective.
# Ordered to match PARAM_KEYS (insertion order of DEFAULT_PARAM_RANGES).
# Rationale:
#   diastatic_power_WK  0.30  — enzyme activity; uncorrectable in-process
#   fine_extract_db_pct 0.25  — yield & economics; direct alcohol/cost impact
#   wort_pH             0.20  — mash chemistry; partially correctable with salts
#   total_protein_pct   0.15  — haze/head retention; process-manageable
#   moisture_pct        0.07  — storage/yield; predictable, low brew-day impact
#   wort_colour_EBC     0.03  — spec parameter; least critical for base-malt blend
PARAM_WEIGHTS: dict[str, float] = {
    "moisture_pct": 0.07,
    "fine_extract_db_pct": 0.25,
    "wort_pH": 0.20,
    "diastatic_power_WK": 0.30,
    "total_protein_pct": 0.15,
    "wort_colour_EBC": 0.03,
}
# Pre-built weight vector aligned to PARAM_KEYS order.
_PARAM_WEIGHT_VEC: np.ndarray = np.array(
    [PARAM_WEIGHTS.get(k, 1.0 / len(PARAM_KEYS)) for k in PARAM_KEYS],
    dtype=np.float64,
)


def _score_blend_vectorised(
    actual: dict[str, Any],
    target: dict[str, Any],
    param_ranges: dict[str, float],
) -> float:
    """Normalised brew-master-weighted L2 error using numpy.

    ~10-20x faster than the Python loop. Weights in PARAM_WEIGHTS reflect
    brewing importance: diastatic_power (0.30) > fine_extract (0.25) >
    wort_pH (0.20) > total_protein (0.15) > moisture (0.07) > colour (0.03).
    """
    active_keys = [k for k in PARAM_KEYS if k in target and target.get(k) is not None]
    if not active_keys:
        return float("inf")
    a = np.array([actual.get(k, 0.0) for k in active_keys], dtype=np.float64)
    t = np.array([target.get(k, 0.0) for k in active_keys], dtype=np.float64)
    r = np.array([param_ranges.get(k, 1.0) for k in active_keys], dtype=np.float64)
    w = np.array([PARAM_WEIGHTS.get(k, 1.0 / len(active_keys)) for k in active_keys], dtype=np.float64)
    r = np.where(r == 0.0, 1.0, r)
    delta = a - t
    for i, key in enumerate(active_keys):
        mode = str(TARGET_OBJECTIVE_MODE.get(str(key), "exact"))
        if mode == "max":
            delta[i] = max(0.0, delta[i])
        elif mode == "min":
            delta[i] = max(0.0, -delta[i])
    return float(np.sqrt(np.sum(w * (delta / r) ** 2)))


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
    active_keys = [k for k in PARAM_KEYS if k in target and target.get(k) is not None]
    if not active_keys:
        return np.full(shape=(len(candidates),), fill_value=np.inf, dtype=np.float64)
    t = np.array([target.get(k, 0.0) for k in active_keys], dtype=np.float64)
    r = np.array([param_ranges.get(k, 1.0) for k in active_keys], dtype=np.float64)
    w = np.array([PARAM_WEIGHTS.get(k, 1.0 / len(active_keys)) for k in active_keys], dtype=np.float64)
    r = np.where(r == 0.0, 1.0, r)
    A = np.array(
        [[c["blended_params"].get(k, 0.0) for k in active_keys] for c in candidates],
        dtype=np.float64,
    )
    D = A - t
    for i, key in enumerate(active_keys):
        mode = str(TARGET_OBJECTIVE_MODE.get(str(key), "exact"))
        if mode == "max":
            D[:, i] = np.maximum(0.0, D[:, i])
        elif mode == "min":
            D[:, i] = np.maximum(0.0, -D[:, i])
    return np.sqrt(np.sum(w * (D / r) ** 2, axis=1))


def _diverse_top_k(
    candidates: list[dict[str, Any]],
    k: int = 5,
) -> list[dict[str, Any]]:
    """Diversity-aware top-k with threshold relaxation.

    Starts with strict thresholds on both discharge-fraction distance and
    blended-parameter distance, then relaxes gradually only if needed.
    """
    if len(candidates) <= k:
        return candidates

    pool = sorted(candidates, key=lambda x: x["objective_score"])[: max(k * 6, 30)]

    def _frac_vec(c: dict) -> np.ndarray:
        return np.array(
            [float(r["discharge_fraction"]) for r in c["recommended_discharge"]],
            dtype=np.float64,
        )

    def _param_distance(a: dict[str, Any], b: dict[str, Any]) -> float:
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

    selected_indices: list[int] = [0]
    selected: list[dict[str, Any]] = [pool[0]]
    pool_vecs = [_frac_vec(c) for c in pool]

    min_frac_dist = 0.22
    min_param_dist = 0.18
    relax = 0.75

    def _select_pass(frac_thresh: float, param_thresh: float) -> None:
        while len(selected) < k:
            best_idx = None
            best_score = -1.0
            for i, cand in enumerate(pool):
                if cand in selected:
                    continue
                frac_min = min(
                    np.linalg.norm(pool_vecs[i] - pool_vecs[j])
                    for j in selected_indices
                )
                param_min = min(_param_distance(cand, s) for s in selected)
                if frac_min < frac_thresh and param_min < param_thresh:
                    continue
                # Prefer candidates far in fraction space, then by param distance.
                rank_score = frac_min + 0.5 * param_min
                if rank_score > best_score:
                    best_score = rank_score
                    best_idx = i
            if best_idx is None:
                break
            selected.append(pool[best_idx])
            selected_indices.append(best_idx)

    _select_pass(min_frac_dist, min_param_dist)
    if len(selected) < k:
        _select_pass(min_frac_dist * relax, min_param_dist * relax)
    if len(selected) < k:
        _select_pass(min_frac_dist * relax * relax, min_param_dist * relax * relax)

    if len(selected) < k:
        for cand in pool:
            if len(selected) >= k:
                break
            if cand in selected:
                continue
            selected.append(cand)

    return selected[:k]


RANGE_BASED_MIN_DELTAS: dict[str, float] = {
    # Strong diversity thresholds derived from generator ranges:
    # moisture (2.0-5.0): 30% -> 0.90
    # fine_extract (79.0-87.0): 30% -> 2.40
    # wort_pH (5.5-6.0): 30% -> 0.15
    # diastatic (200-325): 30% -> 37.5
    # total_protein (9.5-11.5): 30% -> 0.60
    # colour (2.7-4.5): 30% -> 0.54
    "moisture_pct": 0.90,
    "fine_extract_db_pct": 2.40,
    "wort_pH": 0.15,
    "diastatic_power_WK": 37.50,
    "total_protein_pct": 0.60,
    "wort_colour_EBC": 0.54,
}


def _pick_diverse_candidates_by_param_deltas(
    candidates: list[dict[str, Any]],
    k: int = 4,
) -> list[dict[str, Any]]:
    """Pick top-k by diversity using normalized feature distance.

    Build a feature vector from blended COA params + discharge fractions.
    Start with the best objective score, then greedily add the candidate
    with the largest minimum distance to the already-selected set.
    """
    if len(candidates) <= k:
        return sorted(candidates, key=lambda x: float(x.get("objective_score", float("inf"))))

    pool = list(candidates)
    pool_sorted = sorted(pool, key=lambda x: float(x.get("objective_score", float("inf"))))

    def _feature_vec(c: dict[str, Any]) -> np.ndarray:
        params = c.get("blended_params", {}) or {}
        param_vals = [float(params.get(k, 0.0)) for k in PARAM_KEYS]
        discharge = c.get("recommended_discharge", []) or []
        frac_vals = [float(r.get("discharge_fraction", 0.0)) for r in discharge]
        return np.array(param_vals + frac_vals, dtype=np.float64)

    feats = np.stack([_feature_vec(c) for c in pool_sorted], axis=0)
    if feats.size == 0:
        return pool_sorted[:k]
    mean = feats.mean(axis=0)
    std = feats.std(axis=0)
    std = np.where(std <= 1e-9, 1.0, std)
    feats = (feats - mean) / std

    selected_idx: list[int] = [0]
    while len(selected_idx) < k and len(selected_idx) < len(pool_sorted):
        best_idx = None
        best_dist = -1.0
        for i in range(len(pool_sorted)):
            if i in selected_idx:
                continue
            dmin = min(
                float(np.linalg.norm(feats[i] - feats[j])) for j in selected_idx
            )
            if dmin > best_dist:
                best_dist = dmin
                best_idx = i
        if best_idx is None:
            break
        selected_idx.append(best_idx)

    return [pool_sorted[i] for i in selected_idx[:k]]


def _fraction_vector_for_candidate(
    candidate: dict[str, Any], silo_ids: list[str]
) -> list[float]:
    """Return fractions ordered by silo_ids, defaulting to 0.0 if missing."""
    discharge = candidate.get("recommended_discharge", []) or []
    by_silo = {
        str(r.get("silo_id", "")): float(r.get("discharge_fraction", 0.0) or 0.0)
        for r in discharge
    }
    return [by_silo.get(str(sid), 0.0) for sid in silo_ids]


def _fraction_distance(a: list[float], b: list[float]) -> float:
    return float(np.linalg.norm(np.asarray(a, dtype=np.float64) - np.asarray(b, dtype=np.float64)))


def _clip_fraction(v: float) -> float:
    return max(DISCHARGE_FRACTION_MIN, min(DISCHARGE_FRACTION_MAX, float(v)))


def _compute_feasibility_warnings(
    layers_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
    target_params: dict[str, float],
) -> list[dict[str, Any]]:
    """Compute per-parameter achievable ranges from current inventory and flag targets outside them.

    Each layer carries the COA of its supplier.  The achievable range for a
    blended parameter is [min(supplier_value), max(supplier_value)] across all
    layers that still have positive mass — a perfect blend can only be inside
    that convex hull.
    """
    coa_cols = [
        "moisture_pct",
        "fine_extract_db_pct",
        "wort_pH",
        "diastatic_power_WK",
        "total_protein_pct",
        "wort_colour_EBC",
    ]
    if layers_df.empty or suppliers_df.empty:
        return []

    # Keep only layers with positive remaining mass.
    mass_col = "segment_mass_kg" if "segment_mass_kg" in layers_df.columns else None
    if mass_col:
        active_layers = layers_df[layers_df[mass_col].astype(float) > 1e-6].copy()
    else:
        active_layers = layers_df.copy()

    if active_layers.empty:
        return []

    # Join layers → suppliers on the "supplier" column.
    if "supplier" not in active_layers.columns or "supplier" not in suppliers_df.columns:
        return []

    merged = active_layers.merge(
        suppliers_df[["supplier"] + [c for c in coa_cols if c in suppliers_df.columns]],
        on="supplier",
        how="left",
    )

    warnings: list[dict[str, Any]] = []
    for param, target_val in target_params.items():
        if param not in merged.columns:
            continue
        col_vals = merged[param].dropna().astype(float)
        if col_vals.empty:
            continue
        lo = float(col_vals.min())
        hi = float(col_vals.max())
        target_val_f = float(target_val)
        if target_val_f < lo - 1e-9 or target_val_f > hi + 1e-9:
            direction = "below" if target_val_f < lo else "above"
            warnings.append(
                {
                    "param": param,
                    "target": round(target_val_f, 4),
                    "achievable_min": round(lo, 4),
                    "achievable_max": round(hi, 4),
                    "direction": direction,
                    "message": (
                        f"{param}: target {target_val_f:.4g} is {direction} the achievable "
                        f"inventory range [{lo:.4g} – {hi:.4g}]"
                    ),
                }
            )

    return warnings


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


def _fractions_from_total_discharge(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return rows with discharge_fraction as share of total discharged mass."""
    total = float(
        sum(max(0.0, float(r.get("discharge_mass_kg", 0.0) or 0.0)) for r in (rows or []))
    )
    out: list[dict[str, Any]] = []
    for row in rows or []:
        mass = max(0.0, float(row.get("discharge_mass_kg", 0.0) or 0.0))
        rec = dict(row)
        rec["discharge_fraction"] = round((mass / total) if total > 1e-12 else 0.0, 6)
        out.append(rec)
    return out


def _total_discharge_share_within_bounds(
    rows: list[dict[str, Any]],
    *,
    min_share: float = MIN_TOTAL_DISCHARGE_SHARE,
    max_share: float = MAX_TOTAL_DISCHARGE_SHARE,
) -> bool:
    fractions = _fractions_from_total_discharge(rows)
    if not fractions:
        return False
    return all(
        min_share - 1e-9 <= float(row.get("discharge_fraction", 0.0) or 0.0) <= max_share + 1e-9
        for row in fractions
    )


def _candidate_with_total_fraction(candidate: dict[str, Any]) -> dict[str, Any]:
    out = dict(candidate)
    out["recommended_discharge"] = _fractions_from_total_discharge(
        candidate.get("recommended_discharge", []) or []
    )
    return out


def _total_fraction_vector_for_rows(rows: list[dict[str, Any]], silo_ids: list[str]) -> list[float]:
    by_silo = {
        str(r.get("silo_id", "")): float(r.get("discharge_fraction", 0.0) or 0.0)
        for r in _fractions_from_total_discharge(rows)
    }
    return [by_silo.get(str(sid), 0.0) for sid in silo_ids]


def _build_standard_equal_split_candidate(
    *,
    inputs: dict[str, pd.DataFrame],
    cfg: RunConfig,
    target_params: dict[str, float],
    available_by_silo: dict[str, float],
    target_total_kg: float,
) -> dict[str, Any]:
    """Build a non-optimized equal-split baseline candidate.

    If any silo has insufficient available mass for equal split, return an
    infeasible record with shortage details and do not run the simulator.
    """
    silo_ids = sorted(str(sid) for sid in available_by_silo.keys())
    if not silo_ids:
        return {
            "scenario_type": "standard_equal_split",
            "feasible": False,
            "reason": "no_silos_available",
            "insufficient_silos": [],
        }
    required_per_silo = float(target_total_kg) / float(len(silo_ids))
    insufficient: list[dict[str, Any]] = []
    for sid in silo_ids:
        avail = float(available_by_silo.get(sid, 0.0))
        if avail + 1e-12 < required_per_silo:
            insufficient.append(
                {
                    "silo_id": sid,
                    "required_mass_kg": round(required_per_silo, 2),
                    "available_mass_kg": round(avail, 2),
                    "shortage_mass_kg": round(required_per_silo - avail, 2),
                }
            )

    if insufficient:
        shortage_summary = "; ".join(
            f"{x['silo_id']}: required {x['required_mass_kg']:.2f} kg, available {x['available_mass_kg']:.2f} kg, shortage {x['shortage_mass_kg']:.2f} kg"
            for x in insufficient
        )
        return {
            "scenario_type": "standard_equal_split",
            "feasible": False,
            "reason": "insufficient_mass_for_equal_split",
            "reason_detail": shortage_summary,
            "required_per_silo_kg": round(required_per_silo, 2),
            "insufficient_silos": insufficient,
        }

    standard_rows = [
        {"silo_id": sid, "discharge_mass_kg": round(required_per_silo, 6)}
        for sid in silo_ids
    ]
    candidate_inputs = dict(inputs)
    candidate_inputs["discharge"] = pd.DataFrame(standard_rows)
    result = run_blend(candidate_inputs, cfg)
    discharged_total = float(result["total_discharged_mass_kg"])
    score = _score_blend_vectorised(
        actual=result["total_blended_params"],
        target=target_params,
        param_ranges=DEFAULT_PARAM_RANGES,
    )
    discharge_rows = [
        {
            "silo_id": sid,
            "discharge_mass_kg": round(required_per_silo, 6),
            "discharge_fraction": round(
                (required_per_silo / float(available_by_silo.get(sid, 0.0)))
                if float(available_by_silo.get(sid, 0.0)) > 1e-12
                else 0.0,
                6,
            ),
        }
        for sid in silo_ids
    ]
    return {
        "scenario_type": "standard_equal_split",
        "feasible": True,
        "required_per_silo_kg": round(required_per_silo, 2),
        "objective_score": float(score),
        "recommended_discharge": discharge_rows,
        "blended_params": _visible_blended_params(result["total_blended_params"]),
        "total_discharged_mass_kg": discharged_total,
    }


def create_app() -> FastAPI:
    app = FastAPI(title="DEM Simulation API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

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
            use_latest_state=True,
            include_all_candidates=bool(req.include_all_candidates),
        )
        return optimize(opt_req)

    @app.post("/api/process/apply_discharge")
    def process_apply_discharge(req: ProcessApplyDischargeRequest) -> dict[str, Any]:
        _ensure_state_initialized()
        state = get_state()
        available_now = sum(
            float(x.get("remaining_mass_kg", x.get("segment_mass_kg", 0.0)))
            for x in state.get("layers", [])
        )
        if available_now + 1e-12 < FIXED_DISCHARGE_TARGET_KG:
            fill_out = run_fill_only_simulation()
            state = fill_out.get("state", state)
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
        available_total = float(sum(available_by_silo.values()))
        if available_total + 1e-12 < FIXED_DISCHARGE_TARGET_KG:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Insufficient available mass after fill simulation for fixed discharge target "
                    f"{FIXED_DISCHARGE_TARGET_KG:.3f} kg. Currently available: {available_total:.3f} kg."
                ),
            )
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
            plan_run_id=req.plan_run_id,
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
                    plan_run_id,
                    sim_event_id,
                    discharge_by_silo,
                    predicted_run,
                    summary_before,
                    summary_after
                )
                VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
                """,
                (
                    req.plan_run_id,
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

    @app.get("/api/production-plans/active")
    def get_active_production_plan() -> dict[str, Any]:
        ensure_db_schema()
        rows = fetchall(
            """
            SELECT plan_run_id
            FROM production_plan_runs
            WHERE status <> 'completed'
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """
        )
        if not rows:
            return {"active": None}
        plan_run_id = str(rows[0].get("plan_run_id", ""))
        return {"active": _plan_run_response(plan_run_id)}

    @app.get("/api/production-plans/{plan_run_id}")
    def get_production_plan(plan_run_id: str) -> dict[str, Any]:
        ensure_db_schema()
        return _plan_run_response(plan_run_id)

    @app.get("/api/production-plans/{plan_run_id}/stream")
    def stream_production_plan(plan_run_id: str) -> StreamingResponse:
        ensure_db_schema()

        def event_iter():
            last_sig = ""
            while True:
                sig = _stream_signature(plan_run_id)
                if sig != last_sig:
                    last_sig = sig
                    if sig == "missing":
                        payload = {
                            "type": "waiting",
                            "plan_run_id": plan_run_id,
                            "message": "Waiting for production plan to start...",
                        }
                    else:
                        payload = {
                            "type": "snapshot",
                            "plan_run_id": plan_run_id,
                            "data": _plan_run_response(plan_run_id),
                        }
                    yield f"data: {json.dumps(jsonable_encoder(payload))}\n\n"
                else:
                    yield ": keepalive\n\n"
                time.sleep(1.0)

        return StreamingResponse(
            event_iter(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )

    @app.post("/api/production-plans/load")
    def load_production_plan(req: ProductionPlanLoadRequest) -> dict[str, Any]:
        ensure_db_schema()
        requested_plan_run_id = str(req.plan_run_id or "").strip()
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
        effective_schedule_id = req.schedule_id or f"sched_{req.seed}_{req.brews_count}_{int(time.time() * 1000)}"
        generated_schedule = generate_schedule(
            GenerateScheduleRequest(
                schedule_id=effective_schedule_id,
                name=req.name,
                brews_count=req.brews_count,
                seed=req.seed,
                target_params=req.target_params,
            )
        )
        schedule_id = str(generated_schedule.get("schedule_id", ""))
        plan_run_id = requested_plan_run_id or _new_plan_run_id(schedule_id)
        execute(
            """
            INSERT INTO production_plan_runs (
                plan_run_id, schedule_id, name, status, current_stage, current_message, progress_pct, current_brew_id, current_brew_index, meta
            )
            VALUES (%s, %s, %s, 'active', 'load_started', 'Starting production plan load...', 0.0, NULL, NULL, %s::jsonb)
            ON CONFLICT (plan_run_id)
            DO UPDATE SET
                schedule_id = EXCLUDED.schedule_id,
                name = EXCLUDED.name,
                status = 'active',
                current_stage = 'load_started',
                current_message = 'Starting production plan load...',
                progress_pct = 0.0,
                current_brew_id = NULL,
                current_brew_index = NULL,
                last_event_id = NULL,
                completed_at = NULL,
                updated_at = NOW(),
                meta = EXCLUDED.meta
            """,
            (
                plan_run_id,
                schedule_id,
                req.name,
                json.dumps(
                    {
                        "seed": req.seed,
                        "silos_count": req.silos_count,
                        "lots_count": req.lots_count,
                        "lot_size_kg": req.lot_size_kg,
                    }
                ),
            ),
        )
        summary = summarize_state()
        random_event_id = _write_sim_event(
            plan_run_id=plan_run_id,
            event_type="generate_random_data",
            action="generate_random_data",
            state_after=get_state(),
            incoming_queue_count=int(summary.get("incoming_queue", {}).get("count", 0)),
            incoming_queue_mass_kg=float(summary.get("incoming_queue", {}).get("total_mass_kg", 0.0)),
            meta={"seed": req.seed, "silos_count": req.silos_count, "lots_count": req.lots_count},
        )
        if random_event_id is not None:
            _update_plan_run(
                plan_run_id,
                current_stage="random_generated",
                current_message="Random inventory generated.",
                progress_pct=20.0,
                last_event_id=random_event_id,
            )
        _update_plan_run(
            plan_run_id,
            current_stage="schedule_generated",
            current_message=f"Schedule generated with {req.brews_count} brews.",
            progress_pct=35.0,
        )
        _update_plan_run(
            plan_run_id,
            current_stage="simulation_running",
            current_message="Running fill simulation for loaded production plan...",
            progress_pct=50.0,
        )
        run_out = process_run_simulation(
            ProcessRunSimulationRequest(
                silos=payload.get("silos", []),
                layers=payload.get("layers", []),
                suppliers=payload.get("suppliers", []),
                incoming_queue=payload.get("incoming_queue", []),
            )
        )
        run_state = run_out.get("state", {})
        run_summary = run_out.get("summary", {})
        run_event_id = _write_sim_event(
            plan_run_id=plan_run_id,
            event_type="run_simulation_fill_only",
            action="run_simulation_fill_only",
            state_after=run_state,
            total_discharged_mass_kg=0.0,
            total_remaining_mass_kg=None,
            incoming_queue_count=int(run_summary.get("incoming_queue", {}).get("count", 0)),
            incoming_queue_mass_kg=float(run_summary.get("incoming_queue", {}).get("total_mass_kg", 0.0)),
            meta={"source": "production_plan_load"},
        )
        if run_event_id is not None:
            _update_plan_run(
                plan_run_id,
                current_stage="simulation_completed",
                current_message="Fill simulation completed.",
                progress_pct=65.0,
                last_event_id=run_event_id,
            )
        first_brew = _pick_current_brew(_schedule_items(schedule_id))
        if not first_brew:
            raise HTTPException(status_code=422, detail="No brew items were created for the production plan.")
        _update_plan_run(
            plan_run_id,
            current_stage="optimizing",
            current_message=f"Optimizing {str(first_brew.get('brew_id', 'brew 1'))}...",
            progress_pct=80.0,
            current_brew_id=str(first_brew.get("brew_id", "")),
            current_brew_index=int(first_brew.get("brew_index", 0) or 0),
        )
        _, optimize_event_id = _optimize_brew_for_plan_run(
            optimize_fn=optimize,
            plan_run_id=plan_run_id,
            schedule_id=schedule_id,
            brew_id=str(first_brew.get("brew_id", "")),
            target_params=_json_obj(first_brew.get("target_params")),
            iterations=req.optimize_iterations,
            seed=req.optimize_seed,
            config=req.config,
            include_all_candidates=req.include_all_candidates,
        )
        _update_plan_run(
            plan_run_id,
            status="active",
            current_stage="awaiting_apply",
            current_message=f"{str(first_brew.get('brew_id', 'BREW001'))} ready for scenario selection.",
            progress_pct=100.0 * (1.0 / max(1, req.brews_count)) * 0.5,
            current_brew_id=str(first_brew.get("brew_id", "")),
            current_brew_index=int(first_brew.get("brew_index", 0) or 0),
            last_event_id=optimize_event_id,
        )
        return _plan_run_response(plan_run_id)

    @app.post("/api/production-plans/{plan_run_id}/brews/{brew_id}/optimize")
    def optimize_production_plan_brew(
        plan_run_id: str,
        brew_id: str,
        req: ProductionPlanOptimizeRequest,
    ) -> dict[str, Any]:
        ensure_db_schema()
        head = _plan_run_head(plan_run_id)
        _assert_expected_last_event(plan_run_id, req.expected_last_event_id)
        _activate_plan_run_state(plan_run_id)
        rows = fetchall(
            """
            SELECT brew_id, brew_index, target_params, status
            FROM brew_schedule_items
            WHERE schedule_id = %s AND brew_id = %s
            """,
            (str(head.get("schedule_id", "")), brew_id),
        )
        if not rows:
            raise HTTPException(status_code=404, detail="schedule item not found")
        brew_row = rows[0]
        target_params = _json_obj(brew_row.get("target_params"))
        progress_now = _workflow_progress(str(head.get("schedule_id", "")))
        _update_plan_run(
            plan_run_id,
            current_stage="optimizing",
            current_message=f"Optimizing {brew_id}...",
            progress_pct=progress_now,
            current_brew_id=brew_id,
            current_brew_index=int(brew_row.get("brew_index", 0) or 0),
        )
        _, optimize_event_id = _optimize_brew_for_plan_run(
            optimize_fn=optimize,
            plan_run_id=plan_run_id,
            schedule_id=str(head.get("schedule_id", "")),
            brew_id=brew_id,
            target_params=target_params,
            iterations=req.iterations,
            seed=req.seed,
            config=req.config,
            include_all_candidates=req.include_all_candidates,
        )
        _update_plan_run(
            plan_run_id,
            status="active",
            current_stage="awaiting_apply",
            current_message=f"{brew_id} ready for scenario selection.",
            progress_pct=progress_now,
            current_brew_id=brew_id,
            current_brew_index=int(brew_row.get("brew_index", 0) or 0),
            last_event_id=optimize_event_id,
        )
        return _plan_run_response(plan_run_id)

    @app.post("/api/production-plans/{plan_run_id}/brews/{brew_id}/apply")
    def apply_production_plan_brew(
        plan_run_id: str,
        brew_id: str,
        req: ProductionPlanApplyRequest,
    ) -> dict[str, Any]:
        ensure_db_schema()
        head = _plan_run_head(plan_run_id)
        _assert_expected_last_event(plan_run_id, req.expected_last_event_id)
        _activate_plan_run_state(plan_run_id)
        rows = fetchall(
            """
            SELECT brew_id, brew_index, optimize_result
            FROM brew_schedule_items
            WHERE schedule_id = %s AND brew_id = %s
            """,
            (str(head.get("schedule_id", "")), brew_id),
        )
        if not rows:
            raise HTTPException(status_code=404, detail="schedule item not found")
        opt_result = _json_obj(rows[0].get("optimize_result"))
        top_candidates = _json_list(opt_result.get("top_candidates"))
        idx = int(req.candidate_index)
        if idx < 0 or idx >= len(top_candidates):
            raise HTTPException(status_code=422, detail="invalid candidate_index for schedule item")
        selected = top_candidates[idx] if isinstance(top_candidates[idx], dict) else {}
        discharge_plan = _json_list(selected.get("recommended_discharge"))
        if not discharge_plan:
            raise HTTPException(status_code=422, detail="selected candidate has empty recommended_discharge")
        _update_plan_run(
            plan_run_id,
            current_stage="applying",
            current_message=f"Applying selected scenario for {brew_id}...",
            progress_pct=_workflow_progress(str(head.get("schedule_id", ""))),
            current_brew_id=brew_id,
            current_brew_index=int(rows[0].get("brew_index", 0) or 0),
        )
        out = process_apply_discharge(
            ProcessApplyDischargeRequest(discharge=discharge_plan, config=req.config, plan_run_id=plan_run_id)
        )
        apply_rows = fetchall(
            """
            SELECT id
            FROM sim_events
            WHERE plan_run_id = %s AND event_type = 'apply_discharge'
            ORDER BY id DESC
            LIMIT 1
            """,
            (plan_run_id,),
        )
        applied_event_id = int(apply_rows[0].get("id")) if apply_rows else None
        execute(
            """
            UPDATE brew_schedule_items
            SET status = 'applied',
                selected_candidate_index = %s,
                applied_event_id = %s,
                updated_at = NOW()
            WHERE schedule_id = %s AND brew_id = %s
            """,
            (idx, applied_event_id, str(head.get("schedule_id", "")), brew_id),
        )
        items = _schedule_items(str(head.get("schedule_id", "")))
        next_brew = _pick_current_brew(items)
        if next_brew is None:
            _update_plan_run(
                plan_run_id,
                status="completed",
                current_stage="completed",
                current_message=f"{brew_id} applied. Production plan completed.",
                progress_pct=100.0,
                current_brew_id=brew_id,
                current_brew_index=int(rows[0].get("brew_index", 0) or 0),
                last_event_id=applied_event_id,
                completed=True,
            )
            return _plan_run_response(plan_run_id)
        if str(next_brew.get("status", "")) != "optimized":
            _update_plan_run(
                plan_run_id,
                current_stage="optimizing",
                current_message=f"{brew_id} applied. Optimizing {str(next_brew.get('brew_id', 'next brew'))}...",
                progress_pct=_workflow_progress(str(head.get("schedule_id", ""))),
                current_brew_id=str(next_brew.get("brew_id", "")),
                current_brew_index=int(next_brew.get("brew_index", 0) or 0),
                last_event_id=applied_event_id,
            )
            _optimize_brew_for_plan_run(
                optimize_fn=optimize,
                plan_run_id=plan_run_id,
                schedule_id=str(head.get("schedule_id", "")),
                brew_id=str(next_brew.get("brew_id", "")),
                target_params=_json_obj(next_brew.get("target_params")),
                iterations=80,
                seed=42,
                config=req.config,
                include_all_candidates=False,
            )
        optimize_rows = fetchall(
            """
            SELECT id
            FROM sim_events
            WHERE plan_run_id = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (plan_run_id,),
        )
        latest_event_id = int(optimize_rows[0].get("id")) if optimize_rows else applied_event_id
        _update_plan_run(
            plan_run_id,
            status="active",
            current_stage="awaiting_apply",
            current_message=f"{str(next_brew.get('brew_id', 'Next brew'))} ready for scenario selection.",
            progress_pct=_workflow_progress(str(head.get("schedule_id", ""))),
            current_brew_id=str(next_brew.get("brew_id", "")),
            current_brew_index=int(next_brew.get("brew_index", 0) or 0),
            last_event_id=latest_event_id,
        )
        return _plan_run_response(plan_run_id)

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
            use_latest_state=True,
            include_all_candidates=bool(req.include_all_candidates),
        )
        out = optimize(opt_req)
        execute(
            """
            UPDATE brew_schedule_items
            SET status = 'optimized', optimize_result = %s::jsonb, updated_at = NOW()
            WHERE schedule_id = %s AND brew_id = %s
            """,
            (_json_dumps_safe(out), schedule_id, brew_id),
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
        inputs = _ensure_suppliers_dataframe(inputs)
        errors = validate_inputs_shape(inputs)
        coa_errors, coa_warnings = validate_supplier_coa(inputs["suppliers"])
        errors.extend(coa_errors)
        return {"valid": len(errors) == 0, "errors": errors, "coa_warnings": coa_warnings}

    @app.post("/api/run")
    def run(req: RunRequest) -> dict[str, Any]:
        inputs = {
            "silos": pd.DataFrame(req.silos),
            "layers": pd.DataFrame(req.layers),
            "suppliers": pd.DataFrame(req.suppliers),
            "discharge": pd.DataFrame(req.discharge),
        }
        inputs = _ensure_suppliers_dataframe(inputs)
        errors = validate_inputs_shape(inputs)
        coa_errors, coa_warnings = validate_supplier_coa(inputs["suppliers"])
        errors.extend(coa_errors)
        if errors:
            raise HTTPException(status_code=422, detail=errors)

        cfg = RunConfig(**req.config)
        result = run_blend(inputs, cfg)
        out = _result_to_api_payload(result)
        out["coa_warnings"] = coa_warnings
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
        validation_started_at = started_at
        inputs = {
            "silos": pd.DataFrame(req.silos),
            "layers": pd.DataFrame(req.layers),
            "suppliers": pd.DataFrame(req.suppliers),
            "discharge": pd.DataFrame(req.discharge),
        }
        live_queue: list[dict[str, Any]] = []
        if bool(req.use_latest_state):
            _ensure_state_initialized()
            live = get_state()
            inputs["silos"] = pd.DataFrame(live.get("silos", []))
            inputs["layers"] = pd.DataFrame(live.get("layers", []))
            inputs["suppliers"] = pd.DataFrame(live.get("suppliers", []))
            live_queue = list(live.get("incoming_queue", []) or [])
        inputs = _ensure_suppliers_dataframe(inputs)
        inputs = _ensure_discharge_has_silo_ids(inputs)
        errors = validate_inputs_shape(inputs)
        coa_errors, coa_warnings = validate_supplier_coa(inputs["suppliers"])
        errors.extend(coa_errors)
        if errors:
            raise HTTPException(status_code=422, detail=errors)
        if not req.target_params:
            raise HTTPException(status_code=422, detail="target_params must be provided.")

        cfg = RunConfig(**req.config)
        silos_df = inputs["silos"].copy()
        layers_df = inputs["layers"].copy()
        suppliers_df = inputs["suppliers"].copy()
        feasibility_warnings = _compute_feasibility_warnings(
            layers_df=layers_df,
            suppliers_df=suppliers_df,
            target_params=req.target_params,
        )
        available_by_silo = _available_mass_by_silo(layers_df)
        available_total = float(sum(available_by_silo.values()))
        auto_fill_triggered = False
        pre_fill_available_kg = available_total
        post_fill_available_kg = available_total
        if available_total + 1e-12 < FIXED_DISCHARGE_TARGET_KG:
            auto_fill_triggered = True
            before_fill_state = get_state()
            fill_out = run_fill_only_simulation()
            filled_state = fill_out.get("state", {})
            after_fill_summary = fill_out.get("summary", {})
            try:
                _sync_incoming_queue_to_db(filled_state.get("incoming_queue", []))
            except Exception as e:
                print(f"[optimize] incoming_queue sync after autofill failed: {e}")
            fill_event_id = _write_sim_event(
                event_type="optimize_prefill",
                action="run_simulation_fill_only",
                state_before=before_fill_state,
                state_after=filled_state,
                total_discharged_mass_kg=0.0,
                total_remaining_mass_kg=None,
                incoming_queue_count=int(after_fill_summary.get("incoming_queue", {}).get("count", 0)),
                incoming_queue_mass_kg=float(after_fill_summary.get("incoming_queue", {}).get("total_mass_kg", 0.0)),
                meta={"source": "api_optimize_autofill"},
            )
            try:
                _sync_layers_to_db(
                    filled_state,
                    event_type="optimize_prefill",
                    sim_event_id=fill_event_id,
                )
            except Exception as e:
                print(f"[optimize] layers sync after autofill failed: {e}")
            filled_silos = filled_state.get("silos", [])
            filled_layers = filled_state.get("layers", [])
            if isinstance(filled_silos, list) and filled_silos:
                silos_df = pd.DataFrame(filled_silos)
                inputs["silos"] = silos_df
            if isinstance(filled_layers, list):
                layers_df = pd.DataFrame(filled_layers)
                inputs["layers"] = layers_df
            filled_suppliers = filled_state.get("suppliers", [])
            if isinstance(filled_suppliers, list) and filled_suppliers:
                inputs["suppliers"] = pd.DataFrame(filled_suppliers)
                suppliers_df = inputs["suppliers"].copy()
            live_queue = list(filled_state.get("incoming_queue", []) or [])
            feasibility_warnings = _compute_feasibility_warnings(
                layers_df=layers_df,
                suppliers_df=suppliers_df,
                target_params=req.target_params,
            )
            available_by_silo = _available_mass_by_silo(layers_df)
            available_total = float(sum(available_by_silo.values()))
            post_fill_available_kg = available_total
            if available_total + 1e-12 < FIXED_DISCHARGE_TARGET_KG:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Insufficient available mass after fill simulation for fixed optimization target "
                        f"{FIXED_DISCHARGE_TARGET_KG:.3f} kg. Currently available: {available_total:.3f} kg."
                    ),
                )

        _write_lot_coas_csv(
            layers_df=layers_df,
            suppliers_df=suppliers_df,
            incoming_queue=live_queue,
            output_dir=Path("outputs"),
        )
        validation_ended_at = time.perf_counter()
        search_started_at = validation_ended_at
        silo_ids = silos_df["silo_id"].astype(str).tolist()
        total_iter = max(1, req.iterations)
        explore_iters = max(1, int(total_iter * 0.6))
        exploit_iters = total_iter - explore_iters
        best_score = float("inf")
        best_result: dict[str, Any] | None = None
        best_discharge: list[dict[str, Any]] = []
        top_candidates: list[dict[str, Any]] = []
        best_fractions: list[float] = []
        accepted_candidate_vectors: list[list[float]] = []

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
            if not _total_discharge_share_within_bounds(candidate_rows):
                return
            candidate_total_fraction_vec = _total_fraction_vector_for_rows(candidate_rows, silo_ids)
            if any(
                _fraction_distance(candidate_total_fraction_vec, existing_vec) < MIN_CANDIDATE_POOL_DISTANCE
                for existing_vec in accepted_candidate_vectors
            ):
                return
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
                "blended_params": _visible_blended_params(result["total_blended_params"]),
                "total_discharged_mass_kg": discharged_total,
            }
            top_candidates.append(candidate_record)
            accepted_candidate_vectors.append(candidate_total_fraction_vec)
            if score < best_score:
                best_score = score
                best_result = result
                best_discharge = candidate_rows
                best_fractions = [float(c["discharge_fraction"]) for c in candidate_rows]

        # Explore: stratified random sampling in discharge range to improve coverage.
        for i in range(explore_iters):
            rng = random.Random(req.seed + i)
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
            rng = random.Random(req.seed + explore_iters + i)
            anneal = 1.0 - (i / max(1, exploit_iters))
            step = 0.12 * anneal + 0.01
            trial = [_clip_fraction(f + rng.uniform(-step, step)) for f in best_fractions]
            evaluate_fractions(trial)
        search_ended_at = time.perf_counter()
        postprocess_started_at = search_ended_at

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
        all_evaluated_candidates = deepcopy(top_candidates)
        selection_rng = random.Random(req.seed)
        if len(top_candidates) > 4:
            top_candidates = selection_rng.sample(top_candidates, 4)
        for cand in top_candidates:
            cand["scenario_type"] = "optimized"
        standard_candidate = _build_standard_equal_split_candidate(
            inputs=inputs,
            cfg=cfg,
            target_params=req.target_params,
            available_by_silo=available_by_silo,
            target_total_kg=FIXED_DISCHARGE_TARGET_KG,
        )
        top_candidates = top_candidates + [standard_candidate]

        # Ask the brewmaster ML model which candidate it would select.
        top_candidates = _brewmaster_score_candidates(top_candidates, seed=req.seed)

        # Sort by ML probability descending so index-0 is always the predicted pick.
        scored = [c for c in top_candidates if c.get("brewmaster_prob_selected") is not None]
        unscored = [c for c in top_candidates if c.get("brewmaster_prob_selected") is None]
        top_candidates = (
            sorted(scored, key=lambda c: c["brewmaster_prob_selected"], reverse=True)
            + unscored
        )
        top_candidates = [_candidate_with_total_fraction(c) for c in top_candidates]
        best_discharge = _fractions_from_total_discharge(best_discharge)
        standard_candidate = _candidate_with_total_fraction(standard_candidate)
        all_evaluated_candidates = [_candidate_with_total_fraction(c) for c in all_evaluated_candidates]

        # Surface the winner explicitly for easy frontend access.
        brewmaster_recommendation = None
        if top_candidates and top_candidates[0].get("brewmaster_prob_selected") is not None:
            winner = top_candidates[0]
            brewmaster_recommendation = {
                "candidate_num":            winner.get("candidate_num"),
                "prob_selected":            winner["brewmaster_prob_selected"],
                "blended_params":           winner.get("blended_params"),
                "recommended_discharge":    winner.get("recommended_discharge"),
                "objective_score":          winner.get("objective_score"),
                "total_discharged_mass_kg": winner.get("total_discharged_mass_kg"),
            }

        out = {
            "objective_score": best_score,
            "recommended_discharge": best_discharge,
            "best_run": _result_to_api_payload(best_result),
            "target_params": req.target_params,
            "feasibility_warnings": feasibility_warnings,
            "coa_warnings": coa_warnings,
            "fixed_discharge_target_kg": FIXED_DISCHARGE_TARGET_KG,
            "iterations": req.iterations,
            "iterations_effective": total_iter,
            "explore_iterations": explore_iters,
            "exploit_iterations": exploit_iters,
            "objective_method": "normalized_weighted_l2_hybrid_search",
            "param_ranges": DEFAULT_PARAM_RANGES,
            "param_weights": PARAM_WEIGHTS,
            "target_objective_mode": TARGET_OBJECTIVE_MODE,
            "auto_fill_triggered": bool(auto_fill_triggered),
            "pre_fill_available_kg": float(pre_fill_available_kg),
            "post_fill_available_kg": float(post_fill_available_kg),
            "top_candidates": top_candidates,
            "brewmaster_recommendation": brewmaster_recommendation,
            "standard_scenario": standard_candidate,
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
        if bool(req.include_all_candidates):
            out["all_evaluated_candidates"] = all_evaluated_candidates
        out["elapsed_ms"] = round((time.perf_counter() - started_at) * 1000.0, 2)
        db_ms_total = 0.0
        db_started_at = time.perf_counter()
        sim_event_id = _write_sim_event(
            plan_run_id=req.plan_run_id,
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
                    plan_run_id,
                    sim_event_id,
                    objective_score,
                    recommended_discharge,
                    target_params,
                    top_candidates,
                    best_run
                )
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
                """,
                (
                    req.plan_run_id,
                    sim_event_id,
                    float(out["objective_score"]),
                    _json_dumps_safe(out["recommended_discharge"]),
                    _json_dumps_safe(out["target_params"]),
                    _json_dumps_safe(out["top_candidates"]),
                    _json_dumps_safe(out["best_run"]),
                ),
            )
        except Exception:
            pass
        db_ms_total += (time.perf_counter() - db_started_at) * 1000.0
        postprocess_ended_at = time.perf_counter()
        out["timing_breakdown_ms"] = {
            "validation_ms": round((validation_ended_at - validation_started_at) * 1000.0, 2),
            "search_ms": round((search_ended_at - search_started_at) * 1000.0, 2),
            "postprocess_ms": round((postprocess_ended_at - postprocess_started_at) * 1000.0, 2),
            "db_ms": round(db_ms_total, 2),
        }
        _persist_result("optimize", out, payload=req.model_dump())
        return out

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

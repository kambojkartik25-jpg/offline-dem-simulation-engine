from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from .charger import allocate_lots_append_to_existing

_LOCK = Lock()
_MAX_STAGES = 200

STATE: dict[str, Any] = {
    "silos": [],
    "layers": [],
    "suppliers": [],
    "incoming_queue": [],
    "stages": [],
    "history": [],
    "last_updated": "",
    "last_action": "reset",
    "cumulative_discharged_kg": 0.0,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_incoming_queue_locked() -> None:
    deduped_by_id: dict[str, dict[str, Any]] = {}
    deduped_without_id: list[dict[str, Any]] = []
    for lot in STATE["incoming_queue"]:
        lot_id = str(lot.get("lot_id", ""))
        supplier = str(lot.get("supplier", ""))
        mass_kg = float(lot.get("mass_kg", 0.0))
        if mass_kg <= 0:
            continue
        if lot_id:
            if lot_id in deduped_by_id:
                deduped_by_id[lot_id]["mass_kg"] += mass_kg
            else:
                deduped_by_id[lot_id] = {
                    "lot_id": lot_id,
                    "supplier": supplier,
                    "mass_kg": mass_kg,
                }
            continue
        deduped_without_id.append(
            {
                "lot_id": lot_id,
                "supplier": supplier,
                "mass_kg": mass_kg,
            }
        )
    deduped: list[dict[str, Any]] = list(deduped_by_id.values()) + deduped_without_id
    normalized: list[dict[str, Any]] = []
    for row in deduped:
        lot_id = str(row.get("lot_id", ""))
        supplier = str(row.get("supplier", ""))
        mass_kg = float(row.get("mass_kg", 0.0))
        if mass_kg <= 0:
            continue
        normalized.append(
            {"lot_id": lot_id, "supplier": supplier, "mass_kg": round(mass_kg, 6)}
        )
    STATE["incoming_queue"] = normalized


def _assert_state_invariants_locked() -> None:
    """Relaxed invariants:
    - no negative mass in layers/queue
    - each non-empty lot_id maps to a consistent supplier across records
    """
    supplier_by_lot: dict[str, str] = {}

    for layer in STATE["layers"]:
        mass = float(layer.get("remaining_mass_kg", layer.get("segment_mass_kg", 0.0)))
        if mass < -1e-9:
            raise ValueError("Layer mass cannot be negative.")
        lot_id = str(layer.get("lot_id", ""))
        supplier = str(layer.get("supplier", ""))
        if not lot_id:
            continue
        prev = supplier_by_lot.get(lot_id)
        if prev is None:
            supplier_by_lot[lot_id] = supplier
        elif prev != supplier:
            raise ValueError(f"Inconsistent supplier for lot_id={lot_id}.")

    for q in STATE["incoming_queue"]:
        mass = float(q.get("mass_kg", 0.0))
        if mass < -1e-9:
            raise ValueError("Incoming queue mass cannot be negative.")
        lot_id = str(q.get("lot_id", ""))
        supplier = str(q.get("supplier", ""))
        if not lot_id:
            continue
        prev = supplier_by_lot.get(lot_id)
        if prev is None:
            supplier_by_lot[lot_id] = supplier
        elif prev != supplier:
            raise ValueError(f"Inconsistent supplier for lot_id={lot_id}.")


def _touch(action: str, meta: dict[str, Any] | None = None) -> None:
    STATE["last_updated"] = _now_iso()
    STATE["last_action"] = action
    STATE["history"].append(
        {
            "timestamp": STATE["last_updated"],
            "action": action,
            "meta": dict(meta or {}),
        }
    )
    STATE["history"] = STATE["history"][-_MAX_STAGES:]


def get_state() -> dict[str, Any]:
    with _LOCK:
        return deepcopy(STATE)


def set_state(
    silos: list[dict[str, Any]] | None = None,
    layers: list[dict[str, Any]] | None = None,
    incoming_queue: list[dict[str, Any]] | None = None,
    suppliers: list[dict[str, Any]] | None = None,
    stages: list[dict[str, Any]] | None = None,
    action: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with _LOCK:
        if silos is not None:
            STATE["silos"] = deepcopy(silos)
        if layers is not None:
            STATE["layers"] = deepcopy(layers)
        if incoming_queue is not None:
            STATE["incoming_queue"] = deepcopy(incoming_queue)
        if suppliers is not None:
            STATE["suppliers"] = deepcopy(suppliers)
        if stages is not None:
            STATE["stages"] = deepcopy(stages)
        _normalize_incoming_queue_locked()
        _assert_state_invariants_locked()
        _touch(action or "set_state", meta)
        return deepcopy(STATE)


def reset_state() -> dict[str, Any]:
    with _LOCK:
        STATE["silos"] = []
        STATE["layers"] = []
        STATE["suppliers"] = []
        STATE["incoming_queue"] = []
        STATE["stages"] = []
        STATE["history"] = []
        STATE["cumulative_discharged_kg"] = 0.0
        _touch("reset", {})
        return deepcopy(STATE)


def summarize_state() -> dict[str, Any]:
    with _LOCK:
        silos = deepcopy(STATE["silos"])
        layers = deepcopy(STATE["layers"])
        by_silo: dict[str, dict[str, Any]] = {}
        for s in silos:
            sid = str(s["silo_id"])
            cap = float(s.get("capacity_kg", 0.0))
            by_silo[sid] = {
                "silo_id": sid,
                "capacity_kg": cap,
                "used_kg": 0.0,
                "remaining_kg": cap,
                "remaining_pct": 100.0 if cap > 0 else 0.0,
                "lots": [],
            }
        for row in sorted(
            layers,
            key=lambda r: (str(r.get("silo_id", "")), int(r.get("layer_index", 0))),
        ):
            sid = str(row.get("silo_id", ""))
            if sid not in by_silo:
                continue
            mass = float(row.get("remaining_mass_kg", row.get("segment_mass_kg", 0.0)))
            by_silo[sid]["used_kg"] += mass
            by_silo[sid]["lots"].append(
                {
                    "layer_index": int(row.get("layer_index", 0)),
                    "lot_id": str(row.get("lot_id", "")),
                    "supplier": str(row.get("supplier", "")),
                    "remaining_mass_kg": mass,
                }
            )
        for sid, rec in by_silo.items():
            _ = sid
            cap = float(rec["capacity_kg"])
            remaining = cap - float(rec["used_kg"])
            if remaining < 0 and abs(remaining) <= 1e-6:
                remaining = 0.0
            rec["remaining_kg"] = max(0.0, remaining)
            pct = (rec["remaining_kg"] / cap * 100.0) if cap > 0 else 0.0
            rec["remaining_pct"] = max(0.0, min(100.0, pct))
            # Current-position indexing: compact 1..N over active layers only.
            active_lots = [
                lot for lot in rec["lots"] if float(lot.get("remaining_mass_kg", 0.0)) > 1e-9
            ]
            active_lots.sort(key=lambda lot: int(lot.get("layer_index", 0)))
            for idx, lot in enumerate(active_lots, start=1):
                lot["current_layer_index"] = idx
            rec["lots"] = active_lots
        queue = deepcopy(STATE["incoming_queue"])
        return {
            "silos": list(by_silo.values()),
            "incoming_queue": {
                "count": len(queue),
                "total_mass_kg": float(sum(float(x.get("mass_kg", 0.0)) for x in queue)),
            },
            "cumulative_discharged_kg": float(STATE.get("cumulative_discharged_kg", 0.0)),
        }


def add_stage(action: str, before: dict[str, Any], after: dict[str, Any], meta: dict[str, Any]) -> None:
    with _LOCK:
        STATE["stages"].append(
            {
                "timestamp": _now_iso(),
                "action": action,
                "before": deepcopy(before),
                "after": deepcopy(after),
                "meta": deepcopy(meta),
            }
        )
        STATE["stages"] = STATE["stages"][-_MAX_STAGES:]
        _touch("stage", {"stage_action": action})


def run_fill_only_simulation() -> dict[str, Any]:
    before = summarize_state()
    with _LOCK:
        _normalize_incoming_queue_locked()
        charged_lots = 0
        loop_warnings: list[str] = []
        for _ in range(20):
            incoming = deepcopy(STATE["incoming_queue"])
            if not incoming:
                break
            remaining_total = 0.0
            for s in STATE["silos"]:
                sid = str(s.get("silo_id", ""))
                cap = float(s.get("capacity_kg", 0.0))
                used = sum(
                    float(l.get("remaining_mass_kg", l.get("segment_mass_kg", 0.0)))
                    for l in STATE["layers"]
                    if str(l.get("silo_id", "")) == sid
                )
                remaining_total += max(0.0, cap - used)
            if remaining_total <= 1e-9:
                break

            alloc = allocate_lots_append_to_existing(
                lots=incoming,
                silos=deepcopy(STATE["silos"]),
                existing_layers=deepcopy(STATE["layers"]),
                weights=None,
            )
            if alloc.get("warnings"):
                loop_warnings.extend(list(alloc["warnings"]))
            new_layers = deepcopy(alloc["new_layers"])
            if not new_layers:
                break
            charged_lots += len(new_layers)
            STATE["layers"].extend(new_layers)
            STATE["incoming_queue"] = deepcopy(alloc["incoming_queue"])
            _normalize_incoming_queue_locked()
            _assert_state_invariants_locked()
        _touch("run_simulation_fill_only", {"charged_lots": charged_lots})
    after = summarize_state()
    all_silos_full = all(float(s.get("remaining_kg", 0.0)) <= 1e-6 for s in after.get("silos", []))
    queue_mass = float(after.get("incoming_queue", {}).get("total_mass_kg", 0.0))
    if queue_mass > 1e-6 and not all_silos_full:
        loop_warnings.append(
            "incoming queue still has mass but silos are not fully utilized; check lot normalization constraints"
        )
    add_stage(
        action="run_simulation_fill_only",
        before=before,
        after=after,
        meta={
            "charged_lots": charged_lots,
            "leftover_queue_count": after["incoming_queue"]["count"],
            "all_silos_full": all_silos_full,
            "warnings": loop_warnings,
        },
    )
    return {"state": get_state(), "summary": after}


def apply_discharge_to_state(discharge_by_silo: dict[str, float]) -> dict[str, Any]:
    with _LOCK:
        by_silo: dict[str, list[dict[str, Any]]] = {}
        for row in STATE["layers"]:
            sid = str(row.get("silo_id", ""))
            by_silo.setdefault(sid, []).append(deepcopy(row))
        new_layers: list[dict[str, Any]] = []
        total_removed = 0.0
        for s in STATE["silos"]:
            sid = str(s["silo_id"])
            target = max(0.0, float(discharge_by_silo.get(sid, 0.0)))
            remaining = target
            layers = sorted(by_silo.get(sid, []), key=lambda r: int(r.get("layer_index", 0)))
            kept: list[dict[str, Any]] = []
            for layer in layers:
                mass = float(layer.get("remaining_mass_kg", layer.get("segment_mass_kg", 0.0)))
                if remaining <= 1e-12:
                    kept.append(layer)
                    continue
                take = min(mass, remaining)
                mass_after = mass - take
                remaining -= take
                total_removed += take
                layer["remaining_mass_kg"] = round(max(0.0, mass_after), 6)
                layer["segment_mass_kg"] = round(max(0.0, mass_after), 6)
                kept.append(layer)
            for idx, layer in enumerate(kept, start=1):
                layer["layer_index"] = idx
                new_layers.append(layer)
        STATE["layers"] = new_layers
        _assert_state_invariants_locked()
        STATE["cumulative_discharged_kg"] = float(STATE.get("cumulative_discharged_kg", 0.0)) + total_removed
        _touch("apply_discharge", {"total_removed_kg": total_removed})
        return deepcopy(STATE)

from __future__ import annotations

from typing import Any


def _remaining_capacity_by_silo(
    silos: list[dict[str, Any]], layers: list[dict[str, Any]]
) -> dict[str, float]:
    capacities = {str(s["silo_id"]): float(s.get("capacity_kg", 0.0)) for s in silos}
    used = {sid: 0.0 for sid in capacities}
    for layer in layers:
        sid = str(layer.get("silo_id", ""))
        if sid in used:
            # Prefer post-discharge mass when present; fallback to segment mass.
            used[sid] += float(
                layer.get("remaining_mass_kg", layer.get("segment_mass_kg", 0.0))
            )
    out: dict[str, float] = {}
    for sid in capacities:
        remaining = capacities[sid] - used[sid]
        if remaining < 0 and abs(remaining) <= 1e-6:
            remaining = 0.0
        out[sid] = max(0.0, remaining)
    return out


def _next_layer_index(existing_layers: list[dict[str, Any]], silo_id: str) -> int:
    max_idx = 0
    for layer in existing_layers:
        if str(layer.get("silo_id", "")) == silo_id:
            max_idx = max(max_idx, int(layer.get("layer_index", 0)))
    return max_idx + 1


def allocate_lots_to_silos(
    lots: list[dict[str, Any]],
    silos: list[dict[str, Any]],
    weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Sequential charging in silo order with split-on-capacity fallback.

    Tries S1->S2->S3 and places as much as possible in each silo. If a lot
    cannot fully fit in one silo, the remainder is carried to the next silo.
    """
    _ = weights
    layers: list[dict[str, Any]] = []
    incoming_queue: list[dict[str, Any]] = []
    warnings: list[str] = []

    for lot in lots:
        lot_mass = max(0.0, float(lot.get("mass_kg", 0.0)))
        lot_id = str(lot.get("lot_id", ""))
        supplier = str(lot.get("supplier", ""))
        if lot_mass <= 0:
            continue
        remaining_lot = lot_mass
        remaining_capacity = _remaining_capacity_by_silo(silos, layers)
        for silo in silos:
            sid = str(silo["silo_id"])
            room = max(0.0, float(remaining_capacity.get(sid, 0.0)))
            if room <= 1e-12 or remaining_lot <= 1e-12:
                continue
            alloc = min(remaining_lot, room)
            layer_idx = _next_layer_index(layers, sid)
            layers.append(
                {
                    "silo_id": sid,
                    "layer_index": layer_idx,
                    "lot_id": lot_id,
                    "supplier": supplier,
                    "segment_mass_kg": round(alloc, 6),
                }
            )
            remaining_lot -= alloc
            remaining_capacity[sid] = room - alloc
        if remaining_lot > 1e-12:
            incoming_queue.append(
                {"lot_id": lot_id, "supplier": supplier, "mass_kg": round(remaining_lot, 6)}
            )
    if incoming_queue:
        warnings.append(
            f"capacity full, queued {len(incoming_queue)} lot(s)"
        )

    return {"layers": layers, "incoming_queue": incoming_queue, "warnings": warnings}


def allocate_lots_append_to_existing(
    lots: list[dict[str, Any]],
    silos: list[dict[str, Any]],
    existing_layers: list[dict[str, Any]] | None = None,
    weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Append lots on top of existing layers with sequential split fallback.

    Tries S1->S2->S3 and places as much as possible in each silo. Remaining
    mass, if any, is queued.
    """
    _ = weights
    existing_layers = [dict(l) for l in (existing_layers or [])]
    new_layers: list[dict[str, Any]] = []
    incoming_queue: list[dict[str, Any]] = []
    warnings: list[str] = []

    for lot in lots:
        lot_mass = max(0.0, float(lot.get("mass_kg", 0.0)))
        lot_id = str(lot.get("lot_id", ""))
        supplier = str(lot.get("supplier", ""))
        if lot_mass <= 0:
            continue
        remaining_lot = lot_mass
        all_layers = existing_layers + new_layers
        remaining_capacity = _remaining_capacity_by_silo(silos, all_layers)
        for silo in silos:
            sid = str(silo["silo_id"])
            room = max(0.0, float(remaining_capacity.get(sid, 0.0)))
            if room <= 1e-12 or remaining_lot <= 1e-12:
                continue
            alloc = min(remaining_lot, room)
            all_layers_now = existing_layers + new_layers
            layer_idx = _next_layer_index(all_layers_now, sid)
            new_layers.append(
                {
                    "silo_id": sid,
                    "layer_index": layer_idx,
                    "lot_id": lot_id,
                    "supplier": supplier,
                    "segment_mass_kg": round(alloc, 6),
                }
            )
            remaining_lot -= alloc
            remaining_capacity[sid] = room - alloc
        if remaining_lot > 1e-12:
            incoming_queue.append(
                {"lot_id": lot_id, "supplier": supplier, "mass_kg": round(remaining_lot, 6)}
            )
    if incoming_queue:
        warnings.append(
            f"capacity full, queued {len(incoming_queue)} lot(s)"
        )

    return {
        "new_layers": new_layers,
        "incoming_queue": incoming_queue,
        "warnings": warnings,
    }

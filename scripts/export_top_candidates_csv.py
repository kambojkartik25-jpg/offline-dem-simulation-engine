#!/usr/bin/env python3

import argparse
import csv
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


class ApiError(RuntimeError):
    pass


DEFAULT_TARGET_PARAMS = {
    "moisture_pct": 4.5,
    "fine_extract_db_pct": 81.0,
    "wort_pH": 5.8,
    "diastatic_power_WK": 250.0,
    "total_protein_pct": 10.8,
    "wort_colour_EBC": 3.5,
}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the optimization endpoint repeatedly and export the top candidates "
            "from each run to a CSV file."
        )
    )
    parser.add_argument(
        "--base-url",
        help="Optional backend base URL. If omitted, the script runs the local FastAPI app in-process.",
    )
    parser.add_argument("--runs", type=int, default=200, help="Number of optimization runs")
    parser.add_argument("--top-k", type=int, default=4, help="Number of optimized candidates to export per run")
    parser.add_argument("--iterations", type=int, default=80, help="Iterations per optimization run")
    parser.add_argument("--seed", type=int, default=42, help="Seed for the first optimization run")
    parser.add_argument(
        "--seed-step",
        type=int,
        default=1,
        help="Increment added to the seed after each run",
    )
    parser.add_argument(
        "--payload-json",
        help=(
            "Optional path to a JSON file containing a full /api/optimize request payload. "
            "If omitted, the script uses /api/sample."
        ),
    )
    parser.add_argument(
        "--output",
        default="outputs/top_candidates_batch.csv",
        help="CSV output path",
    )
    parser.add_argument("--request-timeout", type=float, default=300.0, help="HTTP timeout in seconds")
    args = parser.parse_args(argv)

    if args.runs <= 0:
        parser.error("--runs must be > 0")
    if args.top_k <= 0:
        parser.error("--top-k must be > 0")
    if args.iterations <= 0:
        parser.error("--iterations must be > 0")
    if args.request_timeout <= 0:
        parser.error("--request-timeout must be > 0")
    return args


def request_json_http(
    base_url: str,
    method: str,
    path: str,
    timeout_s: float,
    payload: dict[str, Any] | None = None,
) -> Any:
    url = f"{base_url}{path}"
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url=url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ApiError(f"{method} {url} failed with HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise ApiError(f"{method} {url} failed: {exc}") from exc

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ApiError(f"{method} {url} returned non-JSON response: {raw[:500]}") from exc


def request_json_local(client: Any, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    response = client.request(method=method, url=path, json=payload)
    if response.status_code >= 400:
        raise ApiError(f"{method} {path} failed with HTTP {response.status_code}: {response.text}")
    return response.json()


def create_requester(args: argparse.Namespace) -> tuple[str, Any]:
    if args.base_url:
        return ("http", args.base_url.rstrip("/"))
    try:
        from fastapi.testclient import TestClient
    except Exception as exc:
        raise ApiError(
            "Local in-process mode requires the optional 'httpx' package. "
            "Either install it or run the web server and pass --base-url."
        ) from exc
    from dem_sim.web import create_app

    return ("local", TestClient(create_app()))


def request_json(
    requester_kind: str,
    requester: Any,
    method: str,
    path: str,
    timeout_s: float,
    payload: dict[str, Any] | None = None,
) -> Any:
    if requester_kind == "http":
        return request_json_http(str(requester), method, path, timeout_s, payload)
    return request_json_local(requester, method, path, payload)


def load_base_payload(args: argparse.Namespace, requester_kind: str, requester: Any) -> dict[str, Any]:
    if args.payload_json:
        payload_path = Path(args.payload_json)
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    else:
        sample = request_json(requester_kind, requester, "GET", "/api/sample", args.request_timeout)
        if not isinstance(sample, dict):
            raise ApiError("/api/sample returned a non-object payload")
        payload = sample
    if not isinstance(payload.get("target_params"), dict) or not payload.get("target_params"):
        payload["target_params"] = dict(DEFAULT_TARGET_PARAMS)
    return payload


def optimized_candidates(response: dict[str, Any], top_k: int) -> list[dict[str, Any]]:
    top_candidates = response.get("top_candidates") or []
    if not isinstance(top_candidates, list):
        return []
    selected: list[dict[str, Any]] = []
    for candidate in top_candidates:
        if not isinstance(candidate, dict):
            continue
        if candidate.get("scenario_type") and candidate.get("scenario_type") != "optimized":
            continue
        selected.append(candidate)
        if len(selected) >= top_k:
            break
    return selected


def flatten_candidate(
    run_number: int,
    run_seed: int,
    candidate_number: int,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "simulation_n": run_number,
        "seed": run_seed,
        "candidate_num": candidate_number,
        "objective_score": candidate.get("objective_score"),
    }

    blended_params = candidate.get("blended_params") or {}
    if isinstance(blended_params, dict):
        for key, value in blended_params.items():
            row[str(key)] = value

    discharge_rows = candidate.get("recommended_discharge") or []
    if isinstance(discharge_rows, list):
        for discharge_row in discharge_rows:
            if not isinstance(discharge_row, dict):
                continue
            silo_id = str(discharge_row.get("silo_id", "")).strip()
            if not silo_id:
                continue
            row[f"{silo_id}_discharge_mass_kg"] = discharge_row.get("discharge_mass_kg")
            row[f"{silo_id}_discharge_fraction"] = discharge_row.get("discharge_fraction")
    return row


def build_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    fixed = [
        "simulation_n",
        "seed",
        "candidate_num",
        "objective_score",
    ]
    remaining = sorted({key for row in rows for key in row.keys() if key not in fixed})
    return fixed + remaining


def write_csv(output_path: Path, rows: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = build_fieldnames(rows)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    requester_kind, requester = create_requester(args)
    base_payload = load_base_payload(args, requester_kind, requester)
    rows: list[dict[str, Any]] = []

    for run_index in range(args.runs):
        run_number = run_index + 1
        run_seed = args.seed + (run_index * args.seed_step)
        payload = json.loads(json.dumps(base_payload))
        payload["iterations"] = args.iterations
        payload["seed"] = run_seed

        response = request_json(requester_kind, requester, "POST", "/api/optimize", args.request_timeout, payload)
        if not isinstance(response, dict):
            raise ApiError(f"/api/optimize run {run_number} returned a non-object payload")

        candidates = optimized_candidates(response, args.top_k)
        for candidate_index, candidate in enumerate(candidates, start=1):
            rows.append(
                flatten_candidate(
                    run_number=run_number,
                    run_seed=run_seed,
                    candidate_number=candidate_index,
                    candidate=candidate,
                )
            )

        print(
            f"run {run_number}/{args.runs}: seed={run_seed} exported_candidates={len(candidates)}",
            flush=True,
        )

    output_path = Path(args.output)
    write_csv(output_path, rows)
    print(f"Wrote {len(rows)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        raise SystemExit(130)
    except ApiError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)

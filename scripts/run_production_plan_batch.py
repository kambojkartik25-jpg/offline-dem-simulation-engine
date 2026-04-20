#!/usr/bin/env python3

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional


DEFAULT_TARGET_PARAMS = {
    "moisture_pct": 4.5,
    "fine_extract_db_pct": 81.0,
    "wort_pH": 5.8,
    "diastatic_power_WK": 250,
    "total_protein_pct": 10.8,
    "wort_colour_EBC": 3.5,
}


class ApiError(RuntimeError):
    pass


class RunnerConfig:
    def __init__(
        self,
        base_url,
        schedules,
        brews_per_schedule,
        poll_interval_s,
        request_timeout_s,
        optimize_iterations,
        optimize_seed,
        random_seed,
        silos_count,
        lots_count,
        lot_size_kg,
        steps,
        include_all_candidates,
        name_prefix,
    ):
        self.base_url = base_url
        self.schedules = schedules
        self.brews_per_schedule = brews_per_schedule
        self.poll_interval_s = poll_interval_s
        self.request_timeout_s = request_timeout_s
        self.optimize_iterations = optimize_iterations
        self.optimize_seed = optimize_seed
        self.random_seed = random_seed
        self.silos_count = silos_count
        self.lots_count = lots_count
        self.lot_size_kg = lot_size_kg
        self.steps = steps
        self.include_all_candidates = include_all_candidates
        self.name_prefix = name_prefix


class ProductionPlanBatchRunner:
    def __init__(self, config: RunnerConfig) -> None:
        self.config = config
        self.random = random.Random(config.random_seed)

    def run(self) -> int:
        print(
            f"Starting batch: schedules={self.config.schedules}, "
            f"brews_per_schedule={self.config.brews_per_schedule}, base_url={self.config.base_url}"
        )
        for schedule_num in range(1, self.config.schedules + 1):
            snapshot = self._start_schedule(schedule_num)
            self._drive_schedule(schedule_num, snapshot)
        print("Batch completed successfully.")
        return 0

    def _start_schedule(self, schedule_num):
        payload = {
            "plan_run_id": f"batch_run_{int(time.time() * 1000)}_{schedule_num:03d}",
            "seed": self.config.random_seed + schedule_num - 1,
            "silos_count": self.config.silos_count,
            "lots_count": self.config.lots_count,
            "lot_size_kg": self.config.lot_size_kg,
            "schedule_id": None,
            "name": f"{self.config.name_prefix} {schedule_num}",
            "brews_count": self.config.brews_per_schedule,
            "target_params": DEFAULT_TARGET_PARAMS,
            "optimize_iterations": self.config.optimize_iterations,
            "optimize_seed": self.config.optimize_seed,
            "config": {"steps": self.config.steps},
            "include_all_candidates": self.config.include_all_candidates,
        }
        print(f"[schedule {schedule_num}/{self.config.schedules}] loading production plan")
        snapshot = self._request("POST", "/api/production-plans/load", payload)
        self._log_snapshot(schedule_num, snapshot, prefix="loaded")
        return snapshot

    def _drive_schedule(self, schedule_num, snapshot):
        applied_count = 0
        while True:
            status = self._plan_status(snapshot)
            if status == "completed":
                plan_run_id = self._plan_run_id(snapshot)
                print(
                    f"[schedule {schedule_num}/{self.config.schedules}] "
                    f"completed plan_run_id={plan_run_id} applied_brews={applied_count}"
                )
                return

            current_brew = snapshot.get("current_brew") or {}
            scenarios = snapshot.get("scenarios") or []

            if not current_brew:
                snapshot = self._poll_snapshot(schedule_num, snapshot, "waiting for current brew")
                continue

            if not scenarios:
                snapshot = self._optimize_current_brew(schedule_num, snapshot)
                continue

            candidate_index = self.random.randrange(len(scenarios))
            brew_id = str(current_brew.get("brew_id", ""))
            brew_index = current_brew.get("brew_index")
            print(
                f"[schedule {schedule_num}/{self.config.schedules}] "
                f"applying brew={brew_id} index={brew_index} candidate_index={candidate_index} "
                f"scenario_count={len(scenarios)}"
            )
            snapshot = self._apply_current_brew(schedule_num, snapshot, candidate_index)
            applied_count += 1

    def _optimize_current_brew(self, schedule_num, snapshot):
        current_brew = snapshot.get("current_brew") or {}
        brew_id = str(current_brew.get("brew_id", ""))
        plan_run_id = self._plan_run_id(snapshot)
        payload = {
            "iterations": self.config.optimize_iterations,
            "seed": self.config.optimize_seed,
            "config": {"steps": self.config.steps},
            "include_all_candidates": self.config.include_all_candidates,
            "expected_last_event_id": self._last_event_id(snapshot),
        }
        print(f"[schedule {schedule_num}/{self.config.schedules}] optimizing brew={brew_id}")
        snapshot = self._request(
            "POST",
            f"/api/production-plans/{urllib.parse.quote(plan_run_id)}/brews/{urllib.parse.quote(brew_id)}/optimize",
            payload,
        )
        if snapshot.get("scenarios"):
            self._log_snapshot(schedule_num, snapshot, prefix="optimized")
            return snapshot
        return self._poll_snapshot(schedule_num, snapshot, f"waiting for scenarios on {brew_id}")

    def _apply_current_brew(self, schedule_num, snapshot, candidate_index):
        current_brew = snapshot.get("current_brew") or {}
        brew_id = str(current_brew.get("brew_id", ""))
        plan_run_id = self._plan_run_id(snapshot)
        payload = {
            "candidate_index": candidate_index,
            "reason": "random",
            "config": {"steps": self.config.steps},
            "expected_last_event_id": self._last_event_id(snapshot),
        }
        snapshot = self._request(
            "POST",
            f"/api/production-plans/{urllib.parse.quote(plan_run_id)}/brews/{urllib.parse.quote(brew_id)}/apply",
            payload,
        )
        self._log_snapshot(schedule_num, snapshot, prefix="applied")
        return snapshot

    def _poll_snapshot(self, schedule_num, snapshot, reason):
        plan_run_id = self._plan_run_id(snapshot)
        print(f"[schedule {schedule_num}/{self.config.schedules}] {reason}; polling plan_run_id={plan_run_id}")
        while True:
            time.sleep(self.config.poll_interval_s)
            snapshot = self._request("GET", f"/api/production-plans/{urllib.parse.quote(plan_run_id)}")
            status = self._plan_status(snapshot)
            if status == "completed":
                self._log_snapshot(schedule_num, snapshot, prefix="polled")
                return snapshot
            if snapshot.get("current_brew") and snapshot.get("scenarios"):
                self._log_snapshot(schedule_num, snapshot, prefix="polled")
                return snapshot
            if "waiting for current brew" in reason and snapshot.get("current_brew"):
                self._log_snapshot(schedule_num, snapshot, prefix="polled")
                return snapshot

    def _request(self, method, path, payload=None):
        url = f"{self.config.base_url}{path}"
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url=url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.config.request_timeout_s) as response:
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

    @staticmethod
    def _plan_status(snapshot):
        return str((snapshot.get("plan_run") or {}).get("status", ""))

    @staticmethod
    def _plan_run_id(snapshot):
        plan_run_id = str((snapshot.get("plan_run") or {}).get("plan_run_id", ""))
        if not plan_run_id:
            raise ApiError("Missing plan_run.plan_run_id in snapshot")
        return plan_run_id

    @staticmethod
    def _last_event_id(snapshot):
        value = (snapshot.get("plan_run") or {}).get("last_event_id")
        return int(value) if value is not None else None

    def _log_snapshot(self, schedule_num, snapshot, prefix):
        plan_run = snapshot.get("plan_run") or {}
        current_brew = snapshot.get("current_brew") or {}
        print(
            f"[schedule {schedule_num}/{self.config.schedules}] {prefix}: "
            f"plan_run_id={plan_run.get('plan_run_id')} "
            f"status={plan_run.get('status')} "
            f"stage={plan_run.get('current_stage')} "
            f"brew={current_brew.get('brew_id')} "
            f"brew_index={current_brew.get('brew_index')} "
            f"scenarios={len(snapshot.get('scenarios') or [])} "
            f"last_event_id={plan_run.get('last_event_id')}"
        )


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Run production-plan load/apply flow repeatedly against the backend API."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Backend base URL")
    parser.add_argument("--schedules", type=int, default=15, help="Number of schedules to run")
    parser.add_argument("--brews-per-schedule", type=int, default=7, help="Brews per schedule")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Polling interval in seconds")
    parser.add_argument("--request-timeout", type=float, default=300.0, help="HTTP request timeout in seconds")
    parser.add_argument("--optimize-iterations", type=int, default=80, help="Optimization iterations")
    parser.add_argument("--optimize-seed", type=int, default=42, help="Optimization seed")
    parser.add_argument("--random-seed", type=int, default=42, help="Scenario selection seed")
    parser.add_argument("--silos-count", type=int, default=3, help="Random data silo count")
    parser.add_argument("--lots-count", type=int, default=100, help="Random data lots count")
    parser.add_argument("--lot-size-kg", type=float, default=25000.0, help="Random data lot size")
    parser.add_argument("--steps", type=int, default=800, help="Config.steps value for optimize/apply")
    parser.add_argument(
        "--include-all-candidates",
        action="store_true",
        help="Request all optimization candidates instead of the default subset",
    )
    parser.add_argument("--name-prefix", default="Batch Brew Schedule", help="Schedule name prefix")
    args = parser.parse_args(argv)

    if args.schedules <= 0:
        parser.error("--schedules must be > 0")
    if args.brews_per_schedule <= 0:
        parser.error("--brews-per-schedule must be > 0")
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be > 0")
    if args.request_timeout <= 0:
        parser.error("--request-timeout must be > 0")

    return RunnerConfig(
        base_url=args.base_url.rstrip("/"),
        schedules=args.schedules,
        brews_per_schedule=args.brews_per_schedule,
        poll_interval_s=args.poll_interval,
        request_timeout_s=args.request_timeout,
        optimize_iterations=args.optimize_iterations,
        optimize_seed=args.optimize_seed,
        random_seed=args.random_seed,
        silos_count=args.silos_count,
        lots_count=args.lots_count,
        lot_size_kg=args.lot_size_kg,
        steps=args.steps,
        include_all_candidates=args.include_all_candidates,
        name_prefix=args.name_prefix,
    )


def main(argv=None):
    config = parse_args(argv or sys.argv[1:])
    runner = ProductionPlanBatchRunner(config)
    try:
        return runner.run()
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        return 130
    except ApiError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

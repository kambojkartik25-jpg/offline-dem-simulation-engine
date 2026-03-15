from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from .db_models import Base, SimHistory, SimResult, SimSnapshot, SimStage


class StorageBackend(Protocol):
    def ensure_schema(self) -> None: ...

    def write_snapshot(
        self,
        event_type: str,
        action: str,
        state: dict[str, Any],
        summary: dict[str, Any],
        payload: dict[str, Any] | None = None,
    ) -> None: ...

    def write_stages(self, stages: list[dict[str, Any]]) -> None: ...

    def write_history(self, history: list[dict[str, Any]]) -> None: ...

    def write_result(
        self,
        event_type: str,
        result: dict[str, Any],
        payload: dict[str, Any] | None = None,
    ) -> None: ...


class NullStorage:
    def ensure_schema(self) -> None:
        return

    def write_snapshot(
        self,
        event_type: str,
        action: str,
        state: dict[str, Any],
        summary: dict[str, Any],
        payload: dict[str, Any] | None = None,
    ) -> None:
        return

    def write_stages(self, stages: list[dict[str, Any]]) -> None:
        return

    def write_history(self, history: list[dict[str, Any]]) -> None:
        return

    def write_result(
        self,
        event_type: str,
        result: dict[str, Any],
        payload: dict[str, Any] | None = None,
    ) -> None:
        return


@dataclass
class PostgresStorage:
    dsn: str

    def __post_init__(self) -> None:
        # SQLAlchemy needs the psycopg3 dialect specifier; normalise bare postgresql:// URLs.
        dsn = self.dsn
        if dsn.startswith("postgresql://") or dsn.startswith("postgres://"):
            dsn = dsn.replace("postgresql://", "postgresql+psycopg://", 1).replace(
                "postgres://", "postgresql+psycopg://", 1
            )
        self._engine = create_engine(dsn, future=True)
        self._session_factory = sessionmaker(self._engine, future=True, expire_on_commit=False)

    def ensure_schema(self) -> None:
        Base.metadata.create_all(self._engine)

    def _session(self) -> Session:
        return self._session_factory()

    def write_snapshot(
        self,
        event_type: str,
        action: str,
        state: dict[str, Any],
        summary: dict[str, Any],
        payload: dict[str, Any] | None = None,
    ) -> None:
        row = SimSnapshot(
            event_type=event_type,
            action=action,
            state_json=state,
            summary_json=summary,
            payload_json=payload or {},
        )
        with self._session() as s:
            s.add(row)
            s.commit()

    def write_stages(self, stages: list[dict[str, Any]]) -> None:
        if not stages:
            return
        with self._session() as s:
            for st in stages:
                ts = str(st.get("timestamp", ""))
                action = str(st.get("action", ""))
                stage_key = f"{ts}|{action}"
                stmt = pg_insert(SimStage).values(
                    stage_key=stage_key,
                    stage_timestamp=ts,
                    action=action,
                    before_json=st.get("before", {}),
                    after_json=st.get("after", {}),
                    meta_json=st.get("meta", {}),
                )
                stmt = stmt.on_conflict_do_nothing(index_elements=["stage_key"])
                s.execute(stmt)
            s.commit()

    def write_history(self, history: list[dict[str, Any]]) -> None:
        if not history:
            return
        with self._session() as s:
            for h in history:
                ts = str(h.get("timestamp", ""))
                action = str(h.get("action", ""))
                history_key = f"{ts}|{action}"
                stmt = pg_insert(SimHistory).values(
                    history_key=history_key,
                    event_timestamp=ts,
                    action=action,
                    meta_json=h.get("meta", {}),
                )
                stmt = stmt.on_conflict_do_nothing(index_elements=["history_key"])
                s.execute(stmt)
            s.commit()

    def write_result(
        self,
        event_type: str,
        result: dict[str, Any],
        payload: dict[str, Any] | None = None,
    ) -> None:
        row = SimResult(
            event_type=event_type,
            result_json=result,
            payload_json=payload or {},
        )
        with self._session() as s:
            s.add(row)
            s.commit()


def get_storage() -> StorageBackend:
    dsn = os.getenv("DEM_SIM_DATABASE_URL", "").strip()
    if not dsn:
        return NullStorage()
    try:
        return PostgresStorage(dsn=dsn)
    except Exception:
        return NullStorage()

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row


def _database_url() -> str:
    dsn = os.getenv("DEM_SIM_DATABASE_URL", "").strip()
    if not dsn:
        raise RuntimeError("DEM_SIM_DATABASE_URL is required")
    return dsn


@contextmanager
def get_conn() -> Any:
    conn = psycopg.connect(_database_url(), row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def execute(sql: str, params: tuple[Any, ...] | None = None) -> None:
    with get_conn() as conn:
        with conn.transaction():
            conn.execute(sql, params or ())


def fetchall(sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.execute(sql, params or ())
        rows = cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            if hasattr(row, "items"):
                out.append(dict(row.items()))
            else:
                out.append(dict(zip([c.name for c in cur.description], row)))
        return out

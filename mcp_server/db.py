"""Read-only Postgres access for the MCP server.

Every query runs in its own short READ ONLY transaction, with a statement
timeout and search_path pinned to the analytics schema, through a server-side
cursor so only the rows we return are pulled over the wire. The transaction is
always rolled back.
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterator, Sequence

import psycopg2

SCHEMA = "analytics"


@dataclass(frozen=True)
class DbSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    statement_timeout_ms: int

    @classmethod
    def from_env(cls) -> "DbSettings":
        env = os.environ
        return cls(
            host=env.get("MCP_PG_HOST", "localhost"),
            port=int(env.get("MCP_PG_PORT", "5433")),
            dbname=env.get("MCP_PG_DB", env.get("POSTGRES_DB", "crypto")),
            user=env.get("MCP_PG_USER", "mcp_reader"),
            password=env.get("MCP_PG_PASSWORD", "mcp_reader_pass"),
            statement_timeout_ms=int(env.get("MCP_STATEMENT_TIMEOUT_MS", "5000")),
        )


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    truncated: bool


def _to_json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return value


class ReadOnlyDatabase:
    def __init__(self, settings: DbSettings | None = None):
        self.settings = settings or DbSettings.from_env()

    @contextmanager
    def _transaction(self) -> Iterator[Any]:
        s = self.settings
        conn = psycopg2.connect(
            host=s.host,
            port=s.port,
            dbname=s.dbname,
            user=s.user,
            password=s.password,
            connect_timeout=5,
            application_name="crypto-market-mcp",
        )
        try:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = %s", (s.statement_timeout_ms,))
                cur.execute(f"SET LOCAL search_path = {SCHEMA}")
            yield conn
        finally:
            conn.rollback()
            conn.close()

    def query(self, sql: str, params: Sequence[Any] | dict | None = None, max_rows: int = 500) -> QueryResult:
        """Run one SELECT and return at most max_rows rows (truncated=True if there were more)."""
        with self._transaction() as conn:
            # A named (server-side) cursor is declared as `DECLARE ... CURSOR FOR <sql>`,
            # which Postgres only accepts for SELECT/VALUES: one more read-only guard.
            with conn.cursor(name=f"mcp_{uuid.uuid4().hex[:12]}") as cur:
                cur.execute(sql, params)
                fetched = cur.fetchmany(max_rows + 1)
                columns = [d[0] for d in cur.description] if cur.description else []
        truncated = len(fetched) > max_rows
        rows = [{c: _to_json(v) for c, v in zip(columns, r)} for r in fetched[:max_rows]]
        return QueryResult(columns=columns, rows=rows, truncated=truncated)

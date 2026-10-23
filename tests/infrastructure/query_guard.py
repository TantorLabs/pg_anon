from __future__ import annotations

from asyncpg import Connection

# Bind parameters must not grow with the number of objects: PostgreSQL allows at most 32767.
MAX_QUERY_ARGS = 16


class QueryArgsGuard:
    """Record every query that has more bind parameters than MAX_QUERY_ARGS."""

    def __init__(self, limit: int = MAX_QUERY_ARGS) -> None:
        self.limit = limit
        self.violations: list[str] = []
        # asyncpg has no public hook for bind parameters, so its private methods are patched
        self.original = Connection._get_statement  # noqa: SLF001

    def install(self, monkeypatch) -> None:
        guard = self

        async def guarded_get_statement(conn, query, *args: object, **kwargs: object):
            statement = await guard.original(conn, query, *args, **kwargs)
            guard.check(query, len(statement._get_parameters()))  # noqa: SLF001
            return statement

        monkeypatch.setattr(Connection, "_get_statement", guarded_get_statement)

    def check(self, query: str, args_count: int) -> None:
        if args_count <= self.limit:
            return
        message = f"query has {args_count} bind parameters (limit {self.limit}): {query[:300]}"
        self.violations.append(message)
        raise AssertionError(message)

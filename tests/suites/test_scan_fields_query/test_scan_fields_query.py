from __future__ import annotations

from pg_anon.common.db_queries import get_scan_fields_query


def test_list_query_is_deterministically_ordered():
    query = get_scan_fields_query()
    assert "ORDER BY 1, 2, a.attnum" in query


def test_limit_clause_added_when_limit_given():
    assert "LIMIT 20" in get_scan_fields_query(limit=20)


def test_no_limit_clause_without_limit():
    assert "LIMIT" not in get_scan_fields_query()

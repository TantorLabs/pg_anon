from __future__ import annotations

from pg_anon.modes.dump import _applicable_hardening_dump_flags, _DumpFlagPos


def _flags(major: int) -> list[str]:
    return [flag for flag, _pos, _reason in _applicable_hardening_dump_flags(major)]


def test_no_hardening_flags_before_pg10():
    assert _flags(9) == []


def test_no_subscriptions_from_pg10():
    flags = _flags(15)
    assert "--no-subscriptions" in flags
    assert "--no-statistics" not in flags


def test_no_statistics_from_pg18():
    flags = _flags(18)
    assert "--no-subscriptions" in flags
    assert "--no-statistics" in flags


def test_no_statistics_is_last_positioned():
    positions = {flag: pos for flag, pos, _reason in _applicable_hardening_dump_flags(18)}
    assert positions["--no-subscriptions"] is _DumpFlagPos.NORMAL
    assert positions["--no-statistics"] is _DumpFlagPos.LAST

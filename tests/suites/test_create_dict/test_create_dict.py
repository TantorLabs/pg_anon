from __future__ import annotations

import json
from pathlib import Path

from .conftest import input_dict, output_dict
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.constants import SAVED_DICTS_INFO_FILE_NAME
from pg_anon.common.enums import ResultCode


def _options(
    db_params,
    db_name: str,
    *,
    meta_dicts: list[str],
    sens_out: str,
    no_sens_out: str | None = None,
    prepared_sens: str | None = None,
    prepared_no_sens: str | None = None,
    save_dicts: bool = False,
):
    args = [
        "create-dict",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={db_name}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        "--scan-mode=full",
        f"--meta-dict-file={','.join(meta_dicts)}",
        f"--output-sens-dict-file={sens_out}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--scan-partial-rows=5000",
        "--debug",
    ]
    if no_sens_out is not None:
        args.append(f"--output-no-sens-dict-file={no_sens_out}")
    if prepared_sens is not None:
        args.append(f"--prepared-sens-dict-file={prepared_sens}")
    if prepared_no_sens is not None:
        args.append(f"--prepared-no-sens-dict-file={prepared_no_sens}")
    if save_dicts:
        args.append("--save-dicts")
    return build_run_options(args)


def _load_json(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _has_rule(sens: dict, schema: str, table: str, field: str) -> bool:
    for rule in sens["dictionary"]:
        if rule.get("schema") == schema and rule.get("table") == table and field in rule.get("fields", {}):
            return True
    return False


def _tables_with_rules(sens: dict) -> set[tuple[str, str]]:
    return {(r["schema"], r["table"]) for r in sens["dictionary"] if "schema" in r and "table" in r}


async def test_create_dict_detects_expected_pii_fields(source_db, db_params):
    sens_out = output_dict("detects_pii.json")
    no_sens_out = output_dict("detects_pii_no_sens.json")

    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_basic.py")],
            sens_out=sens_out,
            no_sens_out=no_sens_out,
        )
    ).run()

    assert res.result_code == ResultCode.DONE
    assert Path(sens_out).exists()
    assert Path(no_sens_out).exists()

    sens = _load_json(sens_out)
    expected_hits = [
        ("hr", "employee", "email"),
        ("hr", "employee", "phone"),
        ("hr", "employee", "ssn"),
        ("billing", "customer", "email"),
        ("billing", "customer", "tax_id"),
        ("billing", "payment_card", "cardholder_name"),
        ("billing", "payment_card", "pan_last4"),
        ("audit", "log_entry", "client_ip"),
        ("audit", "login_attempt", "client_ip"),
    ]
    missing = [t for t in expected_hits if not _has_rule(sens, *t)]
    assert not missing, f"scanner missed known sensitive columns: {missing}"


async def test_create_dict_respects_skip_rules(source_db, db_params):
    sens_out = output_dict("skip_rules.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_basic.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    sens = _load_json(sens_out)
    skipped_schemas = {"analytics_archive", "data_types", "security"}
    violators = [t for t in _tables_with_rules(sens) if t[0] in skipped_schemas]
    assert not violators, f"skipped schemas surfaced: {violators}"

    assert not _has_rule(sens, "hr", "department", "budget")


async def test_create_dict_detects_by_data_regex(source_db, db_params):
    sens_out = output_dict("data_regex.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_basic.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    sens = _load_json(sens_out)
    assert _has_rule(sens, "ecommerce", "product", "sku"), (
        "data_regex failed to catch decoy SKU column shaped like a card number"
    )


async def test_create_dict_default_func_covers_all_sens_types(source_db, db_params):
    sens_out = output_dict("default_func.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_default_func.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    sens = _load_json(sens_out)
    assert sens["dictionary"], "expected at least one sensitive rule"
    for rule in sens["dictionary"]:
        for func in rule.get("fields", {}).values():
            assert "default" in func, f"rule {rule} did not use default func"


async def test_create_dict_rescan_with_prepared_no_sens_skips_known_safe(source_db, db_params):
    sens_first = output_dict("rescan_first.json")
    no_sens_first = output_dict("rescan_no_sens_first.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_basic.py")],
            sens_out=sens_first,
            no_sens_out=no_sens_first,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    sens_second = output_dict("rescan_second.json")
    no_sens_second = output_dict("rescan_no_sens_second.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_basic.py")],
            sens_out=sens_second,
            no_sens_out=no_sens_second,
            prepared_no_sens=no_sens_first,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    first = _load_json(sens_first)
    second = _load_json(sens_second)
    assert {(r["schema"], r["table"]) for r in first["dictionary"]} == {
        (r["schema"], r["table"]) for r in second["dictionary"]
    }


async def test_create_dict_save_dicts_writes_run_directory(source_db, db_params):
    sens_out = output_dict("save_dicts.json")
    no_sens_out = output_dict("save_dicts_no_sens.json")
    meta = input_dict("meta_basic.py")

    options = _options(
        db_params,
        source_db,
        meta_dicts=[meta],
        sens_out=sens_out,
        no_sens_out=no_sens_out,
        save_dicts=True,
    )
    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE

    run_dir = Path(options.run_dir)
    assert (run_dir / SAVED_DICTS_INFO_FILE_NAME).exists()
    assert (run_dir / "input" / Path(meta).name).exists()
    assert (run_dir / "output" / Path(sens_out).name).exists()
    assert (run_dir / "output" / Path(no_sens_out).name).exists()


async def test_create_dict_fails_on_non_existent_scan_func(source_db, db_params):
    sens_out = output_dict("invalid_func.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_invalid_func.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.FAIL


async def test_create_dict_rescan_with_both_prepared_dicts_is_stable(source_db, db_params):
    sens_first = output_dict("rescan_both_first_sens.json")
    no_sens_first = output_dict("rescan_both_first_no_sens.json")

    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_basic.py")],
            sens_out=sens_first,
            no_sens_out=no_sens_first,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    sens_second = output_dict("rescan_both_second_sens.json")
    no_sens_second = output_dict("rescan_both_second_no_sens.json")

    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_basic.py")],
            sens_out=sens_second,
            no_sens_out=no_sens_second,
            prepared_sens=sens_first,
            prepared_no_sens=no_sens_first,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    first = _load_json(sens_first)
    second = _load_json(sens_second)

    first_tables = {(r["schema"], r["table"]) for r in first["dictionary"]}
    second_tables = {(r["schema"], r["table"]) for r in second["dictionary"]}
    assert first_tables == second_tables, (
        f"re-scan with both prepared dicts produced different table set: "
        f"only-in-first={first_tables - second_tables}, "
        f"only-in-second={second_tables - first_tables}"
    )

    def _flatten(d):
        return {(r["schema"], r["table"], f) for r in d["dictionary"] for f in r.get("fields", {})}

    diff = _flatten(first) ^ _flatten(second)
    assert not diff, f"re-scan reclassified fields: {sorted(diff)}"

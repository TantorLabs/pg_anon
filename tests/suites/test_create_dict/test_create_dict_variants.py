from __future__ import annotations

import json
from pathlib import Path

from .conftest import input_dict, output_dict
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode


def _options(db_params, db_name, *, meta_dicts, sens_out, no_sens_out=None):
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
    return build_run_options(args)


def _load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _rules_index(sens):
    out = {}
    for rule in sens.get("dictionary", []):
        key = (rule.get("schema"), rule.get("table"))
        out.setdefault(key, {}).update(rule.get("fields", {}))
    return out


async def test_variant_include_rules_isolates_explicit_field(source_db, db_params):
    sens_out = output_dict("variant_include_rules.json")
    res = await PgAnonApp(_options(
        db_params, source_db,
        meta_dicts=[input_dict("meta_include_rules.py")],
        sens_out=sens_out,
    )).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))

    assert "email" in idx.get(("hr", "employee"), {}), (
        f"include-pinned hr.employee.email missing; got rules={idx}"
    )

    forbidden = [
        ("billing", "customer"),
        ("audit", "log_entry"),
        ("audit", "login_attempt"),
        ("content", "comment"),
    ]
    leaked = [t for t in forbidden if t in idx]
    assert not leaked, (
        f"include_rules should restrict to (hr, employee), but these leaked through: {leaked}"
    )


async def test_variant_include_and_skip_rules_with_masks(source_db, db_params):
    sens_out = output_dict("variant_include_skip.json")
    res = await PgAnonApp(_options(
        db_params, source_db,
        meta_dicts=[input_dict("meta_include_skip_with_masks.py")],
        sens_out=sens_out,
    )).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))

    assert "email" in idx.get(("hr", "employee"), {}), \
        f"hr.employee.email expected; got rules={idx}"
    assert "email" in idx.get(("billing", "customer"), {}), \
        f"billing.customer.email expected; got rules={idx}"

    assert "phone" in idx.get(("hr", "employee"), {}), \
        f"hr.employee.phone expected; got rules={idx}"

    audit_tables = [k for k in idx if k[0] == "audit"]
    assert not audit_tables, f"audit.* must be skipped, got: {audit_tables}"


async def test_variant_partial_constants_matches_substrings_in_data(source_db, db_params):
    sens_out = output_dict("variant_partial_constants.json")
    res = await PgAnonApp(_options(
        db_params, source_db,
        meta_dicts=[input_dict("meta_partial_constants.py")],
        sens_out=sens_out,
    )).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))
    assert "email" in idx.get(("hr", "employee"), {}), (
        f"partial_constants should hit hr.employee.email; "
        f"got {list(idx.get(('hr','employee'), {}))}"
    )


async def test_variant_words_and_phrases_constants(source_db, db_params):
    sens_out = output_dict("variant_words_and_phrases.json")
    res = await PgAnonApp(_options(
        db_params, source_db,
        meta_dicts=[input_dict("meta_words_and_phrases.py")],
        sens_out=sens_out,
    )).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))
    assert "name" in idx.get(("hr", "department"), {}), (
        f"data_const constants should hit hr.department.name; "
        f"got {list(idx.get(('hr','department'), {}))}"
    )


async def test_variant_data_sql_condition_does_not_break_scan(source_db, db_params):
    sens_out = output_dict("variant_data_sql_condition.json")
    res = await PgAnonApp(_options(
        db_params, source_db,
        meta_dicts=[input_dict("meta_data_sql_condition.py")],
        sens_out=sens_out,
    )).run()
    assert res.result_code == ResultCode.DONE, (
        "create-dict must accept sql_condition without error"
    )

    idx = _rules_index(_load(sens_out))
    assert "email" in idx.get(("hr", "employee"), {}), (
        "regex over data sampled under sql_condition should still detect email"
    )


async def test_variant_data_func_with_custom_scan_func(source_db, db_params):
    sens_out = output_dict("variant_data_func.json")
    res = await PgAnonApp(_options(
        db_params, source_db,
        meta_dicts=[input_dict("meta_data_func.py")],
        sens_out=sens_out,
    )).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))
    assert idx, "data_func scan must classify at least one column"

    assert "email" in idx.get(("hr", "employee"), {}), (
        f"data_func should classify hr.employee.email; got {idx}"
    )

    flat = json.dumps(_load(sens_out))
    assert "partial_email" in flat, (
        "data_func anon_func template must appear on at least one classified column"
    )


async def test_variant_data_func_per_field_with_custom_scan_func(source_db, db_params):
    sens_out = output_dict("variant_data_func_per_field.json")
    res = await PgAnonApp(_options(
        db_params, source_db,
        meta_dicts=[input_dict("meta_data_func_per_field.py")],
        sens_out=sens_out,
    )).run()
    assert res.result_code == ResultCode.DONE, (
        "create-dict must accept scan_func_per_field without error"
    )

    idx = _rules_index(_load(sens_out))
    flat = json.dumps(_load(sens_out))

    must_have = [
        ("hr", "employee", "email"),
        ("hr", "employee", "phone"),
        ("hr", "employee", "ssn"),
        ("billing", "customer", "tax_id"),
        ("billing", "payment_card", "cardholder_name"),
        ("billing", "payment_card", "pan_last4"),
        ("audit", "login_attempt", "username"),
    ]
    missing = [(s, t, f) for (s, t, f) in must_have if f not in idx.get((s, t), {})]
    assert not missing, (
        f"scan_func_per_field should classify these PII columns by name: {missing}"
    )

    assert "pii_marker" in flat, (
        "scan_func_per_field anon_func template must appear on at least one classified column"
    )


def _markers_for(rules_for_table: dict[str, str]) -> set[str]:
    markers = set()
    for func in rules_for_table.values():
        for token in (
            "int_alias", "int4_alias", "int8_alias", "bool_alias",
            "varchar_alias", "text_alias",
            "timestamp_alias", "timestamptz_alias",
            "varchar20_complex", "double_complex",
            "timestamp3_complex", "timetz3_complex",
            "default",
        ):
            if token in func:
                markers.add(token)
    return markers


async def test_variant_type_aliases_picks_per_alias_func(source_db, db_params):
    sens_out = output_dict("variant_type_aliases.json")
    res = await PgAnonApp(_options(
        db_params, source_db,
        meta_dicts=[input_dict("meta_type_aliases.py")],
        sens_out=sens_out,
    )).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))
    sample_rules = idx.get(("data_types", "sample"), {})
    assert sample_rules, "include_rules must surface data_types.sample fields"

    used = _markers_for(sample_rules)
    expected_present = {
        "int4_alias",
        "bool_alias",
        "text_alias",
        "timestamptz_alias",
    }
    missing = expected_present - used
    assert not missing, (
        f"type-alias resolution missed: {missing}; observed markers: {used}"
    )


async def test_variant_type_aliases_complex_handles_whitespace_and_precision(source_db, db_params):
    sens_out = output_dict("variant_type_aliases_complex.json")
    res = await PgAnonApp(_options(
        db_params, source_db,
        meta_dicts=[input_dict("meta_type_aliases_complex.py")],
        sens_out=sens_out,
    )).run()
    assert res.result_code == ResultCode.DONE, (
        "create-dict must normalize complex type aliases without raising"
    )

    idx = _rules_index(_load(sens_out))
    sample_rules = idx.get(("data_types", "sample"), {})
    assert sample_rules, "include_rules must surface data_types.sample fields"

    used = _markers_for(sample_rules)
    complex_markers = {"varchar20_complex", "double_complex", "timestamp3_complex", "timetz3_complex"}
    assert used & complex_markers, (
        f"no complex-alias func picked up — normalization likely broken; "
        f"observed markers: {used}"
    )

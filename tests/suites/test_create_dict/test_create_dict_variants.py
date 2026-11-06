from __future__ import annotations

import json
from pathlib import Path

from .conftest import input_dict, output_dict
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode


def _options(
    db_params,
    db_name,
    *,
    meta_dicts,
    sens_out,
    no_sens_out=None,
    scan_mode="full",
    db_connections=None,
    prepared_sens=None,
):
    args = [
        "create-dict",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={db_name}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--scan-mode={scan_mode}",
        f"--meta-dict-file={','.join(meta_dicts)}",
        f"--output-sens-dict-file={sens_out}",
        f"--db-connections-per-process={db_connections or db_params.db_connections_per_process}",
        "--scan-partial-rows=5000",
        "--debug",
    ]
    if no_sens_out is not None:
        args.append(f"--output-no-sens-dict-file={no_sens_out}")
    if prepared_sens is not None:
        args.append(f"--prepared-sens-dict-file={prepared_sens}")
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
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_include_rules.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))

    assert "email" in idx.get(("hr", "employee"), {}), f"include-pinned hr.employee.email missing; got rules={idx}"

    forbidden = [
        ("billing", "customer"),
        ("audit", "log_entry"),
        ("audit", "login_attempt"),
        ("content", "comment"),
    ]
    leaked = [t for t in forbidden if t in idx]
    assert not leaked, f"include_rules should restrict to (hr, employee), but these leaked through: {leaked}"


async def test_variant_include_and_skip_rules_with_masks(source_db, db_params):
    sens_out = output_dict("variant_include_skip.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_include_skip_with_masks.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))

    assert "email" in idx.get(("hr", "employee"), {}), f"hr.employee.email expected; got rules={idx}"
    assert "email" in idx.get(("billing", "customer"), {}), f"billing.customer.email expected; got rules={idx}"

    assert "phone" in idx.get(("hr", "employee"), {}), f"hr.employee.phone expected; got rules={idx}"

    audit_tables = [k for k in idx if k[0] == "audit"]
    assert not audit_tables, f"audit.* must be skipped, got: {audit_tables}"


async def test_variant_partial_constants_matches_substrings_in_data(source_db, db_params):
    sens_out = output_dict("variant_partial_constants.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_partial_constants.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))
    assert "email" in idx.get(("hr", "employee"), {}), (
        f"partial_constants should hit hr.employee.email; got {list(idx.get(('hr', 'employee'), {}))}"
    )


async def test_variant_words_and_phrases_constants(source_db, db_params):
    sens_out = output_dict("variant_words_and_phrases.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_words_and_phrases.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))
    assert "name" in idx.get(("hr", "department"), {}), (
        f"data_const constants should hit hr.department.name; got {list(idx.get(('hr', 'department'), {}))}"
    )


async def test_variant_data_sql_condition_does_not_break_scan(source_db, db_params):
    sens_out = output_dict("variant_data_sql_condition.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_data_sql_condition.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE, "create-dict must accept sql_condition without error"

    idx = _rules_index(_load(sens_out))
    assert "email" in idx.get(("hr", "employee"), {}), (
        "regex over data sampled under sql_condition should still detect email"
    )


async def test_variant_data_func_with_custom_scan_func(source_db, db_params):
    sens_out = output_dict("variant_data_func.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_data_func.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))
    assert idx, "data_func scan must classify at least one column"

    assert "email" in idx.get(("hr", "employee"), {}), f"data_func should classify hr.employee.email; got {idx}"

    flat = json.dumps(_load(sens_out))
    assert "partial_email" in flat, "data_func anon_func template must appear on at least one classified column"


async def test_variant_data_func_per_field_with_custom_scan_func(source_db, db_params):
    sens_out = output_dict("variant_data_func_per_field.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_data_func_per_field.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE, "create-dict must accept scan_func_per_field without error"

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
    assert not missing, f"scan_func_per_field should classify these PII columns by name: {missing}"

    assert "pii_marker" in flat, "scan_func_per_field anon_func template must appear on at least one classified column"


def _markers_for(rules_for_table: dict[str, str]) -> set[str]:
    markers = set()
    for func in rules_for_table.values():
        for token in (
            "int_alias",
            "int4_alias",
            "int8_alias",
            "bool_alias",
            "varchar_alias",
            "text_alias",
            "timestamp_alias",
            "timestamptz_alias",
            "varchar20_complex",
            "double_complex",
            "timestamp3_complex",
            "timetz3_complex",
            "default",
        ):
            if token in func:
                markers.add(token)
    return markers


async def test_variant_type_aliases_picks_per_alias_func(source_db, db_params):
    sens_out = output_dict("variant_type_aliases.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_type_aliases.py")],
            sens_out=sens_out,
        )
    ).run()
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
    assert not missing, f"type-alias resolution missed: {missing}; observed markers: {used}"


async def test_variant_type_aliases_complex_handles_whitespace_and_precision(source_db, db_params):
    sens_out = output_dict("variant_type_aliases_complex.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_type_aliases_complex.py")],
            sens_out=sens_out,
        )
    ).run()
    assert res.result_code == ResultCode.DONE, "create-dict must normalize complex type aliases without raising"

    idx = _rules_index(_load(sens_out))
    sample_rules = idx.get(("data_types", "sample"), {})
    assert sample_rules, "include_rules must surface data_types.sample fields"

    used = _markers_for(sample_rules)
    complex_markers = {"varchar20_complex", "double_complex", "timestamp3_complex", "timetz3_complex"}
    assert used & complex_markers, (
        f"no complex-alias func picked up — normalization likely broken; observed markers: {used}"
    )


async def test_variant_partial_scan_turns_off_synchronized_seqscans(source_db, db_params):
    def options(scan_mode, sens_out):
        return _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_seqscan_sync.py")],
            sens_out=sens_out,
            scan_mode=scan_mode,
        )

    partial_out = output_dict("variant_seqscan_sync_partial.json")
    res = await PgAnonApp(options("partial", partial_out)).run()
    assert res.result_code == ResultCode.DONE
    partial_idx = _rules_index(_load(partial_out))
    assert "email" in partial_idx.get(("hr", "employee"), {}), "partial scan must run with synchronize_seqscans = off"

    full_out = output_dict("variant_seqscan_sync_full.json")
    res = await PgAnonApp(options("full", full_out)).run()
    assert res.result_code == ResultCode.DONE
    assert not _rules_index(_load(full_out)), "full scan must keep the default synchronize_seqscans"


async def test_variant_partial_scan_data_sql_condition_turns_off_parallel_plan(source_db, db_params):
    sens_out = output_dict("variant_parallel_off_condition.json")
    res = await PgAnonApp(
        _options(
            db_params,
            source_db,
            meta_dicts=[input_dict("meta_parallel_off_condition.py")],
            sens_out=sens_out,
            scan_mode="partial",
        )
    ).run()
    assert res.result_code == ResultCode.DONE

    idx = _rules_index(_load(sens_out))
    assert "phone" in idx.get(("hr", "employee"), {}), (
        "table with data_sql_condition must scan without parallel workers"
    )
    assert ("billing", "customer") not in idx, "table without data_sql_condition must keep the default parallel setting"


async def test_variant_partial_scan_output_is_sorted_and_same_for_any_connections(source_db, db_manager, db_params):
    outputs = {}
    for connections in (1, 4):
        sens_out = output_dict(f"variant_sorted_sens_{connections}.json")
        no_sens_out = output_dict(f"variant_sorted_no_sens_{connections}.json")
        res = await PgAnonApp(
            _options(
                db_params,
                source_db,
                meta_dicts=[input_dict("meta_basic.py")],
                sens_out=sens_out,
                no_sens_out=no_sens_out,
                scan_mode="partial",
                db_connections=connections,
            )
        ).run()
        assert res.result_code == ResultCode.DONE
        outputs[connections] = (Path(sens_out).read_bytes(), Path(no_sens_out).read_bytes())

    assert outputs[1][0] == outputs[4][0], "sens dict must not depend on --db-connections-per-process"
    assert outputs[1][1] == outputs[4][1], "no-sens dict must not depend on --db-connections-per-process"

    attnums = {
        (r["nspname"], r["relname"], r["attname"]): r["attnum"]
        for r in await db_manager.fetch(
            source_db,
            """
            SELECT n.nspname, c.relname, a.attname, a.attnum
              FROM pg_attribute a
              JOIN pg_class c ON c.oid = a.attrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE a.attnum > 0 AND NOT a.attisdropped
            """,
        )
    }
    sens_rules = json.loads(outputs[1][0])["dictionary"]
    no_sens_rules = json.loads(outputs[1][1])["no_sens_dictionary"]
    assert any(len(rule["fields"]) > 1 for rule in sens_rules), "fixture sanity: a table must have several sens fields"

    for rules in (sens_rules, no_sens_rules):
        tables = [(rule["schema"], rule["table"]) for rule in rules]
        assert tables == sorted(tables), "tables must be sorted by schema and table"
        for rule in rules:
            fields = list(rule["fields"])
            by_attnum = sorted(fields, key=lambda f: attnums[(rule["schema"], rule["table"], f)])
            assert fields == by_attnum, f"fields of {rule['schema']}.{rule['table']} must be sorted by attnum"


def _funcs_of(path, schema, table):
    return _rules_index(_load(path)).get((schema, table), {})


async def test_rule_objects_set_function_in_every_search_section(source_db, db_params):
    sens_out = output_dict("rule_objects.json")
    res = await PgAnonApp(
        _options(db_params, source_db, meta_dicts=[input_dict("meta_rule_objects.py")], sens_out=sens_out)
    ).run()
    assert res.result_code == ResultCode.DONE

    employee = _funcs_of(sens_out, "hr", "employee")
    assert "'type_salt'" in employee["first_name"], "string rule in a mixed list must use the function by type"
    assert "'field_rule'" in employee["last_name"], "field.rules object, and field name goes before data_regex"
    assert "'field_constant'" in employee["ssn"], "field.constants object"
    assert "'regex_email'" in employee["email"], "data_regex object"
    assert "'regex_phone'" in employee["phone"], "data_regex object after a rule of other column in the list"

    assert "'const_word'" in _funcs_of(sens_out, "hr", "department")["name"], "data_const.constants word"
    assert "'const_phrase'" in _funcs_of(sens_out, "hr", "performance_review")["notes"], "data_const.constants phrase"
    assert "'partial_const'" in _funcs_of(sens_out, "hr", "salary_history")["reason"], "data_const.partial_constants"


async def test_rule_higher_in_the_list_wins_when_column_matches_several_regexes(source_db, db_params):
    sens_out = output_dict("first_regex_wins.json")
    res = await PgAnonApp(
        _options(db_params, source_db, meta_dicts=[input_dict("meta_first_regex_wins.py")], sens_out=sens_out)
    ).run()
    assert res.result_code == ResultCode.DONE

    assert "'regex_first'" in _funcs_of(sens_out, "hr", "employee")["phone"]


async def test_same_rule_in_two_files_uses_the_first_one(source_db, db_params):
    sens_out = output_dict("same_rule_two_files.json")
    meta_dicts = [input_dict("meta_same_rule_first_file.py"), input_dict("meta_same_rule_second_file.py")]
    res = await PgAnonApp(_options(db_params, source_db, meta_dicts=meta_dicts, sens_out=sens_out)).run()
    assert res.result_code == ResultCode.DONE

    email = _funcs_of(sens_out, "hr", "employee")["email"]
    assert "'type_salt'" in email, "the string rule from the first file wins, so the function is chosen by type"
    assert "'second_file'" not in email


async def test_prepared_sens_dict_goes_before_anon_func_of_rule(source_db, db_params, tmp_path):
    prepared = tmp_path / "prepared_sens.json"
    prepared_funcs = {
        "email": "anon_funcs.digest(\"email\"::text, 'prepared', 'md5')::citext",
        "last_name": "anon_funcs.digest(\"last_name\", 'prepared', 'md5')",
    }
    prepared.write_text(
        json.dumps({"dictionary": [{"schema": "hr", "table": "employee", "fields": prepared_funcs}]}),
        encoding="utf-8",
    )
    sens_out = output_dict("prepared_before_rule_func.json")
    options = _options(
        db_params,
        source_db,
        meta_dicts=[input_dict("meta_rule_objects.py")],
        sens_out=sens_out,
        prepared_sens=str(prepared),
    )
    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE

    employee = _funcs_of(sens_out, "hr", "employee")
    assert employee["email"] == prepared_funcs["email"], "data_regex object must not replace the prepared function"
    assert employee["last_name"] == prepared_funcs["last_name"], "field.rules object must not replace it either"
    assert "'regex_phone'" in employee["phone"], "fields out of the prepared dict still use the function of the rule"


async def test_function_of_data_func_goes_before_anon_func_of_data_const(source_db, db_params):
    sens_out = output_dict("data_func_before_const.json")
    res = await PgAnonApp(
        _options(db_params, source_db, meta_dicts=[input_dict("meta_data_func_before_const.py")], sens_out=sens_out)
    ).run()
    assert res.result_code == ResultCode.DONE

    email = _funcs_of(sens_out, "hr", "employee")["email"]
    assert "'data_func'" in email
    assert "'partial_const'" not in email

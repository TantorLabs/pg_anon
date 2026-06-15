from __future__ import annotations


def _rule_by_table(dictionary: list[dict], schema: str, table: str) -> dict:
    matches = [r for r in dictionary if r.get("schema") == schema and r.get("table") == table]
    assert len(matches) == 1, f"expected exactly one rule for {schema}.{table}, got {len(matches)}"
    return matches[0]


class TestPreparedSensMerge:
    def test_fields_of_same_table_are_merged_across_files(self, write_dict, make_context):
        file_a = write_dict(
            {
                "dictionary": [
                    {"schema": "public", "table": "customers", "fields": {"phone": 'anon_funcs.phone("phone")'}}
                ]
            }
        )
        file_b = write_dict(
            {"dictionary": [{"schema": "public", "table": "customers", "fields": {"name": 'anon_funcs.name("name")'}}]}
        )

        ctx = make_context(prepared_sens_dict_files=[file_a, file_b])
        ctx.read_prepared_dict()

        rule = _rule_by_table(ctx.prepared_dictionary_obj["dictionary"], "public", "customers")
        assert rule["fields"] == {
            "phone": 'anon_funcs.phone("phone")',
            "name": 'anon_funcs.name("name")',
        }

    def test_same_field_rule_is_overwritten_by_later_file(self, write_dict, make_context):
        file_a = write_dict(
            {"dictionary": [{"schema": "public", "table": "customers", "fields": {"phone": "RULE_FROM_A"}}]}
        )
        file_b = write_dict(
            {"dictionary": [{"schema": "public", "table": "customers", "fields": {"phone": "RULE_FROM_B"}}]}
        )

        ctx = make_context(prepared_sens_dict_files=[file_a, file_b])
        ctx.read_prepared_dict()

        rule = _rule_by_table(ctx.prepared_dictionary_obj["dictionary"], "public", "customers")
        assert rule["fields"] == {"phone": "RULE_FROM_B"}

    def test_merge_and_overwrite_combined(self, write_dict, make_context):
        file_a = write_dict(
            {
                "dictionary": [
                    {"schema": "public", "table": "customers", "fields": {"phone": "A_phone", "email": "A_email"}}
                ]
            }
        )
        file_b = write_dict(
            {
                "dictionary": [
                    {"schema": "public", "table": "customers", "fields": {"phone": "B_phone", "name": "B_name"}}
                ]
            }
        )

        ctx = make_context(prepared_sens_dict_files=[file_a, file_b])
        ctx.read_prepared_dict()

        rule = _rule_by_table(ctx.prepared_dictionary_obj["dictionary"], "public", "customers")
        assert rule["fields"] == {"phone": "B_phone", "email": "A_email", "name": "B_name"}

    def test_no_sql_condition_on_either_side_does_not_crash(self, write_dict, make_context):
        file_a = write_dict(
            {"dictionary": [{"schema": "public", "table": "customers", "fields": {"phone": "A_phone"}}]}
        )
        file_b = write_dict({"dictionary": [{"schema": "public", "table": "customers", "fields": {"name": "B_name"}}]})

        ctx = make_context(prepared_sens_dict_files=[file_a, file_b])
        ctx.read_prepared_dict()

        rule = _rule_by_table(ctx.prepared_dictionary_obj["dictionary"], "public", "customers")
        assert rule["fields"] == {"phone": "A_phone", "name": "B_name"}
        assert rule.get("sql_condition") is None

    def test_sql_condition_preserved_when_later_file_lacks_it(self, write_dict, make_context):
        file_a = write_dict(
            {
                "dictionary": [
                    {
                        "schema": "public",
                        "table": "customers",
                        "fields": {"phone": "A_phone"},
                        "sql_condition": "WHERE created > NOW() - '7 days'::interval",
                    }
                ]
            }
        )
        file_b = write_dict({"dictionary": [{"schema": "public", "table": "customers", "fields": {"name": "B_name"}}]})

        ctx = make_context(prepared_sens_dict_files=[file_a, file_b])
        ctx.read_prepared_dict()

        rule = _rule_by_table(ctx.prepared_dictionary_obj["dictionary"], "public", "customers")
        assert rule["sql_condition"] == "WHERE created > NOW() - '7 days'::interval"
        assert rule["fields"] == {"phone": "A_phone", "name": "B_name"}

    def test_sql_condition_overwritten_when_later_file_has_it(self, write_dict, make_context):
        file_a = write_dict(
            {
                "dictionary": [
                    {"schema": "public", "table": "customers", "fields": {"phone": "A"}, "sql_condition": "WHERE a"}
                ]
            }
        )
        file_b = write_dict(
            {
                "dictionary": [
                    {"schema": "public", "table": "customers", "fields": {"name": "B"}, "sql_condition": "WHERE b"}
                ]
            }
        )

        ctx = make_context(prepared_sens_dict_files=[file_a, file_b])
        ctx.read_prepared_dict()

        rule = _rule_by_table(ctx.prepared_dictionary_obj["dictionary"], "public", "customers")
        assert rule["sql_condition"] == "WHERE b"

    def test_different_tables_stay_separate(self, write_dict, make_context):
        file_a = write_dict({"dictionary": [{"schema": "public", "table": "customers", "fields": {"phone": "A"}}]})
        file_b = write_dict({"dictionary": [{"schema": "public", "table": "orders", "fields": {"address": "B"}}]})

        ctx = make_context(prepared_sens_dict_files=[file_a, file_b])
        ctx.read_prepared_dict()

        tables = {(r["schema"], r["table"]) for r in ctx.prepared_dictionary_obj["dictionary"]}
        assert tables == {("public", "customers"), ("public", "orders")}

    def test_exclude_and_validate_sections_accumulate(self, write_dict, make_context):
        file_a = write_dict(
            {
                "dictionary": [{"schema": "public", "table": "customers", "fields": {"phone": "A"}}],
                "dictionary_exclude": [{"schema": "public", "table": "logs"}],
                "validate_tables": [{"schema": "public", "table": "customers"}],
            }
        )
        file_b = write_dict(
            {
                "dictionary": [{"schema": "public", "table": "orders", "fields": {"address": "B"}}],
                "dictionary_exclude": [{"schema_mask": "tenant_.*"}],
                "validate_tables": [{"schema": "public", "table": "orders"}],
            }
        )

        ctx = make_context(prepared_sens_dict_files=[file_a, file_b])
        ctx.read_prepared_dict()

        assert ctx.prepared_dictionary_obj["dictionary_exclude"] == [
            {"schema": "public", "table": "logs"},
            {"schema_mask": "tenant_.*"},
        ]
        assert ctx.prepared_dictionary_obj["validate_tables"] == [
            {"schema": "public", "table": "customers"},
            {"schema": "public", "table": "orders"},
        ]

    def test_dictionary_contents_records_all_files(self, write_dict, make_context):
        file_a = write_dict({"dictionary": [{"schema": "public", "table": "customers", "fields": {"phone": "A"}}]})
        file_b = write_dict({"dictionary": [{"schema": "public", "table": "orders", "fields": {"address": "B"}}]})

        ctx = make_context(prepared_sens_dict_files=[file_a, file_b])
        ctx.read_prepared_dict()

        assert set(ctx.prepared_dictionary_contents.keys()) == {file_a, file_b}


class TestMetaDictMerge:
    def test_list_sections_accumulate(self, write_dict, make_context):
        file_a = write_dict(
            {
                "field": {"rules": ["^a_"], "constants": ["email"]},
                "skip_rules": [{"schema": "tmp_a"}],
                "include_rules": [{"schema": "inc_a"}],
                "data_regex": {"rules": ["a-regex"]},
                "data_sql_condition": [{"schema": "public", "table": "t_a", "sql_condition": "WHERE a"}],
            }
        )
        file_b = write_dict(
            {
                "field": {"rules": ["^b_"], "constants": ["password"]},
                "skip_rules": [{"schema": "tmp_b"}],
                "include_rules": [{"schema": "inc_b"}],
                "data_regex": {"rules": ["b-regex"]},
                "data_sql_condition": [{"schema": "public", "table": "t_b", "sql_condition": "WHERE b"}],
            }
        )

        ctx = make_context(meta_dict_files=[file_a, file_b])
        ctx.read_meta_dict()
        meta = ctx.meta_dictionary_obj

        assert {p.pattern for p in meta["field"]["rules"]} == {"^a_", "^b_"}
        assert meta["field"]["constants"] == ["email", "password"]
        assert meta["skip_rules"] == [{"schema": "tmp_a"}, {"schema": "tmp_b"}]
        assert meta["include_rules"] == [{"schema": "inc_a"}, {"schema": "inc_b"}]
        assert {p.pattern for p in meta["data_regex"]["rules"]} == {"a-regex", "b-regex"}
        assert meta["data_sql_condition"] == [
            {"schema": "public", "table": "t_a", "sql_condition": "WHERE a"},
            {"schema": "public", "table": "t_b", "sql_condition": "WHERE b"},
        ]

    def test_data_const_words_and_phrases_union(self, write_dict, make_context):
        file_a = write_dict({"data_const": {"constants": ["alpha"], "partial_constants": ["@a.com"]}})
        file_b = write_dict({"data_const": {"constants": ["beta"], "partial_constants": ["@b.com"]}})

        ctx = make_context(meta_dict_files=[file_a, file_b])
        ctx.read_meta_dict()
        const = ctx.meta_dictionary_obj["data_const"]

        assert {"alpha", "beta"} <= const["constants"]["words"]
        assert const["partial_constants"] == {"@a.com", "@b.com"}

    def test_funcs_same_type_overwritten_other_types_kept(self, write_dict, make_context):
        file_a = write_dict({"funcs": {"text": "FUNC_TEXT_A", "integer": "FUNC_INT_A"}})
        file_b = write_dict({"funcs": {"text": "FUNC_TEXT_B", "bigint": "FUNC_BIGINT_B"}})

        ctx = make_context(meta_dict_files=[file_a, file_b])
        ctx.read_meta_dict()
        funcs = ctx.meta_dictionary_obj["funcs"]

        assert funcs["text"] == "FUNC_TEXT_B"  # last file wins on clash
        assert funcs["integer"] == "FUNC_INT_A"
        assert funcs["bigint"] == "FUNC_BIGINT_B"

    def test_data_func_same_type_overwritten(self, write_dict, make_context):
        file_a = write_dict({"data_func": {"text": [{"scan_func": "scan_a", "anon_func": "anon_a"}]}})
        file_b = write_dict({"data_func": {"text": [{"scan_func": "scan_b", "anon_func": "anon_b"}]}})

        ctx = make_context(meta_dict_files=[file_a, file_b])
        ctx.read_meta_dict()

        assert ctx.meta_dictionary_obj["data_func"]["text"] == [{"scan_func": "scan_b", "anon_func": "anon_b"}]

    def test_sens_pg_types_from_all_files_present(self, write_dict, make_context):
        file_a = write_dict({"sens_pg_types": ["custom_type_a"]})
        file_b = write_dict({"sens_pg_types": ["custom_type_b"]})

        ctx = make_context(meta_dict_files=[file_a, file_b])
        ctx.read_meta_dict()

        assert "custom_type_a" in ctx.meta_dictionary_obj["sens_pg_types"]
        assert "custom_type_b" in ctx.meta_dictionary_obj["sens_pg_types"]

    def test_no_sens_dictionary_accumulates_from_prepared_no_sens_files(self, write_dict, make_context):
        file_a = write_dict({"no_sens_dictionary": [{"schema": "public", "table": "t1", "fields": ["id"]}]})
        file_b = write_dict({"no_sens_dictionary": [{"schema": "public", "table": "t2", "fields": ["code"]}]})

        ctx = make_context(prepared_no_sens_dict_files=[file_a, file_b])
        ctx.read_meta_dict()

        assert ctx.meta_dictionary_obj["no_sens_dictionary"] == [
            {"schema": "public", "table": "t1", "fields": ["id"]},
            {"schema": "public", "table": "t2", "fields": ["code"]},
        ]

    def test_no_sens_same_table_keeps_both_rules(self, write_dict, make_context):
        file_a = write_dict({"no_sens_dictionary": [{"schema": "public", "table": "customers", "fields": ["phone"]}]})
        file_b = write_dict({"no_sens_dictionary": [{"schema": "public", "table": "customers", "fields": ["name"]}]})

        ctx = make_context(prepared_no_sens_dict_files=[file_a, file_b])
        ctx.read_meta_dict()

        no_sens = ctx.meta_dictionary_obj["no_sens_dictionary"]
        assert no_sens == [
            {"schema": "public", "table": "customers", "fields": ["phone"]},
            {"schema": "public", "table": "customers", "fields": ["name"]},
        ]

        marked_fields = {f for rule in no_sens for f in rule["fields"]}
        assert marked_fields == {"phone", "name"}

    def test_meta_and_no_sens_files_merged_together(self, write_dict, make_context):
        meta_file = write_dict({"field": {"rules": ["^secret_"]}})
        no_sens_file = write_dict({"no_sens_dictionary": [{"schema": "public", "table": "t1", "fields": ["id"]}]})

        ctx = make_context(meta_dict_files=[meta_file], prepared_no_sens_dict_files=[no_sens_file])
        ctx.read_meta_dict()

        assert {p.pattern for p in ctx.meta_dictionary_obj["field"]["rules"]} == {"^secret_"}
        assert ctx.meta_dictionary_obj["no_sens_dictionary"] == [{"schema": "public", "table": "t1", "fields": ["id"]}]


class TestPartialTablesMerge:
    def test_include_rules_accumulate(self, write_dict, make_context):
        file_a = write_dict({"tables": [{"schema": "public", "table": "employees"}]})
        file_b = write_dict({"tables": [{"schema": "ecommerce", "table_mask": "^orders"}]})

        ctx = make_context(partial_tables_dict_files=[file_a, file_b])
        ctx.read_partial_tables_dicts()

        assert ctx.included_tables_rules == [
            {"schema": "public", "table": "employees"},
            {"schema": "ecommerce", "table_mask": "^orders"},
        ]
        assert ctx.excluded_tables_rules == []

    def test_exclude_rules_accumulate(self, write_dict, make_context):
        file_a = write_dict({"tables": [{"schema_mask": "tenant_.*"}]})
        file_b = write_dict({"tables": [{"schema": "public", "table": "logs"}]})

        ctx = make_context(partial_tables_exclude_dict_files=[file_a, file_b])
        ctx.read_partial_tables_dicts()

        assert ctx.excluded_tables_rules == [
            {"schema_mask": "tenant_.*"},
            {"schema": "public", "table": "logs"},
        ]
        assert ctx.included_tables_rules == []

    def test_include_and_exclude_merged_independently(self, write_dict, make_context):
        inc_a = write_dict({"tables": [{"schema": "public", "table": "a"}]})
        inc_b = write_dict({"tables": [{"schema": "public", "table": "b"}]})
        exc_a = write_dict({"tables": [{"schema": "public", "table": "c"}]})

        ctx = make_context(
            partial_tables_dict_files=[inc_a, inc_b],
            partial_tables_exclude_dict_files=[exc_a],
        )
        ctx.read_partial_tables_dicts()

        assert ctx.included_tables_rules == [
            {"schema": "public", "table": "a"},
            {"schema": "public", "table": "b"},
        ]
        assert ctx.excluded_tables_rules == [{"schema": "public", "table": "c"}]

import asyncio
import json
import re
import shutil
import sys
import time
from collections.abc import Callable
from itertools import chain
from pathlib import Path

from asyncpg import Connection, Pool

from pg_anon.common.constants import DEFAULT_HASH_FUNC
from pg_anon.common.db_queries import get_data_from_field_query
from pg_anon.common.db_utils import (
    check_required_connections,
    create_connection,
    create_pool,
    exec_data_scan_func_per_field_query,
    exec_data_scan_func_query,
    get_scan_fields_list,
)
from pg_anon.common.dto import FieldInfo, SearchRule
from pg_anon.common.enums import MatchSource, ScanMode
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import (
    get_base_field_type,
    get_dict_rule_for_table,
    safe_compile,
    save_dicts_info_file,
    setof_to_list,
)
from pg_anon.context import Context


def _field_sort_key(field_info: FieldInfo) -> tuple[str, str, int]:
    return field_info.nspname, field_info.relname, field_info.attnum


def _first_rule_by_values(
    fld_data: list, find_rule: Callable[[str, int], SearchRule | None]
) -> tuple[SearchRule, str] | None:
    """Find the first rule of the list that matches at least one value; return it with the matched value."""
    found = None
    bound = sys.maxsize
    for value in fld_data:
        if value is None:
            continue

        # find_rule gives the matched rule with the smallest position below the bound
        rule = find_rule(value, bound)
        if rule is not None:
            found = rule, value
            # Rules below the nearest rule with another anon_func can not change the result
            bound = rule.other_func_above + 1
            if bound == 0:
                break
    return found


class CreateDictMode:
    def __init__(self, context: Context) -> None:
        self.context = context

    def _check_field_match_by_rule(self, field: dict, rule: dict) -> bool:
        schema_matched = False
        table_matched = False
        field_matched = False

        if "schema" in rule and field["nspname"] == rule["schema"]:
            schema_matched = True
        elif "schema_mask" in rule:
            if rule["schema_mask"] == "*" or re.search(safe_compile(rule["schema_mask"]), field["nspname"]) is not None:
                schema_matched = True
        else:
            # Required schema or schema_mask
            return False

        if "table" not in rule:
            if (
                "table_mask" not in rule
                or rule["table_mask"] == "*"
                or re.search(safe_compile(rule["table_mask"]), field["relname"]) is not None
            ):
                table_matched = True
        elif field["relname"] == rule["table"]:
            table_matched = True

        if "fields" not in rule or field["column_name"] in rule["fields"]:
            field_matched = True

        return bool(schema_matched and table_matched and field_matched)

    def _check_not_skip_fields(self, field: dict) -> bool:
        for rule in self.context.meta_dictionary_obj["skip_rules"]:
            if self._check_field_match_by_rule(field=field, rule=rule):
                self.context.logger.debug(
                    "------> Field %s.%s.%s skipped by rule %s",
                    field["nspname"],
                    field["relname"],
                    field["column_name"],
                    rule,
                )
                return False

        return True

    def _check_include_fields(self, field: dict) -> bool:
        if not self.context.meta_dictionary_obj["include_rules"]:
            return True

        for rule in self.context.meta_dictionary_obj["include_rules"]:
            if self._check_field_match_by_rule(field=field, rule=rule):
                self.context.logger.debug(
                    "------> Field %s.%s.%s included by rule %s",
                    field["nspname"],
                    field["relname"],
                    field["column_name"],
                    rule,
                )
                return True

        return False

    async def _get_fields_for_scan(self) -> dict[str, FieldInfo]:
        """Get scanning fields for create dictionary mode."""
        fields_list = await get_scan_fields_list(
            connection_params=self.context.connection_params, server_settings=self.context.server_settings
        )

        return {
            field["obj_id"]: FieldInfo(**field)
            for field in fields_list
            if self._check_include_fields(field) and self._check_not_skip_fields(field)
        }

    def _scan_fields_by_names(self, fields_info: dict[str, FieldInfo]) -> None:  # noqa: C901, PLR0912
        """Scan fields by names and remove matches according to dict rules.

        Priorities of rules:
            - prepared-sens-dict-file
            - meta-dict-file
            - prepared-no-sens-dict-file
        """
        for obj_id, field_info in fields_info.copy().items():
            matched: bool = False
            include_rule: dict | None = None
            exclude_rule: dict | None = None

            if self.context.prepared_dictionary_obj.get("dictionary"):
                include_rule = get_dict_rule_for_table(
                    dictionary_rules=self.context.prepared_dictionary_obj["dictionary"],
                    schema=field_info.nspname,
                    table=field_info.relname,
                )

            if self.context.prepared_dictionary_obj.get("dictionary_exclude"):
                exclude_rule = get_dict_rule_for_table(
                    dictionary_rules=self.context.prepared_dictionary_obj["dictionary_exclude"],
                    schema=field_info.nspname,
                    table=field_info.relname,
                )

            # include_rule + has field in include_rule => sensitive field
            # not include_rule + exclude_rule => not sensitive field
            # not include_rule + not exclude_rule => unknown. Go to next rules priority -> check by meta-dict
            if include_rule and field_info.column_name in include_rule["fields"]:
                self.context.logger.debug(
                    '------> Field %s.%s.%s is SENSITIVE by rule "%s"',
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    include_rule,
                )
                del fields_info[obj_id]
                field_info.rule = include_rule["fields"][field_info.column_name]
                self.context.create_dict_sens_matches[obj_id] = field_info
                matched = True

            elif exclude_rule:
                self.context.logger.debug(
                    '------> Field %s.%s.%s is INSENSITIVE by rule "%s"',
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    exclude_rule,
                )
                del fields_info[obj_id]
                self.context.create_dict_no_sens_matches[obj_id] = field_info
                matched = True

            if matched:
                continue

            for rule in self.context.meta_dictionary_obj["field"]["constants"]:
                if rule.rule == field_info.column_name:
                    if obj_id in fields_info:
                        self.context.logger.debug(
                            '------> Field %s.%s.%s is SENSITIVE by rule "%s"',
                            field_info.nspname,
                            field_info.relname,
                            field_info.column_name,
                            rule.rule,
                        )
                        del fields_info[obj_id]
                        field_info.rule = rule.anon_func
                        self.context.create_dict_sens_matches[obj_id] = field_info
                    matched = True
                    break

            if matched:
                continue

            for rule in self.context.meta_dictionary_obj["field"]["rules"]:
                if rule.rule.search(field_info.column_name) is not None:
                    if obj_id in fields_info:
                        self.context.logger.debug(
                            '------> Field %s.%s.%s is SENSITIVE by rule "%s"',
                            field_info.nspname,
                            field_info.relname,
                            field_info.column_name,
                            rule.rule,
                        )
                        del fields_info[obj_id]
                        field_info.rule = rule.anon_func
                        self.context.create_dict_sens_matches[obj_id] = field_info
                    matched = True
                    break

            if matched:
                continue

            for rule in self.context.meta_dictionary_obj["no_sens_dictionary"]:
                if (
                    rule["schema"] == field_info.nspname
                    and rule["table"] == field_info.relname
                    and field_info.column_name in rule["fields"]
                ):
                    if obj_id in fields_info:
                        self.context.logger.debug(
                            '------> Field %s.%s.%s is INSENSITIVE by rule "%s"',
                            field_info.nspname,
                            field_info.relname,
                            field_info.column_name,
                            rule,
                        )
                        del fields_info[obj_id]
                        self.context.create_dict_no_sens_matches[obj_id] = field_info
                    break

    def _check_data_by_constants(
        self, name: str, dictionary_obj: dict, field_info: FieldInfo, fld_data: list
    ) -> SearchRule | None:
        words = dictionary_obj["data_const"]["constants"]["words"]
        phrases = dictionary_obj["data_const"]["constants"]["phrases"]
        rules = dictionary_obj["data_const"]["constant_rules"]
        if not words and not phrases:
            return None

        self.context.logger.debug(
            "========> Process[%s]: checking by constants data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        min_length = self.context.data_const_constants_min_length or 0

        def find_rule(value: str, bound: int) -> SearchRule | None:
            found = None
            lower_value = value.lower()
            value_words = (word.lower() for word in value.split() if len(word) >= min_length)
            constants = chain((w for w in value_words if w in words), (p for p in phrases if p in lower_value))
            for constant in constants:
                rule = rules[constant]
                if rule.position < bound and (found is None or rule.position < found.position):
                    if rule.other_func_above < 0:
                        return rule
                    found = rule
            return found

        if matched := _first_rule_by_values(fld_data, find_rule):
            self.context.logger.debug(
                "========> Process[%s]: Field %s.%s.%s is SENSITIVE by constant %s",
                name,
                field_info.nspname,
                field_info.relname,
                field_info.column_name,
                matched[0].rule,
            )
            return matched[0]

        self.context.logger.debug(
            "========> Process[%s]: No one constants matched in data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        return None

    def _check_data_by_partial_constants(
        self, name: str, dictionary_obj: dict, field_info: FieldInfo, fld_data: list
    ) -> SearchRule | None:
        rules = dictionary_obj["data_const"]["partial_constant_rules"]
        if not rules:
            return None

        self.context.logger.debug(
            "========> Process[%s]: checking by partial constants data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )

        def find_rule(value: str, bound: int) -> SearchRule | None:
            lower_value = value.lower()
            for rule in rules.values():
                if rule.position >= bound:
                    break
                if rule.rule in lower_value:
                    return rule
            return None

        if matched := _first_rule_by_values(fld_data, find_rule):
            self.context.logger.debug(
                "========> Process[%s]: Field %s.%s.%s is SENSITIVE by partial constant %s",
                name,
                field_info.nspname,
                field_info.relname,
                field_info.column_name,
                matched[0].rule,
            )
            return matched[0]

        self.context.logger.debug(
            "========> Process[%s]: No one partial constants matched in data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        return None

    async def _check_data_by_functions(  # noqa: C901, PLR0912
        self, connection: Connection, name: str, dictionary_obj: dict, field_info: FieldInfo, fld_data: list
    ) -> SearchRule | None:
        if not dictionary_obj["data_func"]:
            return None

        self.context.logger.debug(
            "========> Process[%s]: checking by functions data of field %s.%s.%s (type = %s)",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
            field_info.type,
        )

        rules_by_type = dictionary_obj["data_func"].get(field_info.type, [])
        if not rules_by_type:
            rules_by_type = dictionary_obj["data_func"].get(get_base_field_type(field_info), [])
        rules_for_anyelements = dictionary_obj["data_func"].get("anyelement", [])
        data_func_rules = [*rules_by_type, *rules_for_anyelements]

        if not data_func_rules:
            self.context.logger.debug(
                "========> Process[%s]: Not found data checking functions for field %s.%s.%s (type = %s)",
                name,
                field_info.nspname,
                field_info.relname,
                field_info.column_name,
                field_info.type,
            )
            return None

        scan_func_per_field_rules = []
        scan_func_rules = []
        for rule in data_func_rules:
            if "scan_func_per_field" in rule:
                scan_func_per_field_rules.append(rule)
            elif "scan_func" in rule:
                scan_func_rules.append(rule)
            else:
                self.context.logger.warning("========> Process[%s]: Rule SKIPPED. Wrong data func rule: %s", name, rule)

        for rule in scan_func_per_field_rules:
            if await exec_data_scan_func_per_field_query(
                connection=connection, scan_func_per_field=rule["scan_func_per_field"], field_info=field_info
            ):
                self.context.logger.debug(
                    "========> Process[%s]: Field %s.%s.%s is SENSITIVE by data scan func per field %s",
                    name,
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    rule["scan_func_per_field"],
                )
                return SearchRule(rule["scan_func_per_field"], rule["anon_func"])

        for rule in scan_func_rules:
            matched_count = 0
            rule_expected_matches_count = rule.get("n_count", 1)

            for value in fld_data:
                if value is None:
                    continue

                if await exec_data_scan_func_query(
                    connection=connection, scan_func=rule["scan_func"], value=value, field_info=field_info
                ):
                    matched_count += 1

                    if matched_count == rule_expected_matches_count:
                        self.context.logger.debug(
                            "========> Process[%s]: Field %s.%s.%s is SENSITIVE by data scan func %s",
                            name,
                            field_info.nspname,
                            field_info.relname,
                            field_info.column_name,
                            rule["scan_func"],
                        )
                        return SearchRule(rule["scan_func"], rule["anon_func"])

        self.context.logger.debug(
            "========> Process[%s]: No one data functions found sensitive data in field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )

        return None

    def _check_data_by_regexp(
        self, name: str, dictionary_obj: dict, create_dict_matches: dict, field_info: FieldInfo, fld_data: list
    ) -> SearchRule | None:
        if field_info.obj_id in create_dict_matches:
            return None

        self.context.logger.debug(
            "========> Process[%s]: checking by regexp data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        rules = dictionary_obj["data_regex"]["rules"]

        def find_rule(value: str, bound: int) -> SearchRule | None:
            for rule in rules:
                if rule.position >= bound:
                    break
                if rule.rule.search(value) is not None:
                    return rule
            return None

        if rules and (matched := _first_rule_by_values(fld_data, find_rule)):
            self.context.logger.debug(
                "========> Process[%s]: Field %s.%s.%s is SENSITIVE by data_regex (regex=%s; value=%s)",
                name,
                field_info.nspname,
                field_info.relname,
                field_info.column_name,
                matched[0].rule,
                matched[1],
            )
            return matched[0]

        self.context.logger.debug(
            "========> Process[%s]: No one regexp rules found sensitive data in field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        return None

    async def _check_sensitive_data_in_fld(
        self,
        connection: Connection,
        name: str,
        dictionary_obj: dict,
        create_dict_matches: dict,
        field_info: FieldInfo,
        fld_data: list,
    ) -> dict:
        if not fld_data:
            self.context.logger.debug(
                "---> Process[%s]: Check sensitive skipped cause field %s.%s.%s has not data",
                name,
                field_info.nspname,
                field_info.relname,
                field_info.column_name,
            )
            return {}

        self.context.logger.debug(
            "---> Process[%s]: Started check sensitive data in field - %s.%s.%s. (%s values)",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
            len(fld_data),
        )

        for source in MatchSource:
            match source:
                case MatchSource.DATA_FUNC:
                    rule = await self._check_data_by_functions(connection, name, dictionary_obj, field_info, fld_data)
                case MatchSource.DATA_CONST:
                    rule = self._check_data_by_constants(name, dictionary_obj, field_info, fld_data)
                case MatchSource.DATA_PARTIAL_CONST:
                    rule = self._check_data_by_partial_constants(name, dictionary_obj, field_info, fld_data)
                case MatchSource.DATA_REGEX:
                    rule = self._check_data_by_regexp(name, dictionary_obj, create_dict_matches, field_info, fld_data)

            if rule is not None:
                field_info.rule = rule.anon_func
                self.context.logger.debug(
                    "<--- Process[%s]: Finished check sensitive data in field %s.%s.%s - is SENSITIVE by %s",
                    name,
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    source.value,
                )
                return {field_info.obj_id: field_info}

        self.context.logger.debug(
            "<--- Process[%s]: Finished check sensitive data in field %s.%s.%s - is INSENSITIVE",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        return {}

    def _field_can_be_sensitive_by_type(self, dictionary_obj: dict, field_info: FieldInfo) -> bool:
        if field_info.type in dictionary_obj["sens_pg_types"]:
            return True

        base_field_type = get_base_field_type(field_info)
        return base_field_type in dictionary_obj["sens_pg_types"]

    async def _scan_partial_data(
        self, connection: Connection, name: str, dictionary_obj: dict, field_info: FieldInfo, query: str
    ) -> dict:
        fld_data = await connection.fetch(query)
        return await self._check_sensitive_data_in_fld(
            connection=connection,
            name=name,
            dictionary_obj=dictionary_obj,
            create_dict_matches=self.context.create_dict_sens_matches,
            field_info=field_info,
            fld_data=setof_to_list(fld_data),
        )

    async def _scan_obj_func(  # noqa: C901
        self,
        name: str,
        pool: Pool,
        field_info: FieldInfo,
        scan_mode: ScanMode | None,
        dictionary_obj: dict,
        scan_partial_rows: int,
    ) -> dict[str, FieldInfo] | None:
        field_full_name = f"{field_info.nspname}.{field_info.relname}.{field_info.column_name}"

        self.context.logger.debug(
            "====>>> Process[%s]: Started scan task for field %s (%s)", name, field_full_name, field_info
        )

        start_t = time.time()
        if not self._field_can_be_sensitive_by_type(dictionary_obj, field_info):
            self.context.logger.debug(
                "Process[%s]: Field %s is INSENSITIVE by type %s", name, field_full_name, field_info.type
            )
            return None

        res: dict[str, FieldInfo] = {}
        condition = None
        if self.context.meta_dictionary_obj.get("data_sql_condition"):
            rule = get_dict_rule_for_table(
                dictionary_rules=self.context.meta_dictionary_obj["data_sql_condition"],
                schema=field_info.nspname,
                table=field_info.relname,
            )
            if rule:
                condition = rule.get("sql_condition")

        try:
            async with pool.acquire() as db_conn:
                if scan_mode == ScanMode.PARTIAL:
                    query = get_data_from_field_query(
                        field_info=field_info, limit=scan_partial_rows, condition=condition
                    )
                    if condition:
                        # A parallel plan under LIMIT returns rows in random order
                        async with db_conn.transaction():
                            await db_conn.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                            res = await self._scan_partial_data(db_conn, name, dictionary_obj, field_info, query)
                    else:
                        res = await self._scan_partial_data(db_conn, name, dictionary_obj, field_info, query)
                elif scan_mode == ScanMode.FULL:
                    async with db_conn.transaction(isolation="repeatable_read", readonly=True):
                        query = get_data_from_field_query(field_info=field_info, condition=condition)
                        cursor = await db_conn.cursor(query)
                        next_rows = True
                        while next_rows:
                            fld_data = await cursor.fetch(scan_partial_rows)
                            res = await self._check_sensitive_data_in_fld(
                                connection=db_conn,
                                name=name,
                                dictionary_obj=dictionary_obj,
                                create_dict_matches=self.context.create_dict_sens_matches,
                                field_info=field_info,
                                fld_data=setof_to_list(fld_data),
                            )
                            if len(fld_data) == 0 or len(res) > 0:
                                break

        except Exception as ex:
            field = f"{field_full_name} (type={field_info.type})"
            self.context.logger.exception("Exception in scan_obj_func:\n%s", field)
            raise PgAnonError(ErrorCode.SCAN_FIELD_ERROR, f"Can't execute task for field {field}. Error: {ex}") from ex

        end_t = time.time()
        if end_t - start_t > 10:  # noqa: PLR2004
            self.context.logger.debug(
                "Process[%s]: scan_obj_func took %s sec. Task %s", name, round(end_t - start_t, 2), field_info
            )

        self.context.logger.debug(
            "<<<<==== Process[%s]: Found %s items(s) Finished task %s ", name, len(res), field_info
        )
        return res

    async def _run_scan_tasks(self, fields_info_list: list[FieldInfo]) -> list:  # noqa: C901
        self.context.logger.info("Using %s concurrent connections", self.context.options.db_connections_per_process)

        server_settings = self.context.server_settings
        if self.context.options.scan_mode == ScanMode.PARTIAL:
            # Keeps the first rows of a big table the same for all scan tasks
            server_settings = {**server_settings, "synchronize_seqscans": "off"}

        pool = await create_pool(
            connection_params=self.context.connection_params,
            server_settings=server_settings,
            min_size=self.context.options.db_connections_per_process,
            max_size=self.context.options.db_connections_per_process,
        )

        results: list = []
        tasks: set[asyncio.Task] = set()

        status_ratio = 10
        if len(fields_info_list) > 1000:
            status_ratio = 100
        if len(fields_info_list) > 50000:
            status_ratio = 1000

        try:
            fields_count = len(fields_info_list)
            for idx, field_info in enumerate(fields_info_list):
                while len(tasks) >= self.context.options.db_connections_per_process:
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for done_task in done:
                        if exc := done_task.exception():
                            for t in tasks:
                                t.cancel()
                            raise exc
                        r = done_task.result()
                        if r is not None and len(r) > 0:
                            results.append(r)

                task = asyncio.create_task(
                    self._scan_obj_func(
                        "main",
                        pool,
                        field_info,
                        self.context.options.scan_mode,
                        self.context.meta_dictionary_obj,
                        self.context.options.scan_partial_rows,
                    )
                )
                tasks.add(task)

                if idx % status_ratio == 0:
                    progress_percents = round(float(idx) * 100 / fields_count, 2)
                    self.context.logger.info("Progress %s%%", progress_percents)

            while tasks:
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for done_task in done:
                    if exc := done_task.exception():
                        for t in tasks:
                            t.cancel()
                        raise exc
                    r = done_task.result()
                    if r is not None and len(r) > 0:
                        results.append(r)
        finally:
            await pool.close()

        return results

    def _prepare_sens_dict_rule(
        self, meta_dictionary_obj: dict, field_info: FieldInfo, prepared_sens_dict_rules: dict
    ) -> dict:
        hash_func = field_info.rule

        if hash_func is None:
            base_field_type = get_base_field_type(field_info)
            if field_info.type in meta_dictionary_obj["funcs"]:
                hash_func = meta_dictionary_obj["funcs"][field_info.type]
            elif base_field_type in meta_dictionary_obj["funcs"]:
                hash_func = meta_dictionary_obj["funcs"][base_field_type]
            else:
                hash_func = meta_dictionary_obj["funcs"].get("default", DEFAULT_HASH_FUNC)

        hash_func = hash_func.replace("%s", field_info.column_name)

        if field_info.tbl_id not in prepared_sens_dict_rules:
            prepared_sens_dict_rules[field_info.tbl_id] = {
                "schema": field_info.nspname,
                "table": field_info.relname,
                "fields": {field_info.column_name: hash_func},
            }
        else:
            prepared_sens_dict_rules[field_info.tbl_id]["fields"].update({field_info.column_name: hash_func})
        return prepared_sens_dict_rules

    def _prepare_no_sens_dict_rule(self, field_info: FieldInfo, prepared_no_sens_dict_rules: dict) -> dict:
        if field_info.tbl_id not in prepared_no_sens_dict_rules:
            prepared_no_sens_dict_rules[field_info.tbl_id] = {
                "schema": field_info.nspname,
                "table": field_info.relname,
                "fields": [field_info.column_name],
            }
        else:
            prepared_no_sens_dict_rules[field_info.tbl_id]["fields"].append(field_info.column_name)
        return prepared_no_sens_dict_rules

    async def _create_dict(self) -> None:  # noqa: C901, PLR0912
        fields_info: dict[str, FieldInfo] = await self._get_fields_for_scan()
        if not fields_info:
            raise PgAnonError(ErrorCode.NO_OBJECTS_FOR_SCAN, "No objects for scan!")

        # fill self.context.create_dict_sens_matches and self.context.create_dict_no_sens_matches
        self._scan_fields_by_names(fields_info)

        prepared_sens_dict_rules: dict = {}
        scanned_sens_fields: list[FieldInfo] = []
        need_prepare_no_sens_dict: bool = bool(self.context.options.output_no_sens_dict_file)

        if fields_info:
            scan_results = await self._run_scan_tasks(list(fields_info.values()))

            # Fill results based on scan tasks
            for res in scan_results:
                for field_info in res.values():
                    scanned_sens_fields.append(field_info)
                    if need_prepare_no_sens_dict:
                        del fields_info[field_info.obj_id]

        # ============================================================================================
        # Fill results based on check_sensitive_fld_names
        # ============================================================================================
        sens_fields = scanned_sens_fields + list(self.context.create_dict_sens_matches.values())
        for field_info in sorted(sens_fields, key=_field_sort_key):
            prepared_sens_dict_rules = self._prepare_sens_dict_rule(
                self.context.meta_dictionary_obj, field_info, prepared_sens_dict_rules
            )
        # ============================================================================================

        output_sens_dict = {"dictionary": list(prepared_sens_dict_rules.values())}

        output_sens_dict_filename = Path.cwd() / (self.context.options.output_sens_dict_file or "")
        output_dir = output_sens_dict_filename.parent
        output_dir.mkdir(parents=True, exist_ok=True)

        with output_sens_dict_filename.open("w", encoding="utf-8") as file:
            file.write(json.dumps(output_sens_dict, indent=4, ensure_ascii=False))

        if need_prepare_no_sens_dict:
            prepared_no_sens_dict_rules: dict = {}

            no_sens_fields = list(self.context.create_dict_no_sens_matches.values()) + list(fields_info.values())
            for field_info in sorted(no_sens_fields, key=_field_sort_key):
                prepared_no_sens_dict_rules = self._prepare_no_sens_dict_rule(field_info, prepared_no_sens_dict_rules)

            output_no_sens_dict = {"no_sens_dictionary": list(prepared_no_sens_dict_rules.values())}
            output_no_sens_dict_file_name = Path.cwd() / (self.context.options.output_no_sens_dict_file or "")
            with output_no_sens_dict_file_name.open("w", encoding="utf-8") as file:
                file.write(json.dumps(output_no_sens_dict, indent=4, ensure_ascii=False))

    def _save_input_dicts_to_run_dir(self) -> None:
        if not self.context.options.save_dicts:
            return

        input_dicts_dir = Path(self.context.options.run_dir) / "input"
        input_dicts_dir.mkdir(parents=True, exist_ok=True)

        input_dict_files: list[str] = list(self.context.options.meta_dict_files or [])
        if self.context.options.prepared_sens_dict_files:
            input_dict_files.extend(self.context.options.prepared_sens_dict_files)
        if self.context.options.prepared_no_sens_dict_files:
            input_dict_files.extend(self.context.options.prepared_no_sens_dict_files)

        for dict_file in input_dict_files:
            shutil.copy2(dict_file, input_dicts_dir / Path(dict_file).name)

    def _save_output_dicts_to_run_dir(self) -> None:
        if not self.context.options.save_dicts:
            return

        output_dicts_dir = Path(self.context.options.run_dir) / "output"
        output_dicts_dir.mkdir(parents=True, exist_ok=True)

        if self.context.options.output_sens_dict_file:
            shutil.copy2(
                self.context.options.output_sens_dict_file,
                output_dicts_dir / Path(self.context.options.output_sens_dict_file).name,
            )

        if self.context.options.output_no_sens_dict_file:
            shutil.copy2(
                self.context.options.output_no_sens_dict_file,
                output_dicts_dir / Path(self.context.options.output_no_sens_dict_file).name,
            )

    async def _check_available_connections(self) -> None:
        if self.context.options.disable_checks:
            return

        connection = await create_connection(
            self.context.connection_params, server_settings=self.context.server_settings
        )
        try:
            # scan pool plus this connection
            await check_required_connections(connection, self.context.options.db_connections_per_process + 1)
        finally:
            await connection.close()

    async def run(self) -> None:
        """Run the create_dict mode to scan and build the sensitive data dictionary."""
        self.context.logger.info("-------------> Started create_dict mode")

        try:
            self._save_input_dicts_to_run_dir()
            await self._check_available_connections()

            self.context.read_meta_dict()
            if self.context.options.prepared_sens_dict_files:
                self.context.read_prepared_dict()
            await self._create_dict()

            self._save_output_dicts_to_run_dir()
            self.context.logger.info("<------------- Finished create_dict mode")
        finally:
            if self.context.options.save_dicts:
                save_dicts_info_file(self.context.options)

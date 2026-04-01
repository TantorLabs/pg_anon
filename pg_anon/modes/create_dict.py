import asyncio
import json
import multiprocessing
import multiprocessing.synchronize
import re
import shutil
import time
from pathlib import Path

from aioprocessing import AioQueue
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
from pg_anon.common.dto import FieldInfo
from pg_anon.common.enums import ScanMode
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.multiprocessing_utils import init_process
from pg_anon.common.utils import (
    chunkify,
    get_base_field_type,
    get_dict_rule_for_table,
    safe_compile,
    save_dicts_info_file,
    setof_to_list,
)
from pg_anon.context import Context


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
                if rule == field_info.column_name:
                    if obj_id in fields_info:
                        self.context.logger.debug(
                            '------> Field %s.%s.%s is SENSITIVE by rule "%s"',
                            field_info.nspname,
                            field_info.relname,
                            field_info.column_name,
                            rule,
                        )
                        del fields_info[obj_id]
                        self.context.create_dict_sens_matches[obj_id] = field_info
                    matched = True
                    break

            if matched:
                continue

            for rule in self.context.meta_dictionary_obj["field"]["rules"]:
                if re.search(rule, field_info.column_name) is not None:
                    if obj_id in fields_info:
                        self.context.logger.debug(
                            '------> Field %s.%s.%s is SENSITIVE by rule "%s"',
                            field_info.nspname,
                            field_info.relname,
                            field_info.column_name,
                            rule,
                        )
                        del fields_info[obj_id]
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

    def _check_data_by_constants(self, name: str, dictionary_obj: dict, field_info: FieldInfo, fld_data: list) -> bool:
        words = dictionary_obj["data_const"]["constants"]["words"]
        phrases = dictionary_obj["data_const"]["constants"]["phrases"]
        if not words and not phrases:
            return False

        self.context.logger.debug(
            "========> Process[%s]: checking by constants data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )

        for value in fld_data:
            if value is None:
                continue

            for word in value.split():
                if len(word) >= (self.context.data_const_constants_min_length or 0) and word.lower() in words:
                    self.context.logger.debug(
                        "========> Process[%s]: Field %s.%s.%s is SENSITIVE by constant %s",
                        name,
                        field_info.nspname,
                        field_info.relname,
                        field_info.column_name,
                        word,
                    )
                    return True

            for phrase in phrases:
                if phrase in value.lower():
                    self.context.logger.debug(
                        "========> Process[%s]: Field %s.%s.%s is SENSITIVE by constant %s",
                        name,
                        field_info.nspname,
                        field_info.relname,
                        field_info.column_name,
                        phrase,
                    )
                    return True

        self.context.logger.debug(
            "========> Process[%s]: No one constants matched in data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        return False

    def _check_data_by_partial_constants(
        self, name: str, dictionary_obj: dict, field_info: FieldInfo, fld_data: list
    ) -> bool:
        if not dictionary_obj["data_const"]["partial_constants"]:
            return False

        self.context.logger.debug(
            "========> Process[%s]: checking by partial constants data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )

        for value in fld_data:
            if value is None:
                continue

            lower_value = value.lower()
            for partial_constant in dictionary_obj["data_const"]["partial_constants"]:
                if partial_constant in lower_value:
                    self.context.logger.debug(
                        "========> Process[%s]: Field %s.%s.%s is SENSITIVE by partial constant %s",
                        name,
                        field_info.nspname,
                        field_info.relname,
                        field_info.column_name,
                        partial_constant,
                    )
                    return True

        self.context.logger.debug(
            "========> Process[%s]: No one partial constants matched in data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        return False

    async def _check_data_by_functions(  # noqa: C901, PLR0912
        self, connection: Connection, name: str, dictionary_obj: dict, field_info: FieldInfo, fld_data: list
    ) -> bool:
        if not dictionary_obj["data_func"]:
            return False

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
            return False

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
                field_info.rule = rule["anon_func"]
                self.context.logger.debug(
                    "========> Process[%s]: Field %s.%s.%s is SENSITIVE by data scan func per field %s",
                    name,
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    rule["scan_func_per_field"],
                )
                return True

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
                        field_info.rule = rule["anon_func"]
                        self.context.logger.debug(
                            "========> Process[%s]: Field %s.%s.%s is SENSITIVE by data scan func %s",
                            name,
                            field_info.nspname,
                            field_info.relname,
                            field_info.column_name,
                            rule["scan_func"],
                        )
                        return True

        self.context.logger.debug(
            "========> Process[%s]: No one data functions found sensitive data in field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )

        return False

    def _check_data_by_regexp(
        self, name: str, dictionary_obj: dict, create_dict_matches: dict, field_info: FieldInfo, fld_data: list
    ) -> bool:
        if field_info.obj_id in create_dict_matches:
            return False

        self.context.logger.debug(
            "========> Process[%s]: checking by regexp data of field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )

        if dictionary_obj["data_regex"]["rules"]:
            for value in fld_data:
                for rule in dictionary_obj["data_regex"]["rules"]:
                    if value is not None and re.search(rule, value) is not None:
                        self.context.logger.debug(
                            "========> Process[%s]: Field %s.%s.%s is SENSITIVE by data_regex (regex=%s; value=%s)",
                            name,
                            field_info.nspname,
                            field_info.relname,
                            field_info.column_name,
                            rule,
                            value,
                        )
                        return True

        self.context.logger.debug(
            "========> Process[%s]: No one regexp rules found sensitive data in field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        return False

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
        result = {field_info.obj_id: field_info}
        matched = False

        if not matched and await self._check_data_by_functions(
            connection=connection,
            name=name,
            dictionary_obj=dictionary_obj,
            field_info=field_info,
            fld_data=fld_data,
        ):
            matched = True

        if self._check_data_by_constants(
            name=name,
            dictionary_obj=dictionary_obj,
            field_info=field_info,
            fld_data=fld_data,
        ):
            matched = True

        if not matched and self._check_data_by_partial_constants(
            name=name,
            dictionary_obj=dictionary_obj,
            field_info=field_info,
            fld_data=fld_data,
        ):
            matched = True

        if not matched and self._check_data_by_regexp(
            name=name,
            dictionary_obj=dictionary_obj,
            create_dict_matches=create_dict_matches,
            field_info=field_info,
            fld_data=fld_data,
        ):
            matched = True

        if matched:
            self.context.logger.debug(
                "<--- Process[%s]: Finished check sensitive data in field %s.%s.%s - is SENSITIVE",
                name,
                field_info.nspname,
                field_info.relname,
                field_info.column_name,
            )
            return result

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

    async def _scan_obj_func(
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
                    fld_data = await db_conn.fetch(query)
                    res = await self._check_sensitive_data_in_fld(
                        connection=db_conn,
                        name=name,
                        dictionary_obj=dictionary_obj,
                        create_dict_matches=self.context.create_dict_sens_matches,
                        field_info=field_info,
                        fld_data=setof_to_list(fld_data),
                    )
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

    def _process_create_dict(  # noqa: C901, PLR0915
        self,
        name: str,
        queue: AioQueue,
        fields_info_chunk: list[FieldInfo],
        stop_event: multiprocessing.synchronize.Event,
    ) -> None:
        tasks_res: list = []

        status_ratio = 10
        if len(fields_info_chunk) > 1000:  # noqa: PLR2004
            status_ratio = 100
        if len(fields_info_chunk) > 50000:  # noqa: PLR2004
            status_ratio = 1000

        def _should_stop() -> bool:
            return stop_event is not None and stop_event.is_set()

        async def _wait_and_check(tasks: set[asyncio.Task]) -> set[asyncio.Task] | None:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            if _should_stop():
                self.context.logger.info("Process [%s] received stop signal, terminating", name)
                return None

            for done_task in done:
                if exc := done_task.exception():
                    raise exc

            return pending

        async def _process_run() -> None:
            db_connections = self.context.options.db_connections_per_process
            pool = await create_pool(
                connection_params=self.context.connection_params,
                server_settings=self.context.server_settings,
                min_size=db_connections,
                max_size=db_connections,
            )
            tasks: set[asyncio.Task] = set()

            self.context.logger.info("============> Started collecting list_tagged_fields in mode: create-dict")
            self.context.logger.info("<============ Finished collecting list_tagged_fields in mode: create-dict")

            try:
                for idx, field_info in enumerate(fields_info_chunk):
                    if _should_stop():
                        self.context.logger.info("Process [%s] received stop signal, terminating", name)
                        return

                    while len(tasks) >= self.context.options.db_connections_per_process:
                        result = await _wait_and_check(tasks)
                        if result is None:
                            return
                        tasks = result

                    task_res = loop.create_task(
                        self._scan_obj_func(
                            name,
                            pool,
                            field_info,
                            self.context.options.scan_mode,
                            self.context.meta_dictionary_obj,
                            self.context.options.scan_partial_rows,
                        )
                    )
                    tasks_res.append(task_res)
                    tasks.add(task_res)

                    if idx % status_ratio == 0:
                        progress_percents = round(float(idx) * 100 / len(fields_info_chunk), 2)
                        self.context.logger.info("Process [%s] Progress %d%%", name, progress_percents)

                while tasks:
                    remaining = await _wait_and_check(tasks)
                    if remaining is None:
                        return
                    tasks = remaining
            finally:
                await pool.close()

        loop = asyncio.new_event_loop()

        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_process_run())

            tasks_res_final = [r for task in tasks_res if (r := task.result()) is not None and len(r) > 0]

            queue.put(tasks_res_final)
        except Exception as ex:
            self.context.logger.exception("================> Process [%s]", name)
            queue.put([ex])
        finally:
            self.context.logger.debug("================> Process [%s] closing", name)
            loop.close()
            queue.put(None)  # Shut down the worker
            queue.close()
            self.context.logger.debug("================> Process [%s] closed", name)

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

        if hash_func.find("%s") != -1:
            hash_func = hash_func % field_info.column_name

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

        self._scan_fields_by_names(
            fields_info
        )  # fill self.context.create_dict_sens_matches and self.context.create_dict_no_sens_matches

        # create output dict
        prepared_sens_dict_rules: dict = {}
        need_prepare_no_sens_dict: bool = bool(self.context.options.output_no_sens_dict_file)

        if fields_info:
            fields_info_chunks = list(chunkify(list(fields_info.values()), self.context.options.processes))

            # Shared event to signal all processes to stop on error
            stop_event = multiprocessing.Event()

            tasks = []
            for idx, fields_info_chunk in enumerate(fields_info_chunks):
                tasks.append(
                    asyncio.ensure_future(
                        init_process(
                            name=str(idx + 1),
                            ctx=self.context,
                            target_func=self._process_create_dict,
                            tasks=fields_info_chunk,
                            stop_event=stop_event,
                        )
                    )
                )

            # Wait with immediate error detection
            completed_tasks: list = []
            remaining = tasks
            while remaining:
                done, pending = await asyncio.wait(remaining, return_when=asyncio.FIRST_EXCEPTION)

                for task in done:
                    exc = task.exception()
                    if exc:
                        # Signal all processes to stop
                        stop_event.set()
                        # Wait for remaining processes to finish (with timeout)
                        if pending:
                            await asyncio.wait(pending, timeout=10)
                        raise exc

                completed_tasks.extend(done)
                remaining = list(pending)

            # ============================================================================================
            # Fill results based on processes
            # ============================================================================================
            for v in completed_tasks:
                if v.result() is not None:
                    for res in v.result():
                        for field_info in res.values():
                            prepared_sens_dict_rules = self._prepare_sens_dict_rule(
                                self.context.meta_dictionary_obj, field_info, prepared_sens_dict_rules
                            )
                            if need_prepare_no_sens_dict:
                                del fields_info[field_info.obj_id]

        # ============================================================================================
        # Fill results based on check_sensitive_fld_names
        # ============================================================================================
        for field_info in self.context.create_dict_sens_matches.values():
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

            for field_info in self.context.create_dict_no_sens_matches.values():
                prepared_no_sens_dict_rules = self._prepare_no_sens_dict_rule(field_info, prepared_no_sens_dict_rules)

            for field_info in fields_info.values():
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
        connection = await create_connection(
            self.context.connection_params, server_settings=self.context.server_settings
        )
        try:
            required_connections = self.context.options.processes * self.context.options.db_connections_per_process
            await check_required_connections(connection, required_connections)
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

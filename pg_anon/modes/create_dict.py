import asyncio
import json
import os
import re
import time
from typing import List, Dict, Optional

from aioprocessing import AioQueue
from asyncpg import Connection

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME, DEFAULT_HASH_FUNC
from pg_anon.common.db_queries import get_data_from_field_query
from pg_anon.common.db_utils import get_scan_fields_list, exec_data_scan_func_query, create_pool
from pg_anon.common.dto import FieldInfo
from pg_anon.common.enums import ScanMode
from pg_anon.common.multiprocessing_utils import init_process
from pg_anon.common.utils import (
    chunkify,
    exception_helper,
    setof_to_list,
    get_dict_rule_for_table,
    normalize_field_type,
)
from pg_anon.context import Context

SENS_PG_TYPES = ["text", "integer", "bigint", "character", "json", "mvarchar"]


class CreateDictMode:
    def __init__(self, context: Context):
        self.context = context

    def _check_field_match_by_rule(self, field: Dict, rule: Dict) -> bool:
        schema_matched = False
        table_matched = False
        field_matched = False

        if "schema" in rule and field["nspname"] == rule["schema"]:
                schema_matched = True
        elif "schema_mask" in rule:
            if rule["schema_mask"] == "*":
                schema_matched = True
            elif re.search(rule["schema_mask"], field["nspname"]) is not None:
                schema_matched = True
        else:
            # Required schema or schema_mask
            return False

        if "table" not in rule:
            if "table_mask" not in rule:
                table_matched = True
            else:
                if rule["table_mask"] == "*":
                    table_matched = True
                elif re.search(rule["table_mask"], field["relname"]) is not None:
                    table_matched = True
        elif field["relname"] == rule["table"]:
            table_matched = True

        if "fields" not in rule:
            field_matched = True
        elif field["column_name"] in rule["fields"]:
            field_matched = True

        if schema_matched and table_matched and field_matched:
            return True

        return False

    def _check_not_skip_fields(self, field: Dict) -> bool:
        for rule in self.context.meta_dictionary_obj["skip_rules"]:
            if self._check_field_match_by_rule(field=field, rule=rule):
                self.context.logger.debug(f"!!! ------> Field {field['nspname']}.{field['relname']}.{field['column_name']} skipped by rule {rule}")
                return False

        return True

    def _check_include_fields(self, field: Dict) -> bool:
        if not self.context.meta_dictionary_obj["include_rules"]:
            return True

        for rule in self.context.meta_dictionary_obj["include_rules"]:
            if self._check_field_match_by_rule(field=field, rule=rule):
                self.context.logger.debug(f"!!! ------> Field {field['nspname']}.{field['relname']}.{field['column_name']} included by rule {rule}")
                return True

        return False

    async def _get_fields_for_scan(self) -> Dict[str, FieldInfo]:
        """
        Get scanning fields for create dictionary mode
        :return: dict of fields with key by obj_id for create dictionary mode
        """
        fields_list = await get_scan_fields_list(connection_params=self.context.connection_params, server_settings=self.context.server_settings)

        return {
            field['obj_id']: FieldInfo(**field) for field in fields_list
            if self._check_include_fields(field) and self._check_not_skip_fields(field)
        }

    def _prepare_meta_dictionary_obj(self):
        self.context.meta_dictionary_obj["data_const"]["constants"] = set(
            self.context.meta_dictionary_obj["data_const"]["constants"]
        )
        self.context.meta_dictionary_obj["data_const"]["partial_constants"] = set(
            self.context.meta_dictionary_obj["data_const"]["partial_constants"]
        )

        regex_for_compile = []
        for v in self.context.meta_dictionary_obj["data_regex"]["rules"]:
            # re.DOTALL using for searching in text with \n
            regex_for_compile.append(re.compile(v, re.DOTALL))

        self.context.meta_dictionary_obj["data_regex"]["rules"] = regex_for_compile.copy()

        regex_for_compile = []
        for v in self.context.meta_dictionary_obj["field"]["rules"]:
            regex_for_compile.append(re.compile(v))

        self.context.meta_dictionary_obj["field"]["rules"] = regex_for_compile.copy()

    def _scan_fields_by_names(self, fields_info: Dict[str, FieldInfo]):
        """
        Scanning fields by names and removes matches according to dict rules

        Priorities of rules:
            - prepared-sens-dict-file
            - meta-dict-file
            - prepared-no-sens-dict-file
        """

        for obj_id, field_info in fields_info.copy().items():
            matched: bool = False
            include_rule: Optional[Dict] = None
            exclude_rule: Optional[Dict] = None

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
            if include_rule and field_info.column_name in include_rule['fields']:
                self.context.logger.debug(
                    f'!!! ------> Field {field_info.nspname}.{field_info.relname}.{field_info.column_name} is SENSITIVE by rule "{include_rule}"'
                )
                del fields_info[obj_id]
                field_info.rule = include_rule['fields'][field_info.column_name]
                self.context.create_dict_sens_matches[obj_id] = field_info
                matched = True

            elif exclude_rule:
                self.context.logger.debug(
                    f'!!! ------> Field {field_info.nspname}.{field_info.relname}.{field_info.column_name} is INSENSITIVE by rule "{exclude_rule}"'
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
                            f'!!! ------> Field {field_info.nspname}.{field_info.relname}.{field_info.column_name} is SENSITIVE by rule "{rule}"'
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
                            f'!!! ------> Field {field_info.nspname}.{field_info.relname}.{field_info.column_name} is SENSITIVE by rule "{rule}"'
                        )
                        del fields_info[obj_id]
                        self.context.create_dict_sens_matches[obj_id] = field_info
                    matched = True
                    break

            if matched:
                continue

            for rule in self.context.meta_dictionary_obj["no_sens_dictionary"]:
                if (rule['schema'] == field_info.nspname and
                    rule['table'] == field_info.relname and
                    field_info.column_name in rule['fields']
                ):
                    if obj_id in fields_info:
                        self.context.logger.debug(
                            f'!!! ------> Field {field_info.nspname}.{field_info.relname}.{field_info.column_name} is INSENSITIVE by rule "{rule}"'
                        )
                        del fields_info[obj_id]
                        self.context.create_dict_no_sens_matches[obj_id] = field_info
                    break

    def _check_data_by_constants(
            self,
            name: str,
            dictionary_obj: Dict,
            field_info: FieldInfo,
            fld_data: List
    ) -> bool:
        if not dictionary_obj["data_const"]["constants"]:
            return False

        self.context.logger.debug(
            f'========> Process[{name}]: checking by constants data of field {field_info.nspname}.{field_info.relname}.{field_info.column_name}'
        )

        for value in fld_data:
            if value is None:
                continue

            for word in value.split():
                if len(word) >= 5 and word.lower() in dictionary_obj["data_const"]["constants"]:
                    self.context.logger.debug(
                        f'========> Process[{name}]: Field {field_info.nspname}.{field_info.relname}.{field_info.column_name} is SENSITIVE by constant {word}'
                    )
                    return True

        self.context.logger.debug(
            f'========> Process[{name}]: No one constants matched in data of field {field_info.nspname}.{field_info.relname}.{field_info.column_name}'
        )
        return False

    def _check_data_by_partial_constants(
            self,
            name: str,
            dictionary_obj: Dict,
            field_info: FieldInfo,
            fld_data: List
    ) -> bool:
        if not dictionary_obj["data_const"]["partial_constants"]:
            return False

        self.context.logger.debug(
            f'========> Process[{name}]: checking by partial constants data of field {field_info.nspname}.{field_info.relname}.{field_info.column_name}'
        )

        for value in fld_data:
            if value is None:
                continue

            lower_value = value.lower()
            for partial_constant in dictionary_obj["data_const"]["partial_constants"]:
                if partial_constant.lower() in lower_value:
                    self.context.logger.debug(
                        f'========> Process[{name}]: Field {field_info.nspname}.{field_info.relname}.{field_info.column_name} is SENSITIVE by partial constant {partial_constant}'
                    )
                    return True

        self.context.logger.debug(
            f'========> Process[{name}]: No one partial constants matched in data of field {field_info.nspname}.{field_info.relname}.{field_info.column_name}'
        )
        return False

    async def _check_data_by_functions(
            self,
            connection: Connection,
            name: str,
            dictionary_obj: Dict,
            field_info: FieldInfo,
            fld_data: List
    ) -> bool:
        if not dictionary_obj["data_func"]:
            return False

        self.context.logger.debug(
            f'========> Process[{name}]: checking by functions data of field {field_info.nspname}.{field_info.relname}.{field_info.column_name} (type = {field_info.type})'
        )

        field_type = normalize_field_type(field_info)
        rules_by_type = dictionary_obj["data_func"].get(field_type, [])
        rules_for_anyelements = dictionary_obj["data_func"].get('anyelement', [])
        data_func_rules = [rules_by_type, rules_for_anyelements]

        if not data_func_rules:
            self.context.logger.debug(
                f'========> Process[{name}]: Not found data checking functions for field {field_info.nspname}.{field_info.relname}.{field_info.column_name} (type = {field_info.type})'
            )
            return False

        for rules in data_func_rules:
            for rule in rules:
                matched_count = 0
                rule_expected_matches_count = rule.get("n_count", 1)

                for value in fld_data:
                    if value is None:
                        continue

                    if await exec_data_scan_func_query(
                        connection=connection,
                        scan_func=rule["scan_func"],
                        value=value,
                        field_info=field_info
                    ):
                        matched_count += 1

                        if matched_count == rule_expected_matches_count:
                            field_info.rule = rule["anon_func"]
                            self.context.logger.debug(
                                f'========> Process[{name}]: Field {field_info.nspname}.{field_info.relname}.{field_info.column_name} is SENSITIVE by data scan func {rule["scan_func"]}'
                            )
                            return True

        self.context.logger.debug(
            f'========> Process[{name}]: No one data functions found sensitive data in field {field_info.nspname}.{field_info.relname}.{field_info.column_name}'
        )

        return False

    def _check_data_by_regexp(
            self,
            name: str,
            dictionary_obj: Dict,
            create_dict_matches: List,
            field_info: FieldInfo,
            fld_data: List
    ) -> bool:
        if field_info.obj_id in create_dict_matches:
            return False

        self.context.logger.debug(
            f'========> Process[{name}]: checking by regexp data of field {field_info.nspname}.{field_info.relname}.{field_info.column_name}'
        )

        if dictionary_obj["data_regex"]["rules"]:
            for value in fld_data:
                for rule in dictionary_obj["data_regex"]["rules"]:
                    if value is not None and re.search(rule, value) is not None:
                        self.context.logger.debug(
                            f'========> Process[{name}]: Field {field_info.nspname}.{field_info.relname}.{field_info.column_name} is SENSITIVE by data_regex (regex={rule}; value={value})'
                        )
                        return True

        self.context.logger.debug(
            f'========> Process[{name}]: No one regexp rules found sensitive data in field {field_info.nspname}.{field_info.relname}.{field_info.column_name}'
        )
        return False

    async def _check_sensitive_data_in_fld(
            self,
            connection: Connection,
            name: str,
            dictionary_obj: Dict,
            create_dict_matches: List,
            field_info: FieldInfo,
            fld_data: List
    ) -> dict:
        if not fld_data:
            self.context.logger.debug(
                f"---> Process[{name}]: Check sensitive skipped cause field {field_info.nspname}.{field_info.relname}.{field_info.column_name} has not data"
            )
            return {}

        self.context.logger.debug(f"---> Process[{name}]: Started check sensitive data in field - {field_info.nspname}.{field_info.relname}.{field_info.column_name}. ({len(fld_data)} values)")
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
            self.context.logger.debug(f"<--- Process[{name}]: Finished check sensitive data in field {field_info.nspname}.{field_info.relname}.{field_info.column_name} - is SENSITIVE")
            return result

        self.context.logger.debug(f"<--- Process[{name}]: Finished check sensitive data in field {field_info.nspname}.{field_info.relname}.{field_info.column_name} - is INSENSITIVE")
        return {}

    def _check_sens_pg_types(self, dictionary_obj, field_type: str):
        """Check if actual field type is sens."""
        data_types = dictionary_obj.get("sens_pg_types", [])
        sens_types = data_types if data_types else SENS_PG_TYPES

        return any(pg_type in field_type for pg_type in sens_types)

    async def _scan_obj_func(
        self,
        name,
        pool,
        field_info: FieldInfo,
        scan_mode: ScanMode,
        dictionary_obj,
        scan_partial_rows,
    ):

        self.context.logger.debug(f"====>>> Process[{name}]: Started scan task for field {field_info.nspname}.{field_info.relname}.{field_info.column_name} ({field_info})")

        start_t = time.time()
        field_type = normalize_field_type(field_info)
        if not self._check_sens_pg_types(dictionary_obj, field_type):
            self.context.logger.debug(
                f"========> Process[%s]: scan_obj_func: task %s skipped by field type %s"
                % (name, str(field_info), "[integer, text, bigint, character varying(x)]")
            )
            return None

        res = {}
        condition = None
        if self.context.meta_dictionary_obj.get("data_sql_condition"):
            rule = get_dict_rule_for_table(
                dictionary_rules=self.context.meta_dictionary_obj["data_sql_condition"],
                schema=field_info.nspname,
                table=field_info.relname,
            )
            if rule:
                condition = rule.get('sql_condition')

        try:
            async with pool.acquire() as db_conn:
                if scan_mode == ScanMode.PARTIAL:
                    query = get_data_from_field_query(
                        field_info=field_info,
                        limit=scan_partial_rows,
                        condition=condition
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
                    async with db_conn.transaction(isolation='repeatable_read', readonly=True):
                        query = get_data_from_field_query(
                            field_info=field_info,
                            condition=condition
                        )
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

        except Exception as e:
            self.context.logger.error("Exception in scan_obj_func:\n" + exception_helper())
            raise Exception("Can't execute task: %s" % field_info)

        end_t = time.time()
        if end_t - start_t > 10:
            self.context.logger.debug(
                "!!! Process[%s]: scan_obj_func took %s sec. Task %s"
                % (name, str(round(end_t - start_t, 2)), str(field_info))
            )

        self.context.logger.debug(
            "<<<<==== Process[%s]: Found %s items(s) Finished task %s "
            % (name, str(len(res)), str(field_info))
        )
        return res

    def _process_create_dict(self, name: str, queue: AioQueue, fields_info_chunk: List[FieldInfo]):
        tasks_res = []

        status_ratio = 10
        if len(fields_info_chunk) > 1000:
            status_ratio = 100
        if len(fields_info_chunk) > 50000:
            status_ratio = 1000

        async def _process_run():
            pool = await create_pool(
                connection_params=self.context.connection_params,
                server_settings=self.context.server_settings,
                min_size=self.context.args.db_connections_per_process,
                max_size=self.context.args.db_connections_per_process
            )
            tasks = set()

            self.context.logger.info(
                "============> Started collecting list_tagged_fields in mode: create-dict"
            )
            self.context.logger.info(
                "<============ Finished collecting list_tagged_fields in mode: create-dict"
            )

            for idx, field_info in enumerate(fields_info_chunk):
                if len(tasks) >= self.context.args.db_connections_per_process:
                    done, tasks = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    exception = done.pop().exception()
                    if exception is not None:
                        await pool.close()
                        raise exception
                task_res = loop.create_task(
                    self._scan_obj_func(
                        name,
                        pool,
                        field_info,
                        self.context.args.scan_mode,
                        self.context.meta_dictionary_obj,
                        self.context.args.scan_partial_rows,
                    )
                )
                tasks_res.append(task_res)
                tasks.add(task_res)
                if idx % status_ratio:
                    progress_percents = round(float(idx) * 100 / len(fields_info_chunk), 2)
                    self.context.logger.info(f"Process [{name}] Progress {progress_percents}%")
            if len(tasks) > 0:
                await asyncio.wait(tasks)
            await pool.close()

        loop = asyncio.new_event_loop()

        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_process_run())

            tasks_res_final = []
            for task in tasks_res:
                if task.result() is not None and len(task.result()) > 0:
                    tasks_res_final.append(task.result())

            queue.put(tasks_res_final)
        except asyncio.exceptions.TimeoutError:
            self.context.logger.error(f"================> Process [{name}]: asyncio.exceptions.TimeoutError")
        except Exception as ex:
            self.context.logger.error(f"================> Process [{name}]: {ex}")
            raise ex
        finally:
            self.context.logger.debug(f"================> Process [{name}] closing")
            loop.close()
            queue.put(None)  # Shut down the worker
            queue.close()
            self.context.logger.debug(f"================> Process [{name}] closed")

    def _prepare_sens_dict_rule(self, meta_dictionary_obj: dict, field_info: FieldInfo, prepared_sens_dict_rules: dict):
        res_hash_func = field_info.rule

        if res_hash_func is None:
            field_type = normalize_field_type(field_info)
            hash_func = meta_dictionary_obj["funcs"].get(field_type, DEFAULT_HASH_FUNC)
            res_hash_func = hash_func if hash_func.find("%s") == -1 else hash_func % field_info.column_name

        if field_info.tbl_id not in prepared_sens_dict_rules:
            prepared_sens_dict_rules[field_info.tbl_id] = {
                "schema": field_info.nspname,
                "table": field_info.relname,
                "fields": {field_info.column_name: res_hash_func},
            }
        else:
            prepared_sens_dict_rules[field_info.tbl_id]["fields"].update(
                {field_info.column_name: res_hash_func}
            )
        return prepared_sens_dict_rules

    def _prepare_no_sens_dict_rule(self, field_info: FieldInfo, prepared_no_sens_dict_rules: dict):
        if field_info.tbl_id not in prepared_no_sens_dict_rules:
            prepared_no_sens_dict_rules[field_info.tbl_id] = {
                "schema": field_info.nspname,
                "table": field_info.relname,
                "fields": [field_info.column_name],
            }
        else:
            prepared_no_sens_dict_rules[field_info.tbl_id]["fields"].append(field_info.column_name)
        return prepared_no_sens_dict_rules

    async def _create_dict(self):
        fields_info: Dict[str, FieldInfo] = await self._get_fields_for_scan()
        if not fields_info:
            raise Exception("No objects for create dictionary!")

        self._scan_fields_by_names(fields_info)  # fill self.context.create_dict_sens_matches and self.context.create_dict_no_sens_matches

        # create output dict
        prepared_sens_dict_rules = {}
        need_prepare_no_sens_dict: bool = bool(self.context.args.output_no_sens_dict_file)

        if fields_info:
            fields_info_chunks = list(chunkify(list(fields_info.values()), self.context.args.processes))

            tasks = []
            for idx, fields_info_chunk in enumerate(fields_info_chunks):
                tasks.append(
                    asyncio.ensure_future(
                        init_process(
                            name=str(idx + 1),
                            ctx=self.context,
                            target_func=self._process_create_dict,
                            tasks=fields_info_chunk
                        )
                    )
                )
            await asyncio.wait(tasks)

            # ============================================================================================
            # Fill results based on processes
            # ============================================================================================
            for v in tasks:
                if v.result() is not None:
                    for res in v.result():
                        if isinstance(res, Exception):
                            raise res

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

        output_sens_dict_filename = os.path.join(self.context.current_dir, "dict", self.context.args.output_sens_dict_file)
        output_dir = os.path.dirname(output_sens_dict_filename)
        os.makedirs(output_dir, exist_ok=True)

        with open(output_sens_dict_filename, "w", encoding='utf-8') as file:
            file.write(json.dumps(output_sens_dict, indent=4, ensure_ascii=False))

        if need_prepare_no_sens_dict:
            prepared_no_sens_dict_rules = {}

            for field_info in self.context.create_dict_no_sens_matches.values():
                prepared_no_sens_dict_rules = self._prepare_no_sens_dict_rule(
                    field_info, prepared_no_sens_dict_rules
                )

            for field_info in fields_info.values():
                prepared_no_sens_dict_rules = self._prepare_no_sens_dict_rule(
                    field_info, prepared_no_sens_dict_rules
                )

            output_no_sens_dict = {"no_sens_dictionary": list(prepared_no_sens_dict_rules.values())}
            output_no_sens_dict_file_name = os.path.join(self.context.current_dir, "dict", self.context.args.output_no_sens_dict_file)
            with open(output_no_sens_dict_file_name, "w", encoding='utf-8') as file:
                file.write(json.dumps(output_no_sens_dict, indent=4, ensure_ascii=False))

    async def run(self) -> None:
        self.context.logger.info("-------------> Started create_dict mode")

        try:
            self.context.read_meta_dict()
            self._prepare_meta_dictionary_obj()
            if self.context.args.prepared_sens_dict_files:
                self.context.read_prepared_dict()
            await self._create_dict()

            self.context.logger.info("<------------- Finished create_dict mode")
        except Exception as ex:
            self.context.logger.error("<------------- create_dict failed\n" + exception_helper())
            raise ex

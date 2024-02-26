import asyncio
import itertools
import json
import os

import re
from logging import getLogger
from typing import List, Optional, Any, Dict
from multiprocessing import Pool
import asyncpg

from pg_anon.common import (
    PgAnonResult,
    ResultCode,
    ScanMode,
    exception_helper,
    setof_to_list,
)


logger = getLogger(__name__)

SENS_PG_TYPES = ["text", "integer", "bigint", "character", "json"]


class TaggedFields:
    def __init__(
        self, nspname: str, relname: str, column_name: str, column_comment: str
    ):
        self.nspname = nspname
        self.relname = relname
        self.column_name = column_name
        self.column_comment = column_comment


class FieldInfo:
    query: Optional[str] = None
    sensitive: Optional[bool] = None
    # Todo: save row data as set() it may be faster, at least will remove duplicated records
    row_data: List[Any] = None

    def __init__(
        self,
        nspname: str,
        relname: str,
        column_name: str,
        type: str,
        oid: int,
        attnum: int,
        obj_id: str,
        tbl_id: str,
        tagged_fields: Optional[List[TaggedFields]] = None,
    ):
        self.nspname = nspname
        self.relname = relname
        self.column_name = column_name
        self.type = type
        self.oid = oid
        self.attnum = attnum
        self.obj_id = obj_id
        self.tbl_id = tbl_id
        self.tagged_fields = tagged_fields

    def __str__(self):
        return (
            f"nspname={self.nspname}, "
            f"relname={self.relname}, "
            f"column_name={self.column_name}, "
            f"type={self.type}, oid={self.oid}, "
            f"attnum={self.attnum}, "
            f"obj_id={self.obj_id}, "
            f"tbl_id={self.tbl_id}"
        )

    @property
    def field_comment(self) -> Optional[str]:
        for field in self.tagged_fields:
            if (
                field.nspname == self.nspname
                and field.relname == self.relname
                and field.column_name == self.column_name
            ):
                if ":sens" in field.column_comment:
                    self.sensitive = True
                    return ":sens"
                elif ":nosens" in field.column_comment:
                    self.sensitive = False
                    return ":nosens"
        return

    def create_query(self, scan_mode: ScanMode, scan_partial_rows: int):
        query = (
            f'select distinct("{self.column_name}")::text from "{self.nspname}"."{self.relname}" '
            f'WHERE "{self.column_name}" is not null'
        )

        if scan_mode == ScanMode.PARTIAL:
            query = f"{query} LIMIT {str(scan_partial_rows)}"
        self.query = query

    def check_sens_pg_types(self):
        """Check if actual field type is sens."""
        for pg_type in SENS_PG_TYPES:
            if pg_type in self.type:
                return True
        return False

    async def get_row(self, pool):
        async with pool.acquire() as db_conn:
            if self.query:
                row_data = await db_conn.fetch(self.query)
                self.row_data = setof_to_list(row_data)


class SensFieldScan:
    result: PgAnonResult = PgAnonResult()
    dictionary_obj: dict = dict()
    create_dict_matches: dict = dict()
    tagged_fields: List[TaggedFields] = []
    sens_fields: dict = dict()

    def __init__(
        self,
        conn_params: Dict[str, str],
        processes: int,
        threads: int,
        output_dict_file: dict,
        current_dir: str,
        scan_mode: ScanMode,
        scan_partial_rows: bool,
        dict_file_name: str,
    ):
        self.result.result_code = ResultCode.DONE
        self.conn_params = conn_params
        self.processes = processes
        self.threads = threads
        self.output_dict_file = output_dict_file
        self.current_dir = current_dir
        self.scan_mode = scan_mode
        self.scan_partial_rows = scan_partial_rows
        self.dict_file_name = dict_file_name

    async def _get_tagged_fields(self, pool):
        """Get fields tagged :sens and :nosens."""
        query = """
        SELECT
            nspname AS schema_name,
            relname AS table_name,
            attname AS column_name,
            description AS column_comment
        FROM
            pg_description
            JOIN pg_attribute ON pg_description.objoid = pg_attribute.attrelid
                               AND pg_description.objsubid = pg_attribute.attnum
            JOIN pg_class ON pg_class.oid = pg_attribute.attrelid
            JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE
            pg_class.relkind = 'r' AND pg_attribute.attnum > 0 AND NOT pg_attribute.attisdropped
            and (description like '%:sens%' OR description like '%:nosens%')
        ORDER BY
            nspname,
            relname,
            attname;
        """
        db_conn = await pool.acquire()
        try:
            query_res = await db_conn.fetch(query)
        finally:
            await db_conn.close()
        self.tagged_fields = [
            TaggedFields(
                nspname=record["schema_name"],
                relname=record["table_name"],
                column_name=record["column_name"],
                column_comment=record["column_comment"],
            )
            for record in query_res
        ]
        # await db_conn.close()
        # await pool.release(db_conn)

    def check_skip_fields(self, fld):
        if "skip_rules" not in self.dictionary_obj:
            return True
        for v in self.dictionary_obj["skip_rules"]:
            schema_match = False
            tbl_match = False
            fld_match = False
            res = True
            if "schema" in v and fld["nspname"] == v["schema"]:
                schema_match = True
            if "table" in v and fld["relname"] == v["table"]:
                tbl_match = True
            if "fields" in v and fld["column_name"] in v["fields"]:
                fld_match = True
            if schema_match and tbl_match and fld_match:
                res = False

            if "fields" not in v and schema_match and tbl_match:
                res = False

            if "table" not in v and "fields" not in v and schema_match:
                res = False

            if not res:
                logger.debug(
                    "!!! ------> check_skip_fields: filtered fld %s by rule %s"
                    % (str(dict(fld)), str(v))
                )
                return res

        return True

    def _check_tagged_fields(self, fields_info: List[FieldInfo]) -> List[FieldInfo]:
        for field_info, tagged_field in itertools.product(
            fields_info, self.tagged_fields
        ):
            print(field_info, tagged_field)
            if (
                tagged_field.nspname == field_info.nspname
                and tagged_field.relname == field_info.relname
                and tagged_field.column_name == field_info.column_name
            ):
                if ":sens" in tagged_field.column_comment:
                    self.sens_fields[field_info.obj_id] = field_info
                    logger.info(f"Adding tagged :sens field: {str(field_info)}")
                fields_info.remove(field_info)
        return fields_info

    async def _generate_scan_objs(self):
        query = """
        -- generate task queue
        SELECT 
            n.nspname,
            c.relname,
            a.attname AS column_name,
            format_type(a.atttypid, a.atttypmod) as type,
            -- a.*
            c.oid, a.attnum,
            anon_funcs.digest(n.nspname || '.' || c.relname || '.' || a.attname, '', 'md5') as obj_id,
            anon_funcs.digest(n.nspname || '.' || c.relname, '', 'md5') as tbl_id
        FROM pg_class c
        JOIN pg_namespace n on c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid
        JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE
            a.attnum > 0
            AND c.relkind IN ('r', 'p')
            AND a.atttypid = t.oid
            AND n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
            AND coalesce(i.indisprimary, false) = false
            AND row(c.oid, a.attnum) not in (
                SELECT
                    t.oid,
                    a.attnum --,
                    -- pn_t.nspname,
                    -- t.relname AS table_name,
                    -- a.attname AS column_name
                FROM pg_class AS t
                JOIN pg_attribute AS a ON a.attrelid = t.oid
                JOIN pg_depend AS d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
                JOIN pg_class AS s ON s.oid = d.objid
                JOIN pg_namespace AS pn_t ON pn_t.oid = t.relnamespace
                WHERE
                    t.relkind IN ('r', 'p')
                    AND s.relkind = 'S'
                    AND d.deptype = 'a'
                    AND d.classid = 'pg_catalog.pg_class'::regclass
                    AND d.refclassid = 'pg_catalog.pg_class'::regclass
            )
            -- AND c.relname = 'card_numbers'  -- debug
        ORDER BY 1, 2, a.attnum
        """
        pool = await asyncpg.create_pool(
            **self.conn_params,
            min_size=self.threads,
            max_size=self.threads,
        )
        db_conn = await pool.acquire()
        try:
            query_res = await db_conn.fetch(query)
        finally:
            await db_conn.close()
        # Tagged Fields with :sense tag
        await self._get_tagged_fields(pool)
        await pool.close()
        sens_fields = [
            FieldInfo(**fld) for fld in query_res if self.check_skip_fields(fld=fld)
        ]
        sens_fields = [
            sens_field for sens_field in sens_fields if sens_field.check_sens_pg_types()
        ]

        sens_fields = self._check_tagged_fields(sens_fields)

        return sens_fields

    def prepare_dictionary_obj(self):
        self.dictionary_obj["data_const"]["constants"] = set(
            self.dictionary_obj["data_const"]["constants"]
        )

        regex_for_compile = []
        for v in self.dictionary_obj["data_regex"]["rules"]:
            regex_for_compile.append(re.compile(v))

        self.dictionary_obj["data_regex"]["rules"] = regex_for_compile.copy()

        regex_for_compile = []
        for v in self.dictionary_obj["field"]["rules"]:
            regex_for_compile.append(re.compile(v))

        self.dictionary_obj["field"]["rules"] = regex_for_compile.copy()

    async def _check_sensitive_fld_names(self, fields_info: List[FieldInfo]):
        for field_info in fields_info:
            if "rules" in self.dictionary_obj["field"]:
                for rule in self.dictionary_obj["field"]["rules"]:
                    if re.search(rule, field_info.column_name) is not None:
                        logger.debug(
                            '!!! ------> check_sensitive_fld_names: match by "%s", removed %s'
                            % (str(rule), str(field_info))
                        )
                        # objs.remove(v)
                        self.create_dict_matches[field_info.obj_id] = field_info

            if "constants" in self.dictionary_obj["field"]:
                for rule in self.dictionary_obj["field"]["constants"]:
                    if rule == field_info.column_name:
                        logger.debug(
                            '!!! ------> check_sensitive_fld_names: match by "%s", removed %s'
                            % (str(rule), str(field_info))
                        )
                        fields_info.remove(field_info)
                        self.create_dict_matches[field_info.obj_id] = field_info

    def _check_sensitive_data_in_fld(self, field_info: FieldInfo) -> dict:
        fld_data = field_info.row_data
        if field_info.relname == "card_numbers":
            x = 1
        fld_data_set = set()
        dict_matches = {}
        for v in fld_data:
            if v is None:
                continue
            for word in v.split():
                if len(word) >= 5:
                    fld_data_set.add(word.lower())

        result = set.intersection(
            self.dictionary_obj["data_const"]["constants"], fld_data_set
        )
        if len(result) > 0:
            logger.debug(
                "========> Process: check_sensitive_data: match by constant %s , %s"
                % (str(result), str(field_info))
            )
            dict_matches[field_info.obj_id] = field_info

        for v in fld_data:
            if (
                field_info.obj_id not in dict_matches
                and field_info.obj_id not in self.create_dict_matches
            ):
                for r in self.dictionary_obj["data_regex"]["rules"]:
                    if v is not None and re.search(r, v) is not None:
                        logger.debug(
                            '========> Process: check_sensitive_data: match by "%s", %s, %s'
                            % (str(r), str(v), str(field_info))
                        )
                        dict_matches[field_info.obj_id] = field_info
            else:
                break

        return dict_matches

    def _add_metadict_rule(self, field_info: FieldInfo, anon_rules: dict):
        hash_func = "anon_funcs.digest(\"%s\", 'salt_word', 'md5')"  # by default use md5 with salt

        for fld_type, func in self.dictionary_obj["funcs"].items():
            if str(field_info.type).find(fld_type) > -1:
                hash_func = func

        res_hash_func = (
            hash_func
            if hash_func.find("%s") == -1
            else hash_func % field_info.column_name
        )

        if field_info.tbl_id not in anon_rules:
            anon_rules[field_info.tbl_id] = {
                "schema": field_info.nspname,
                "table": field_info.relname,
                "fields": {field_info.column_name: res_hash_func},
            }
        else:
            anon_rules[field_info.tbl_id]["fields"].update(
                {field_info.column_name: res_hash_func}
            )
        return anon_rules

    def _create_output_dict(self):

        # create output dict
        output_dict = dict()
        output_dict["dictionary"] = []
        anon_dict_rules = {}

        # Fill results based on processes

        for field_info in self.sens_fields.values():
            anon_dict_rules = self._add_metadict_rule(field_info, anon_dict_rules)

        # Fill results based on check_sensitive_fld_names
        for field_info in self.create_dict_matches.values():
            anon_dict_rules = self._add_metadict_rule(field_info, anon_dict_rules)

        for _, v in anon_dict_rules.items():
            output_dict["dictionary"].append(v)

        with open(
            os.path.join(self.current_dir, "dict", self.output_dict_file), "w"
        ) as output_dict_file:

            output_dict_file.write(json.dumps(output_dict, indent=4))

    async def _get_row_data(self, fields_info: List[FieldInfo]):
        tasks = []
        pool = await asyncpg.create_pool(
            **self.conn_params,
            min_size=self.threads,
            max_size=self.threads,
        )

        for field_info in fields_info:
            tasks.append(asyncio.ensure_future(field_info.get_row(pool)))
        await asyncio.gather(*tasks)
        await pool.close()

    def _check_sens_data(self, fields_info):
        if self.processes > 1:
            with Pool(processes=self.processes) as pool:
                scan_results = pool.starmap(
                    self._check_sensitive_data_in_fld,
                    ((field_info,) for field_info in fields_info),
                )
            scan_results = [scan_result for scan_result in scan_results if scan_result]
            return scan_results

        scan_results = []
        for field_info in fields_info:
            scan_results.append(
                self._check_sensitive_data_in_fld(
                    field_info,
                )
            )
        scan_results = [scan_result for scan_result in scan_results if scan_result]
        for scan_result in scan_results:
            self.sens_fields.update(scan_result)

    async def _create_dict_impl(self):

        fields_info: List[FieldInfo] = await self._generate_scan_objs()

        if not fields_info:
            raise Exception("No objects for create dictionary!")

        await self._check_sensitive_fld_names(fields_info=fields_info)

        for field_info in fields_info:
            field_info.create_query(
                scan_mode=self.scan_mode,
                scan_partial_rows=self.scan_partial_rows,
            )

        await self._get_row_data(fields_info=fields_info)

        fields_info = [field_info for field_info in fields_info if field_info.row_data]

        self._check_sens_data(fields_info=fields_info)

        self._create_output_dict()

    def _get_dict_from_file(self):
        with open(
            os.path.join(self.current_dir, "dict", self.dict_file_name), "r"
        ) as dictionary_file:
            dictionary_content = dictionary_file.read()
            self.dictionary_obj = eval(dictionary_content)
            self.prepare_dictionary_obj()

    async def create_dict(self):

        logger.info("-------------> Started create_dict mode")
        try:
            self._get_dict_from_file()
        except Exception as exc:
            logger.error(f"Failed to prepare dict_file: {exc}")
            self.result.result_code = ResultCode.FAIL
            return self.result

        try:
            await self._create_dict_impl()
        except:
            logger.error("<------------- create_dict failed\n" + exception_helper())
            self.result.result_code = ResultCode.FAIL
            return self.result

        if self.result.result_code == ResultCode.DONE:
            logger.info("<------------- Finished create_dict mode")
        return self.result

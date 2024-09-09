import json
from typing import List, Dict

import asyncpg
from prettytable import PrettyTable, SINGLE_BORDER

from pg_anon.common.db_utils import get_fields_list
from pg_anon.common.dto import PgAnonResult
from pg_anon.common.enums import ResultCode
from pg_anon.common.utils import exception_helper, get_dump_query, get_dict_rule_for_table
from pg_anon.context import Context


class ViewDataMode:
    context: Context
    _limit: int
    _offset: int
    _schema_name: str
    _table_name: str
    table_rule: Dict
    query: str
    field_names: List[str] = None
    fields: List[List] = None
    table: PrettyTable = None

    def __init__(self, context: Context):
        self.context = context
        self._limit = context.args.limit
        self._offset = context.args.offset
        self._schema_name = context.args.schema_name
        self._table_name = context.args.table_name
        self.field_names = []
        self.fields = []

    async def _get_fields_for_view(self) -> None:
        """
        Get field names and all fields for view-data mode
        """
        fields_list = await get_fields_list(
            connection_params=self.context.conn_params,
            server_settings=self.context.server_settings,
            table_schema=self._schema_name,
            table_name=self._table_name
        )
        for field in fields_list:
            field_name = field["column_name"]
            if field_name in self.table_rule["fields"]:
                self.field_names.append('* ' + field_name)
            else:
                self.field_names.append(field_name)

        db_conn = await asyncpg.connect(**self.context.conn_params)
        table_result = await db_conn.fetch(self.query)
        self.fields = [[record[field["column_name"]] for field in fields_list] for record in table_result]
        await db_conn.close()

    def _prepare_table(self) -> None:
        self.table = PrettyTable(self.field_names)
        self.table.set_style(SINGLE_BORDER)
        for row in self.fields:
            self.table.add_row(row)

    def _prepare_json(self) -> None:
        result = {field: [] for field in self.field_names}

        for field_values in self.fields:
            for field, value in zip(self.field_names, field_values):
                result[field].append(value)

        self.json = json.dumps(result, default=lambda x: str(x), ensure_ascii=False)

    async def _output_fields(self) -> None:

        await self._get_fields_for_view()
        if not self.field_names:
            raise ValueError("No field names for view!")
        if not self.fields:
            raise ValueError("Not found fields for view!")

        if self.context.args.json:
            self._prepare_json()
            print(self.json)
        else:
            self._prepare_table()
            print(self.table)

    async def run(self) -> PgAnonResult:
        result = PgAnonResult()
        result.result_code = ResultCode.DONE
        self.context.logger.info("-------------> Started view_data mode")

        try:
            if self._limit < 1:
                raise ValueError("Processing fields limit must be greater than zero!")
            if self._offset < 0:
                raise ValueError("Processing fields offset must be greater than zero or equals to zero!")
        except ValueError:
            self.context.logger.error("<------------- view_fields failed\n" + exception_helper())
            result.result_code = ResultCode.FAIL
            return result

        self.context.read_prepared_dict()

        self.table_rule = get_dict_rule_for_table(
            dictionary_rules=self.context.prepared_dictionary_obj["dictionary"],
            schema=self._schema_name,
            table=self._table_name,
        )

        files = {}
        included_objs = []
        excluded_objs = []

        try:
            query_without_limit = await get_dump_query(
                ctx=self.context,
                table_schema=self._schema_name,
                table_name=self._table_name,
                table_rule=self.table_rule,
                files=files,
                included_objs=included_objs,
                excluded_objs=excluded_objs
            )
            self.query = query_without_limit + f" LIMIT {self._limit} OFFSET {self._offset}"
        except:
            self.context.logger.error("<------------- view_fields failed\n" + exception_helper())
            result.result_code = ResultCode.FAIL
            return result

        try:
            await self._output_fields()
        except:
            self.context.logger.error("<------------- view_fields failed\n" + exception_helper())
            result.result_code = ResultCode.FAIL
            return result

        if result.result_code == ResultCode.DONE:
            self.context.logger.info("<------------- Finished view_fields mode")

        return result

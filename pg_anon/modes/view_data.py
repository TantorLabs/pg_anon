import json

from prettytable import PrettyTable, SINGLE_BORDER

from pg_anon.common.db_utils import create_connection, get_dump_query, get_fields_list, get_rows_count
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import get_dict_rule_for_table
from pg_anon.context import Context


class ViewDataMode:
    context: Context
    _limit: int
    _offset: int
    _schema_name: str
    _table_name: str
    table_rule: dict
    raw_field_names: list[str] = None
    field_names: list[str] = None
    rows_count: int = 0
    query: str
    data: list[list[str]] = None
    raw_query: str | None = None
    raw_data: list[list[str]] | None = None
    table: PrettyTable = None
    _need_raw_data: bool = False

    def __init__(self, context: Context, need_raw_data: bool = False):
        self.context = context
        self._limit = context.options.limit
        self._offset = context.options.offset
        self._schema_name = context.options.schema_name
        self._table_name = context.options.table_name
        self.field_names = []
        self.raw_field_names = []
        self.data = []
        self.raw_data = []
        self._need_raw_data = need_raw_data

    async def _get_fields_for_view(self) -> None:
        """Get field names and all fields for view-data mode"""
        fields_list = await get_fields_list(
            connection_params=self.context.connection_params,
            server_settings=self.context.server_settings,
            table_schema=self._schema_name,
            table_name=self._table_name,
        )

        if not fields_list:
            raise PgAnonError(
                ErrorCode.TABLE_NOT_FOUND, f'Table "{self._schema_name}.{self._table_name}" hasn\'t exists!'
            )

        for field in fields_list:
            field_name = field["column_name"]
            self.raw_field_names.append(field_name)

            if self.table_rule and field_name in self.table_rule["fields"]:
                self.field_names.append("* " + field_name)
            else:
                self.field_names.append(field_name)

    async def _get_data_for_view(self, query: str) -> list[list[str]]:
        db_conn = await create_connection(self.context.connection_params, server_settings=self.context.server_settings)
        table_result = await db_conn.fetch(query)
        await db_conn.close()

        return [[record[field_name] for field_name in self.raw_field_names] for record in table_result]

    async def get_rows_count(self):
        self.rows_count = await get_rows_count(
            connection_params=self.context.connection_params,
            server_settings=self.context.server_settings,
            schema_name=self._schema_name,
            table_name=self._table_name,
        )
        return self.rows_count

    def _prepare_table(self) -> None:
        self.table = PrettyTable(self.field_names)
        self.table.set_style(SINGLE_BORDER)
        for row in self.data:
            self.table.add_row(row)

    def _prepare_json(self) -> None:
        result = {field: [] for field in self.field_names}

        for field_values in self.data:
            for field, value in zip(self.field_names, field_values, strict=False):
                result[field].append(value)

        self.json = json.dumps(result, default=str, ensure_ascii=False)

    async def _output_fields(self) -> None:
        await self._get_fields_for_view()
        self.data = await self._get_data_for_view(self.query)

        if self._need_raw_data:
            self.raw_data = await self._get_data_for_view(self.raw_query)

        if self.context.options.json:
            self._prepare_json()
            print(self.json)
        else:
            self._prepare_table()
            print(self.table)

    async def _prepare_queries(self):

        query_without_limit = await get_dump_query(
            ctx=self.context,
            table_schema=self._schema_name,
            table_name=self._table_name,
            table_rule=self.table_rule,
            nulls_last=True,
        )
        if not query_without_limit:
            raise PgAnonError(ErrorCode.TABLE_EXCLUDED, f'Table "{self._schema_name}.{self._table_name}" excluded!')

        self.query = query_without_limit + f" LIMIT {self._limit} OFFSET {self._offset}"

        if self._need_raw_data:
            query_without_limit = await get_dump_query(
                ctx=self.context,
                table_schema=self._schema_name,
                table_name=self._table_name,
                table_rule=None,
                nulls_last=True,
            )
            self.raw_query = query_without_limit + f" LIMIT {self._limit} OFFSET {self._offset}"

    async def run(self) -> None:
        self.context.logger.info("-------------> Started view_data mode")

        if self._limit < 1:
            raise PgAnonError(ErrorCode.INVALID_LIMIT, "Processing fields limit must be greater than zero!")
        if self._offset < 0:
            raise PgAnonError(
                ErrorCode.INVALID_OFFSET, "Processing fields offset must be greater than zero or equals to zero!"
            )

        self.context.read_prepared_dict()
        self.table_rule = get_dict_rule_for_table(
            dictionary_rules=self.context.prepared_dictionary_obj["dictionary"],
            schema=self._schema_name,
            table=self._table_name,
        )

        await self._prepare_queries()
        await self._output_fields()

        self.context.logger.info("<------------- Finished view_fields mode")

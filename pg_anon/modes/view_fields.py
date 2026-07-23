import json

from prettytable import PrettyTable, SINGLE_BORDER

from pg_anon.common.db_utils import get_scan_fields_list
from pg_anon.common.dto import FieldInfo
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import get_dict_rule_for_table
from pg_anon.context import Context


class ViewFieldsMode:
    _large_output_hint_threshold: int = 1000

    def __init__(self, context: Context) -> None:
        self.context = context
        self._output_fields_limit: int | None = context.options.fields_count
        self._filter_dict_rule: dict | None = None
        self.fields: list[FieldInfo] | None = None
        self.table: PrettyTable | None = None
        self.json: str | None = None
        self.fields_cut_by_limits: bool = False
        self.empty_data_filler: str = "---"
        self._init_filter_dict_rule()

    def _init_filter_dict_rule(self) -> None:
        self._filter_dict_rule = {}
        has_schema: bool = False
        has_table: bool = False

        if self.context.options.schema_name:
            self._filter_dict_rule["schema"] = self.context.options.schema_name
            has_schema = True

        if self.context.options.schema_mask:
            self._filter_dict_rule["schema_mask"] = self.context.options.schema_mask
            has_schema = True

        if self.context.options.table_name:
            self._filter_dict_rule["table"] = self.context.options.table_name
            has_table = True

        if self.context.options.table_mask:
            self._filter_dict_rule["table_mask"] = self.context.options.table_mask
            has_table = True

        if has_schema and not has_table:
            self._filter_dict_rule["table_mask"] = "*"

        if not has_schema and has_table:
            self._filter_dict_rule["schema_mask"] = "*"

    def _check_by_filters(self, field: FieldInfo) -> bool:
        if self._filter_dict_rule is None:
            return False
        return bool(
            get_dict_rule_for_table(
                dictionary_rules=[self._filter_dict_rule],
                schema=field.nspname,
                table=field.relname,
            )
        )

    async def _get_fields_for_view(self) -> list[FieldInfo]:
        """Get scanning fields for view mode."""
        fields_list = await get_scan_fields_list(
            connection_params=self.context.connection_params,
            server_settings=self.context.server_settings,
        )

        result = []
        for field in fields_list:
            field_info = FieldInfo(**field)
            if not self._filter_dict_rule or self._check_by_filters(field_info):
                result.append(field_info)

        return result

    def _notice_large_output(self) -> None:
        if self.context.options.json:
            return
        if self._output_fields_limit is not None:
            return

        output_count = len(self.fields or [])
        if output_count <= self._large_output_hint_threshold:
            return

        print(
            f"{output_count} fields shown."
            f" Use --fields-count to limit the output, or --json for machine-readable output."
        )

    def _prepare_fields_for_view(self) -> None:
        fields_with_find_rules = []

        for field in (self.fields or []).copy():
            include_rule = get_dict_rule_for_table(
                dictionary_rules=self.context.prepared_dictionary_obj["dictionary"],
                schema=field.nspname,
                table=field.relname,
            )

            if "dictionary_exclude" in self.context.prepared_dictionary_obj:
                exclude_rule = get_dict_rule_for_table(
                    dictionary_rules=self.context.prepared_dictionary_obj["dictionary_exclude"],
                    schema=field.nspname,
                    table=field.relname,
                )

                if exclude_rule is not None and include_rule is None:
                    continue

            if include_rule:
                if field.column_name in include_rule.get("fields", {}):
                    field.rule = include_rule["fields"][field.column_name]
                    field.dict_file_name = include_rule["dict_file_name"]
                    fields_with_find_rules.append(field)
                    continue
                if include_rule.get("raw_sql"):
                    field.rule = include_rule["raw_sql"]
                    field.dict_file_name = include_rule["dict_file_name"]
                    fields_with_find_rules.append(field)
                    continue

            if not self.context.options.view_only_sensitive_fields:
                field.rule = self.empty_data_filler
                field.dict_file_name = self.empty_data_filler
                fields_with_find_rules.append(field)

        self.fields = fields_with_find_rules

    def _prepare_table(self) -> None:
        self.table = PrettyTable(
            [
                "schema",
                "table",
                "field",
                "type",
                "dict_file_name",
                "rule",
            ],
            align="l",
        )
        self.table.set_style(SINGLE_BORDER)

        for field in self.fields or []:
            self.table.add_row(
                [
                    field.nspname,
                    field.relname,
                    field.column_name,
                    field.type,
                    field.dict_file_name,
                    field.rule,
                ]
            )

    def _prepare_json(self) -> None:
        self.json = json.dumps(
            [
                {
                    "schema": field.nspname,
                    "table": field.relname,
                    "field": field.column_name,
                    "type": field.type,
                    "dict_file_name": field.dict_file_name,
                    "rule": field.rule,
                }
                for field in self.fields or []
            ],
            ensure_ascii=False,
        )

    def _apply_output_limit(self) -> None:
        if self._output_fields_limit is None or not self.fields:
            return

        if len(self.fields) > self._output_fields_limit:
            self.fields = self.fields[: self._output_fields_limit]
            self.fields_cut_by_limits = True

    async def _output_fields(self) -> None:
        self.fields = await self._get_fields_for_view()
        if self.fields:
            self._prepare_fields_for_view()

        self._apply_output_limit()

        if self.context.options.json:
            self._prepare_json()
            print(self.json)
        else:
            self._prepare_table()
            print(self.table)
            self._notice_large_output()

    async def run(self) -> None:
        """Run the view_fields mode to display table field details."""
        self.context.logger.info("-------------> Started view_fields mode")

        if self._output_fields_limit is not None and self._output_fields_limit < 1:
            raise PgAnonError(ErrorCode.INVALID_LIMIT, "Fields count must be greater than zero!")
        self.context.read_prepared_dict(save_dict_file_name_for_each_rule=True)
        await self._output_fields()

        self.context.logger.info("<------------- Finished view_fields mode")

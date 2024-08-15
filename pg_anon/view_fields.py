import json
from dataclasses import asdict
from typing import List, Dict

from prettytable import PrettyTable, SINGLE_BORDER

from pg_anon.common.db_utils import get_scan_fields_list, get_scan_fields_count
from pg_anon.common.dto import PgAnonResult, FieldInfo
from pg_anon.common.enums import ResultCode
from pg_anon.common.utils import exception_helper, get_dict_rule_for_table
from pg_anon.context import Context


class ViewFieldsMode:
    context: Context
    _processing_fields_limit: int = 5000
    _filter_dict_rule: Dict = None
    fields: List[FieldInfo] = None
    table: PrettyTable = None
    json: str = None
    fields_cut_by_limits: bool = False

    def __init__(self, context: Context):
        self.context = context
        if context.args.fields_count is not None:
            self._processing_fields_limit = context.args.fields_count
        self._init_filter_dict_rule()

    def _init_filter_dict_rule(self):
        self._filter_dict_rule = {}
        has_schema: bool = False
        has_table: bool = False

        if self.context.args.schema_name:
            self._filter_dict_rule["schema"] = self.context.args.schema_name
            has_schema = True

        if self.context.args.schema_mask:
            self._filter_dict_rule["schema_mask"] = self.context.args.schema_mask
            has_schema = True

        if self.context.args.table_name:
            self._filter_dict_rule["table"] = self.context.args.table_name
            has_table = True

        if self.context.args.table_mask:
            self._filter_dict_rule["table_mask"] = self.context.args.table_mask
            has_table = True

        if has_schema and not has_table:
            self._filter_dict_rule["table_mask"] = '*'

        if not has_schema and has_table:
            self._filter_dict_rule["schema_mask"] = '*'

    def _check_by_filters(self, field: FieldInfo) -> bool:
        return bool(get_dict_rule_for_table(
            dictionary_rules=[self._filter_dict_rule],
            schema=field.nspname,
            table=field.relname,
        ))

    async def _get_fields_for_view(self) -> List[FieldInfo]:
        """
        Get scanning fields for view mode
        :return: list of fields for view mode
        """
        fields_list = await get_scan_fields_list(self.context.conn_params, limit=self._processing_fields_limit)

        result = []
        for field in fields_list:
            field_info = FieldInfo(**field)
            if not self._filter_dict_rule or self._check_by_filters(field_info):
                result.append(field_info)

        return result

    async def _make_notice_fields_cut_by_limits(self):
        fields_count = await get_scan_fields_count(self.context.conn_params)
        if fields_count > self._processing_fields_limit and not self.context.args.json:
            print(f'You try to get too many fields ({fields_count} fields).'
                  f' Will processed for output only first {self._processing_fields_limit} fields.'
                  f' Use arguments --schema-name, --schema-mask, --table-name, --table-mask to reduce fields amount.'
                  f' Also you can use --fields-count to extend limit.')
            self.fields_cut_by_limits = True

    def _prepare_fields_for_view(self):
        fields_with_find_rules = []

        for field in self.fields.copy():
            include_rule = get_dict_rule_for_table(
                dictionary_rules=self.context.prepared_dictionary_obj["dictionary"],
                schema=field.nspname,
                table=field.relname,
            )

            if include_rule:
                if field.column_name in include_rule.get('fields', {}):
                    field.rule = include_rule['fields'][field.column_name]
                    field.dict_file_name = include_rule["dict_file_name"]
                    fields_with_find_rules.append(field)
                    continue
                elif include_rule.get('raw_sql'):
                    field.rule = include_rule['raw_sql']
                    field.dict_file_name = include_rule["dict_file_name"]
                    fields_with_find_rules.append(field)
                    continue

            if not self.context.args.view_only_sensitive_fields:
                field.rule = '---'
                field.dict_file_name = '---'
                fields_with_find_rules.append(field)

        self.fields = fields_with_find_rules

    def _prepare_table(self):
        self.table = PrettyTable([
            'schema',
            'table',
            'field',
            'type',
            'dict_file_name',
            'rule',
        ], align='l')
        self.table.set_style(SINGLE_BORDER)

        for field in self.fields:
            self.table.add_row([
                field.nspname,
                field.relname,
                field.column_name,
                field.type,
                field.dict_file_name,
                field.rule,
            ])

    def _prepare_json(self):
        self.json = json.dumps([asdict(field) for field in self.fields])

    async def _output_fields(self):
        await self._make_notice_fields_cut_by_limits()

        self.fields = await self._get_fields_for_view()
        if not self.fields:
            raise ValueError("Not found fields for view!")

        self._prepare_fields_for_view()

        if not self.fields:
            raise ValueError("Haven't fields for view!")

        if self.context.args.json:
            self._prepare_json()
            print(self.json)
        else:
            self._prepare_table()
            print(self.table)

    async def run(self):
        result = PgAnonResult()
        result.result_code = ResultCode.DONE
        self.context.logger.info("-------------> Started view_fields mode")

        try:
            if self._processing_fields_limit < 1:
                raise ValueError("Processing fields limit must be greater than zero!")
        except ValueError:
            self.context.logger.error("<------------- view_fields failed\n" + exception_helper())
            result.result_code = ResultCode.FAIL
            return result

        try:
            self.context.read_prepared_dict(save_dict_file_name_for_each_rule=True)
            if not self.context.prepared_dictionary_obj.get("dictionary"):
                raise ValueError("Prepared dictionary is empty!")
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

from typing import List, Dict

from pg_anon.common.db_utils import get_schemas, create_connection, get_tables_with_fields
from pg_anon.common.dto import PgAnonResult, ConnectionParams
from pg_anon.common.utils import read_dict_data, get_dict_rule_for_table
from rest_api.pydantic_models import PreviewSchemasRequest, PreviewSchemaTablesRequest, PreviewTableContent, PreviewFieldContent


class PreviewRunner:
    result: PgAnonResult = None

    @classmethod
    async def get_schemas(cls, request: PreviewSchemasRequest) -> List[str]:
        connection_params = ConnectionParams(
            host=request.db_connection_params.host,
            port=request.db_connection_params.port,
            database=request.db_connection_params.db_name,
            user=request.db_connection_params.user_login,
            password=request.db_connection_params.user_password,
        )

        db_conn = await create_connection(connection_params)
        try:
            schemas = await get_schemas(db_conn, request.schema_filter)
        finally:
            await db_conn.close()

        return schemas

    @staticmethod
    def _prepare_sens_dicts(sens_dict_contents) -> Dict[str, List[Dict[str, str]]]:
        sens_dict_data = {
            "dictionary": [],
            "dictionary_exclude": [],
            "validate_tables": [],
        }

        for dict_contents in sens_dict_contents:
            if data := read_dict_data(dict_contents.content, dict_contents.name):
                sens_dict_data["dictionary"].extend(data.get("dictionary", []))
                sens_dict_data["dictionary_exclude"].extend(data.get("dictionary_exclude", []))
                sens_dict_data["validate_tables"].extend(data.get("validate_tables", []))

        return sens_dict_data

    @classmethod
    async def get_schema_tables(cls, schema: str, request: PreviewSchemaTablesRequest) -> List[PreviewTableContent]:
        sens_dict_data = cls._prepare_sens_dicts(request.sens_dict_contents)

        connection_params = ConnectionParams(
            host=request.db_connection_params.host,
            port=request.db_connection_params.port,
            database=request.db_connection_params.db_name,
            user=request.db_connection_params.user_login,
            password=request.db_connection_params.user_password,
        )
        data = await get_tables_with_fields(
            schema=schema,
            connection_params=connection_params,
            limit=request.limit,
            offset=request.offset,
            table_filter=request.table_filter,
        )

        table_fields = {}
        for table_name, field_name, field_type in data:
            if table_name not in table_fields:
                table_fields[table_name] = []
            table_fields[table_name].append((field_name, field_type))

        result = []
        for table_name, fields in table_fields.items():
            include_rule = get_dict_rule_for_table(
                dictionary_rules=sens_dict_data["dictionary"],
                schema=schema,
                table=table_name,
            ) if sens_dict_data["dictionary"] else None

            exclude_rule = get_dict_rule_for_table(
                dictionary_rules=sens_dict_data["dictionary_exclude"],
                schema=schema,
                table=table_name,
            ) if sens_dict_data["dictionary_exclude"] else None

            is_excluded = exclude_rule is not None and include_rule is None
            is_sensitive = include_rule is not None

            if request.view_only_sensitive_tables and not is_sensitive:
                continue

            preview_fields = []
            for field_name, field_type in fields:
                field_is_sensitive = False
                field_rule = None

                if include_rule:
                    if field_name in include_rule.get('fields', {}):
                        field_is_sensitive = True
                        field_rule = str(include_rule['fields'][field_name])
                    elif include_rule.get('raw_sql'):
                        field_is_sensitive = True
                        field_rule = str(include_rule['raw_sql'])

                preview_fields.append(PreviewFieldContent(
                    field_name=field_name,
                    type=field_type,
                    is_sensitive=field_is_sensitive,
                    rule=field_rule,
                ))

            result.append(PreviewTableContent(
                table_name=table_name,
                is_sensitive=is_sensitive,
                is_excluded=is_excluded,
                fields=preview_fields,
            ))

        return result

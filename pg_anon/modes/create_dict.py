import asyncio
import copy
import dataclasses
import json
import re
import shutil
import sys
import time
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from itertools import chain
from pathlib import Path
from typing import NamedTuple

from asyncpg import Connection, Pool

from pg_anon.common.constants import DEFAULT_HASH_FUNC
from pg_anon.common.db_queries import get_data_from_field_query
from pg_anon.common.db_utils import (
    check_required_connections,
    create_connection,
    create_pool,
    exec_data_scan_func_per_field_query,
    get_inherited_columns,
    get_legacy_inheritance_parents,
    get_partition_ancestors_map,
    get_partitioned_tables,
    get_scan_fields_list,
    get_scan_func_matches,
    get_tables_columns,
    InheritedColumn,
)
from pg_anon.common.dto import FieldInfo, SearchRule
from pg_anon.common.enums import MatchSource, ScanMode
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import (
    find_table_rule,
    format_names_list,
    get_base_field_type,
    get_dict_rule_for_table,
    resolve_table_rule,
    safe_compile,
    save_dicts_info_file,
    setof_to_list,
)
from pg_anon.context import Context

Table = tuple[str, str]

# sections of the search in the order of priority: names of fields, then data
SEARCH_SECTIONS = ["field.constants", "field.rules", *(source.value for source in MatchSource)]
DATA_FUNC_SECTION = SEARCH_SECTIONS.index(MatchSource.DATA_FUNC.value)


class Finding(NamedTuple):
    match_key: tuple[int, int]
    table: Table
    function: str


def _field_sort_key(field_info: FieldInfo) -> tuple[str, str, int]:
    return field_info.nspname, field_info.relname, field_info.attnum


def _table_name(table: Table) -> str:
    return f"{table[0]}.{table[1]}"


def _partition_names(partitions: Iterable[Table], schema: str | None, mark_other_schema: bool = False) -> str:
    """Render partitions for a log line: partitions of other schemas first, then by schema and name."""
    mark = " (other schema)" if mark_other_schema else ""
    ordered = sorted(partitions)
    names = [_table_name(table) + mark for table in ordered if table[0] != schema]
    names += [_table_name(table) for table in ordered if table[0] == schema]
    return format_names_list(names)


def _partitions_count(count: int) -> str:
    return "1 partition" if count == 1 else f"{count} partitions"


def own_masking_line(root: Table, partitions: Mapping[Table, Iterable[str]]) -> str:
    """Log line about the partitions of a root that keep their own functions for some fields."""
    names = [_table_name(table) for table in sorted(partitions)]
    all_fields: set[str] = set()
    for table_fields in partitions.values():
        all_fields.update(table_fields)
    fields = sorted(all_fields)
    subject = f"{_partitions_count(len(names))} {'has its' if len(names) == 1 else 'have their'}"
    noun = "field" if len(fields) == 1 else "fields"
    return f"{_table_name(root)}: {subject} own masking for {noun} {', '.join(fields)} ({format_names_list(names)})"


def _is_exact_rule(rule: dict) -> bool:
    return "schema" in rule and "table" in rule


def _has_condition(rule: dict) -> bool:
    return bool(rule.get("sql_condition"))


def _first_rule_by_values(
    fld_data: list, find_rule: Callable[[str, int], SearchRule | None], bound: int
) -> tuple[SearchRule, str] | None:
    """Find the first rule above the bound that matches at least one value; return it with the matched value."""
    found = None
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


class DataFuncRules(NamedTuple):
    """data_func rules for one field type in one order: rules for the whole field first, then rules for values.

    The rule of each SearchRule is the item of the meta dict, with its scan function and n_count.
    """

    per_field: list[SearchRule]
    by_value: list[SearchRule]


def _make_data_func_rules(items: list[dict]) -> DataFuncRules:
    """Give the rules their positions: rules for the whole field first, then rules for values, each in its order."""
    per_field = [item for item in items if "scan_func_per_field" in item]
    by_value = [item for item in items if "scan_func_per_field" not in item]
    rules: list[SearchRule] = []
    other_func_above = -1
    for position, item in enumerate([*per_field, *by_value]):
        if rules and rules[-1].anon_func != item["anon_func"]:
            other_func_above = position - 1
        rules.append(SearchRule(item, item["anon_func"], position, other_func_above))
    return DataFuncRules(rules[: len(per_field)], rules[len(per_field) :])


class MatchedRule(NamedTuple):
    match_key: tuple[int, int]  # (section, position of the rule in the section)
    rule: SearchRule


@dataclasses.dataclass
class FieldScan:
    """Scan state of one field: the best rule found so far and the values matched by scan_func rules."""

    field_info: FieldInfo
    best: MatchedRule | None = None
    # position of a scan_func rule -> values it matched
    matched_values: dict[int, set[str]] = dataclasses.field(default_factory=dict)

    def bound(self, section: int) -> int:
        """Rules of the section with positions below the bound can still give another function."""
        if self.best is None or section < self.best.match_key[0]:
            return sys.maxsize
        if section == self.best.match_key[0]:
            return self.best.rule.other_func_above + 1
        return 0

    def set_best(self, section: int, rule: SearchRule) -> None:
        """Keep the rule as the best found one; the caller checks the bound, so the rule is better than before."""
        self.best = MatchedRule((section, rule.position), rule)


@asynccontextmanager
async def _read_transaction(connection: Connection, scan_mode: ScanMode | None, condition: str | None) -> AsyncIterator:
    """Open the transaction in which a scan task reads its field."""
    if scan_mode == ScanMode.FULL:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            yield
    elif condition:
        async with connection.transaction():
            # A parallel plan under LIMIT returns rows in random order
            await connection.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            yield
    else:
        yield


class CreateDictMode:
    def __init__(self, context: Context) -> None:
        self.context = context
        # partitioned tables above each partition, closest first
        self._partition_ancestors: dict[Table, list[Table]] = {}
        self._partitioned: set[Table] = set()
        self._inherits_parents: set[Table] = set()
        # every field of the scan query by schema, table and column, also out of the scan scope
        self._all_fields: dict[tuple[str, str, str], FieldInfo] = {}
        # partition columns that the scan does not read: keys and sequence links taken from above
        self._key_columns: set[tuple[str, str, str]] = set()
        self._conditions: dict[Table, str | None] = {}
        # field -> (section, position of the rule in the section) of the rule that found it
        self._match_keys: dict[str, tuple[int, int]] = {}
        self._data_func_rules: dict[str, DataFuncRules] = {}

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

    def _find_matched_table(self, field: dict, rule: dict) -> Table | None:
        """Return the table by which the rule matches the field: its own table or a partitioned table above it."""
        table = (field["nspname"], field["relname"])
        for schema, name in [table, *self._partition_ancestors.get(table, [])]:
            if self._check_field_match_by_rule({**field, "nspname": schema, "relname": name}, rule):
                return schema, name
        return None

    def _check_include_fields(self, field: dict) -> bool:
        if not self.context.meta_dictionary_obj["include_rules"]:
            return True

        for rule in self.context.meta_dictionary_obj["include_rules"]:
            if self._find_matched_table(field, rule):
                self.context.logger.debug(
                    "------> Field %s.%s.%s included by rule %s",
                    field["nspname"],
                    field["relname"],
                    field["column_name"],
                    rule,
                )
                return True

        return False

    def _find_skip_rule(self, field: dict) -> tuple[int, Table] | None:
        """Find the first skip rule for the field, with the table it matches by."""
        for index, rule in enumerate(self.context.meta_dictionary_obj["skip_rules"]):
            if matched := self._find_matched_table(field, rule):
                self.context.logger.debug(
                    "------> Field %s.%s.%s skipped by rule %s",
                    field["nspname"],
                    field["relname"],
                    field["column_name"],
                    rule,
                )
                return index, matched
        return None

    def _find_inherited_key_columns(self, inherited_columns: list[InheritedColumn]) -> set[tuple[str, str, str]]:
        """Find columns that a partition takes from a partitioned table where the scan does not read them."""
        partition_links = []
        for link in inherited_columns:
            # INHERITS children take neither keys nor sequence links, so their columns hold plain data
            if (link.parent_schema, link.parent_table) in self._partitioned:
                child = (link.child_schema, link.child_table, link.column)
                parent = (link.parent_schema, link.parent_table, link.column)
                partition_links.append((child, parent))

        # a column is a key if its parent is not read by the scan or is a key itself
        key_columns: set[tuple[str, str, str]] = set()
        found_new = True
        while found_new:
            found_new = False
            for child, parent in partition_links:
                if child not in key_columns and (parent not in self._all_fields or parent in key_columns):
                    key_columns.add(child)
                    found_new = True
        return key_columns

    def _log_partitions_skipped_with_parent(self, skipped: dict[int, set[Table]]) -> None:
        skip_rules = self.context.meta_dictionary_obj["skip_rules"]
        for index, partitions in sorted(skipped.items()):
            count = len(partitions)
            root_schemas = {self._root_table(table)[0] for table in partitions}
            schema = root_schemas.pop() if len(root_schemas) == 1 else None
            self.context.logger.info(
                "The rule %s matches a partitioned table: %s %s skipped too (%s)",
                json.dumps(skip_rules[index], ensure_ascii=False),
                _partitions_count(count),
                "is" if count == 1 else "are",
                _partition_names(partitions, schema),
            )

    async def _load_table_relations(self, fields_list: list) -> None:
        """Load parents of tables, all fields and inherited key columns into the mode state."""
        tables = list(dict.fromkeys((field["nspname"], field["relname"]) for field in fields_list))
        connection = await create_connection(
            self.context.connection_params, server_settings=self.context.server_settings
        )
        try:
            self._partition_ancestors = await get_partition_ancestors_map(connection, tables)
            self._partitioned = await get_partitioned_tables(connection)
            inherited_columns = await get_inherited_columns(connection)
        finally:
            await connection.close()
        self._inherits_parents = await get_legacy_inheritance_parents(
            connection_params=self.context.connection_params, server_settings=self.context.server_settings
        )
        self._all_fields = {
            (field["nspname"], field["relname"], field["column_name"]): FieldInfo(**field) for field in fields_list
        }
        self._key_columns = self._find_inherited_key_columns(inherited_columns)

    def _get_fields_for_scan(self, fields_list: list) -> dict[str, FieldInfo]:
        """Get scanning fields for create dictionary mode by include and skip rules."""
        result = {}
        skipped_with_parent: dict[int, set[Table]] = {}
        for field in fields_list:
            table = (field["nspname"], field["relname"])
            if (field["nspname"], field["relname"], field["column_name"]) in self._key_columns:
                continue
            if not self._check_include_fields(field):
                continue
            skip = self._find_skip_rule(field)
            if skip is None:
                result[field["obj_id"]] = FieldInfo(**field)
                continue
            rule_index, matched_table = skip
            # the partition is skipped only because the rule names a partitioned table above it
            if matched_table != table and table not in self._partitioned:
                skipped_with_parent.setdefault(rule_index, set()).add(table)

        self._log_partitions_skipped_with_parent(skipped_with_parent)
        return result

    def _find_name_rule(self, column_name: str) -> tuple[str, SearchRule] | None:
        """Find the rule of the field section for a column name, with the name of its section."""
        field_rules = self.context.meta_dictionary_obj["field"]
        for rule in field_rules["constants"]:
            if rule.rule == column_name:
                return "field.constants", rule
        for rule in field_rules["rules"]:
            if rule.rule.search(column_name) is not None:
                return "field.rules", rule
        return None

    def _find_no_sens_rule(self, field_info: FieldInfo) -> dict | None:
        table = (field_info.nspname, field_info.relname)
        for schema, name in [table, *self._partition_ancestors.get(table, [])]:
            for rule in self.context.meta_dictionary_obj["no_sens_dictionary"]:
                if rule["schema"] == schema and rule["table"] == name and field_info.column_name in rule["fields"]:
                    return rule
        return None

    def _scan_fields_by_names(self, fields_info: dict[str, FieldInfo]) -> None:
        """Scan fields by names and remove matches according to dict rules.

        Priorities of rules:
            - prepared-sens-dict-file
            - meta-dict-file
            - prepared-no-sens-dict-file
        """
        prepared_rules = self.context.prepared_dictionary_obj.get("dictionary") or []
        table_rules: dict[Table, dict | None] = {}

        for obj_id, field_info in fields_info.copy().items():
            table = (field_info.nspname, field_info.relname)
            if table not in table_rules:
                rule, _ = resolve_table_rule(table, prepared_rules, partition_ancestors=self._partition_ancestors)
                table_rules[table] = rule
            prepared_rule = table_rules[table]

            # the prepared rule goes to the output as is, so the field is not read
            if prepared_rule and field_info.column_name in prepared_rule.get("fields", {}):
                self.context.logger.debug(
                    '------> Field %s.%s.%s is SENSITIVE by rule "%s"',
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    prepared_rule,
                )
                del fields_info[obj_id]
                continue

            if found := self._find_name_rule(field_info.column_name):
                section, name_rule = found
                self.context.logger.debug(
                    '------> Field %s.%s.%s is SENSITIVE by rule "%s"',
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    name_rule.rule,
                )
                del fields_info[obj_id]
                field_info.rule = name_rule.anon_func
                self.context.create_dict_sens_matches[obj_id] = field_info
                self._match_keys[obj_id] = (SEARCH_SECTIONS.index(section), name_rule.position)
                continue

            if no_sens_rule := self._find_no_sens_rule(field_info):
                self.context.logger.debug(
                    '------> Field %s.%s.%s is INSENSITIVE by rule "%s"',
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    no_sens_rule,
                )
                del fields_info[obj_id]
                self.context.create_dict_no_sens_matches[obj_id] = field_info

    def _check_data_by_constants(
        self, name: str, dictionary_obj: dict, field_info: FieldInfo, fld_data: list, bound: int
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

        if matched := _first_rule_by_values(fld_data, find_rule, bound):
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
        self, name: str, dictionary_obj: dict, field_info: FieldInfo, fld_data: list, bound: int
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

        if matched := _first_rule_by_values(fld_data, find_rule, bound):
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

    def _get_data_func_rules(self, field_info: FieldInfo) -> DataFuncRules:
        """Get data_func rules for the type of the field: rules of the type (or its base type), then anyelement."""
        if field_info.type in self._data_func_rules:
            return self._data_func_rules[field_info.type]

        data_func = self.context.meta_dictionary_obj["data_func"]
        rules_by_type = data_func.get(field_info.type) or data_func.get(get_base_field_type(field_info), [])
        items = []
        for item in [*rules_by_type, *data_func.get("anyelement", [])]:
            if "scan_func_per_field" in item or "scan_func" in item:
                items.append(item)
            else:
                self.context.logger.warning("Rule SKIPPED. Wrong data func rule: %s", item)

        rules = _make_data_func_rules(items)
        self._data_func_rules[field_info.type] = rules
        return rules

    async def _check_by_per_field_functions(
        self, connection: Connection, name: str, field_info: FieldInfo
    ) -> SearchRule | None:
        """Call scan_func_per_field rules once for the field, before its data is read."""
        for rule in self._get_data_func_rules(field_info).per_field:
            if await exec_data_scan_func_per_field_query(
                connection=connection, scan_func_per_field=rule.rule["scan_func_per_field"], field_info=field_info
            ):
                self.context.logger.debug(
                    "========> Process[%s]: Field %s.%s.%s is SENSITIVE by data scan func per field %s",
                    name,
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    rule.rule["scan_func_per_field"],
                )
                return rule
        return None

    async def _check_data_by_functions(
        self, connection: Connection, name: str, scan: FieldScan, fld_data: list, bound: int
    ) -> SearchRule | None:
        """Check the values by scan_func rules from the top; a rule matches when it gets n_count values in all."""
        field_info = scan.field_info
        rules = [rule for rule in self._get_data_func_rules(field_info).by_value if rule.position < bound]
        if not rules:
            return None

        self.context.logger.debug(
            "========> Process[%s]: checking by functions data of field %s.%s.%s (type = %s)",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
            field_info.type,
        )

        values = [value for value in fld_data if value is not None]
        for rule in rules:
            n_count = rule.rule.get("n_count", 1)
            matched = scan.matched_values.setdefault(rule.position, set())
            limit = n_count - len(matched)
            matched.update(await get_scan_func_matches(connection, rule.rule["scan_func"], values, field_info, limit))
            if len(matched) >= n_count:
                self.context.logger.debug(
                    "========> Process[%s]: Field %s.%s.%s is SENSITIVE by data scan func %s",
                    name,
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    rule.rule["scan_func"],
                )
                return rule

        self.context.logger.debug(
            "========> Process[%s]: No one data functions found sensitive data in field %s.%s.%s",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
        )
        return None

    def _check_data_by_regexp(
        self, name: str, dictionary_obj: dict, field_info: FieldInfo, fld_data: list, bound: int
    ) -> SearchRule | None:
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

        if rules and (matched := _first_rule_by_values(fld_data, find_rule, bound)):
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

    async def _check_portion(
        self, connection: Connection, name: str, dictionary_obj: dict, scan: FieldScan, fld_data: list
    ) -> None:
        """Check a portion of values by sections in the order of priority, only by rules above the best found rule."""
        field_info = scan.field_info
        if not fld_data:
            self.context.logger.debug(
                "---> Process[%s]: Check sensitive skipped cause field %s.%s.%s has not data",
                name,
                field_info.nspname,
                field_info.relname,
                field_info.column_name,
            )
            return

        self.context.logger.debug(
            "---> Process[%s]: Started check sensitive data in field - %s.%s.%s. (%s values)",
            name,
            field_info.nspname,
            field_info.relname,
            field_info.column_name,
            len(fld_data),
        )

        for source in MatchSource:
            section = SEARCH_SECTIONS.index(source.value)
            bound = scan.bound(section)
            if bound == 0:
                continue

            match source:
                case MatchSource.DATA_FUNC:
                    rule = await self._check_data_by_functions(connection, name, scan, fld_data, bound)
                case MatchSource.DATA_CONST:
                    rule = self._check_data_by_constants(name, dictionary_obj, field_info, fld_data, bound)
                case MatchSource.DATA_PARTIAL_CONST:
                    rule = self._check_data_by_partial_constants(name, dictionary_obj, field_info, fld_data, bound)
                case MatchSource.DATA_REGEX:
                    rule = self._check_data_by_regexp(name, dictionary_obj, field_info, fld_data, bound)

            if rule is not None:
                scan.set_best(section, rule)

    def _first_value_rule_position(
        self, dictionary_obj: dict, source: MatchSource, field_info: FieldInfo
    ) -> int | None:
        """Position of the first rule of the section that checks values, or None if the section has no such rules."""
        match source:
            case MatchSource.DATA_FUNC:
                rules = self._get_data_func_rules(field_info).by_value
                return rules[0].position if rules else None
            case MatchSource.DATA_CONST:
                return 0 if dictionary_obj["data_const"]["constant_rules"] else None
            case MatchSource.DATA_PARTIAL_CONST:
                return 0 if dictionary_obj["data_const"]["partial_constant_rules"] else None
            case MatchSource.DATA_REGEX:
                return 0 if dictionary_obj["data_regex"]["rules"] else None

    def _is_field_done(self, dictionary_obj: dict, scan: FieldScan) -> bool:
        """The rest of the values can not change the result: no rule is left above the found one with another function."""
        for source in MatchSource:
            position = self._first_value_rule_position(dictionary_obj, source, scan.field_info)
            if position is not None and position < scan.bound(SEARCH_SECTIONS.index(source.value)):
                return False
        return True

    def _field_can_be_sensitive_by_type(self, dictionary_obj: dict, field_info: FieldInfo) -> bool:
        if field_info.type in dictionary_obj["sens_pg_types"]:
            return True

        base_field_type = get_base_field_type(field_info)
        return base_field_type in dictionary_obj["sens_pg_types"]

    async def _read_field(  # noqa: PLR0913
        self,
        connection: Connection,
        name: str,
        dictionary_obj: dict,
        scan: FieldScan,
        scan_mode: ScanMode | None,
        scan_partial_rows: int,
    ) -> None:
        """Read the values of the field and check them: one portion in the partial mode, all portions in the full mode."""
        field_info = scan.field_info
        table = (field_info.nspname, field_info.relname)
        query = get_data_from_field_query(
            field_info=field_info,
            limit=None if scan_mode == ScanMode.FULL else scan_partial_rows,
            condition=self._conditions.get(table),
            only=table in self._inherits_parents,
        )
        if scan_mode != ScanMode.FULL:
            fld_data = await connection.fetch(query)
            await self._check_portion(connection, name, dictionary_obj, scan, setof_to_list(fld_data))
            return

        cursor = await connection.cursor(query)
        while True:
            fld_data = await cursor.fetch(scan_partial_rows)
            if not fld_data:
                break
            await self._check_portion(connection, name, dictionary_obj, scan, setof_to_list(fld_data))
            if scan.best is not None and self._is_field_done(dictionary_obj, scan):
                break

    async def _scan_obj_func(
        self,
        name: str,
        pool: Pool,
        field_info: FieldInfo,
        scan_mode: ScanMode | None,
        dictionary_obj: dict,
        scan_partial_rows: int,
    ) -> FieldScan | None:
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

        scan = FieldScan(field_info)
        condition = self._conditions.get((field_info.nspname, field_info.relname))
        try:
            async with pool.acquire() as db_conn, _read_transaction(db_conn, scan_mode, condition):
                rule = await self._check_by_per_field_functions(db_conn, name, field_info)
                if rule is not None:
                    # the per-field rules are the first rules of the data: nothing in the data can win over them
                    scan.set_best(DATA_FUNC_SECTION, rule)
                else:
                    await self._read_field(db_conn, name, dictionary_obj, scan, scan_mode, scan_partial_rows)

        except Exception as ex:
            field = f"{field_full_name} (type={field_info.type})"
            self.context.logger.exception("Exception in scan_obj_func:\n%s", field)
            raise PgAnonError(ErrorCode.SCAN_FIELD_ERROR, f"Can't execute task for field {field}. Error: {ex}") from ex

        end_t = time.time()
        if end_t - start_t > 10:  # noqa: PLR2004
            self.context.logger.debug(
                "Process[%s]: scan_obj_func took %s sec. Task %s", name, round(end_t - start_t, 2), field_info
            )

        if scan.best is None:
            self.context.logger.debug("<<<<==== Process[%s]: Field %s is INSENSITIVE", name, field_full_name)
            return scan

        field_info.rule = scan.best.rule.anon_func
        self._match_keys[field_info.obj_id] = scan.best.match_key
        self.context.logger.debug(
            "<<<<==== Process[%s]: Field %s is SENSITIVE by %s",
            name,
            field_full_name,
            SEARCH_SECTIONS[scan.best.match_key[0]],
        )
        return scan

    async def _run_scan_tasks(self, fields_info_list: list[FieldInfo]) -> list[FieldScan]:  # noqa: C901
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

        results: list[FieldScan] = []
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
                        if (scan := done_task.result()) is not None:
                            results.append(scan)

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
                    if (scan := done_task.result()) is not None:
                        results.append(scan)
        finally:
            await pool.close()

        return results

    def _get_masking_function(self, field_info: FieldInfo) -> str:
        """Masking function of a found field: the function of its rule or the function for its type."""
        meta_dictionary_obj = self.context.meta_dictionary_obj
        hash_func = field_info.rule

        if hash_func is None:
            base_field_type = get_base_field_type(field_info)
            if field_info.type in meta_dictionary_obj["funcs"]:
                hash_func = meta_dictionary_obj["funcs"][field_info.type]
            elif base_field_type in meta_dictionary_obj["funcs"]:
                hash_func = meta_dictionary_obj["funcs"][base_field_type]
            else:
                hash_func = meta_dictionary_obj["funcs"].get("default", DEFAULT_HASH_FUNC)

        return hash_func.replace("%s", field_info.column_name)

    def _root_table(self, table: Table) -> Table:
        """Return the top partitioned table above a partition, or the table itself."""
        above = self._partition_ancestors.get(table)
        return above[-1] if above else table

    def _get_target_field(self, field_info: FieldInfo) -> FieldInfo:
        """The field to write a finding to: the same column of the top partitioned table, or the field itself."""
        table = (field_info.nspname, field_info.relname)
        root = self._root_table(table)
        if root == table:
            return field_info
        return self._all_fields[(*root, field_info.column_name)]

    def _prepare_conditions(self, fields: Iterable[FieldInfo]) -> None:
        """Choose data_sql_condition of each table and log the conditions that partitions take from above."""
        rules = self.context.meta_dictionary_obj.get("data_sql_condition") or []
        inherited: dict[Table, set[Table]] = {}
        for table in dict.fromkeys((field.nspname, field.relname) for field in fields):
            self._conditions[table] = None
            found = find_table_rule(table, rules, self._partition_ancestors)
            if found is None:
                continue
            rule, rule_table = found
            self._conditions[table] = rule.get("sql_condition")
            if rule_table != table:
                inherited.setdefault(rule_table, set()).add(table)

        for source, partitions in sorted(inherited.items()):
            self.context.logger.info(
                "data_sql_condition of %s is used for its %s: %s",
                _table_name(source),
                _partitions_count(len(partitions)),
                _partition_names(partitions, source[0], mark_other_schema=True),
            )

    def _rule_by_all_partitions(self, partitions: list[FieldScan]) -> SearchRule | None:
        """The first scan_func rule that gets n_count by the values of all partitions together, without repeats."""
        values: dict[int, set[str]] = {}
        for scan in partitions:
            for position, matched in scan.matched_values.items():
                values.setdefault(position, set()).update(matched)

        # the columns of all partitions have the type of the root column, so the positions of rules are the same
        for rule in self._get_data_func_rules(partitions[0].field_info).by_value:
            if len(values.get(rule.position, set())) >= rule.rule.get("n_count", 1):
                return rule
        return None

    def _find_by_all_partitions(self, scans: list[FieldScan]) -> list[FieldInfo]:
        """Find fields of partitioned tables that a scan_func rule finds only by the values of all partitions."""
        partitions_by_root: dict[str, list[FieldScan]] = {}
        roots: dict[str, FieldInfo] = {}
        for scan in scans:
            target = self._get_target_field(scan.field_info)
            if target is not scan.field_info:
                partitions_by_root.setdefault(target.obj_id, []).append(scan)
                roots[target.obj_id] = target

        found = []
        for obj_id, partitions in partitions_by_root.items():
            # a root field found by its name has a better rule than any rule of data
            if obj_id in self._match_keys:
                continue
            rule = self._rule_by_all_partitions(partitions)
            if rule is None:
                continue
            match_key = (DATA_FUNC_SECTION, rule.position)
            partition_keys = [scan.best.match_key for scan in partitions if scan.best is not None]
            if partition_keys and min(partition_keys) <= match_key:
                continue
            self._match_keys[obj_id] = match_key
            found.append(dataclasses.replace(roots[obj_id], rule=rule.anon_func))
        return found

    def _choose_functions(self, sens_fields: list[FieldInfo]) -> list[tuple[FieldInfo, str]]:
        """Choose one function for each field; when partitions give different functions, the first section wins."""
        targets: dict[str, FieldInfo] = {}
        findings: dict[str, list[Finding]] = {}
        for field_info in sens_fields:
            target = self._get_target_field(field_info)
            targets[target.obj_id] = target
            # a field without a key goes after all sections
            match_key = self._match_keys.get(field_info.obj_id, (len(SEARCH_SECTIONS), 0))
            table = (field_info.nspname, field_info.relname)
            finding = Finding(match_key, table, self._get_masking_function(field_info))
            findings.setdefault(target.obj_id, []).append(finding)

        chosen = []
        for obj_id, items in findings.items():
            target = targets[obj_id]
            items.sort(key=lambda finding: (finding.match_key, finding.table))
            function = items[0].function
            chosen.append((target, function))
            if len({finding.function for finding in items}) > 1:
                all_functions = ", ".join(f"{finding.function} ({_table_name(finding.table)})" for finding in items)
                self.context.logger.warning(
                    "Partitions of %s.%s give different functions for field %s: %s. The function %s is used",
                    target.nspname,
                    target.relname,
                    target.column_name,
                    all_functions,
                    function,
                )
        chosen.sort(key=lambda item: _field_sort_key(item[0]))
        return chosen

    def _same_masking_without_field(self, table: Table, rules: list[dict], rule: dict, field: str) -> bool:
        """Check that the rules above give the same function for the field if this rule drops it."""
        excludes = self.context.prepared_dictionary_obj.get("dictionary_exclude")
        before, before_excluded = resolve_table_rule(table, rules, excludes, self._partition_ancestors)
        other_fields = {name: function for name, function in rule["fields"].items() if name != field}
        candidate = []
        for item in rules:
            if item is not rule:
                candidate.append(item)
            elif other_fields:
                candidate.append({**rule, "fields": other_fields})
        after, after_excluded = resolve_table_rule(table, candidate, excludes, self._partition_ancestors)
        if before is None or not after or before_excluded != after_excluded:
            return False
        return after["fields"].get(field) == before["fields"][field]

    def _clean_partition_rules(self, rules: list[dict]) -> None:
        """Remove fields from exact rules of partitions when the rules above give the same masking."""
        # a copy: rules can be removed from the list in the loop
        partition_rules = [
            rule for rule in rules if (rule.get("schema"), rule.get("table")) in self._partition_ancestors
        ]
        for rule in partition_rules:
            table = (rule["schema"], rule["table"])
            for field in list(rule.get("fields", {})):
                if not self._same_masking_without_field(table, rules, rule, field):
                    continue
                if len(rule["fields"]) > 1:
                    del rule["fields"][field]
                elif not _has_condition(rule):
                    rules.remove(rule)
                # else: a rule with a condition keeps its last field

    def _log_own_partition_rules(self, rules: list[dict]) -> None:
        by_root: dict[Table, dict[Table, list[str]]] = {}
        for rule in rules:
            table = (rule.get("schema"), rule.get("table"))
            if table in self._partition_ancestors and rule.get("fields"):
                by_root.setdefault(self._root_table(table), {})[table] = list(rule["fields"])
        for root, partitions in sorted(by_root.items()):
            self.context.logger.info(own_masking_line(root, partitions))

    async def _get_db_columns(self) -> dict[Table, set[str]]:
        """Get the columns of all tables of the database."""
        connection = await create_connection(
            self.context.connection_params, server_settings=self.context.server_settings
        )
        try:
            return await get_tables_columns(connection)
        finally:
            await connection.close()

    def _get_kept_prepared_rules(self, db_columns: dict[Table, set[str]]) -> list[dict]:
        """Keep the prepared rules for tables of the database, with only the fields these tables have."""
        rules = self.context.prepared_dictionary_obj.get("dictionary") or []
        kept = []
        for rule in rules:
            if not _is_exact_rule(rule):
                # a rule with masks stays if it matches at least one table of the database
                if any(get_dict_rule_for_table([rule], schema, table) for schema, table in db_columns):
                    kept.append(rule)
                continue
            columns = db_columns.get((rule["schema"], rule["table"]))
            if columns is None:
                continue
            if "fields" not in rule:
                kept.append(rule)
                continue
            fields = {field: func for field, func in rule["fields"].items() if field in columns}
            if fields:
                kept.append({**rule, "fields": fields})
            elif _has_condition(rule):
                # the rule is still needed for its condition, so it stays as it is
                kept.append(rule)
        return kept

    def _make_sens_dict(self, chosen: list[tuple[FieldInfo, str]], kept_rules: list[dict]) -> dict:
        """Build the sens dict: the prepared rules, plus the found fields added to the rules of their tables."""
        rules: list[dict] = copy.deepcopy(kept_rules)
        exact: dict[Table, dict] = {}
        for rule in rules:
            if _is_exact_rule(rule):
                exact.setdefault((rule["schema"], rule["table"]), rule)

        for target, function in chosen:
            table = (target.nspname, target.relname)
            if table not in exact:
                exact[table] = {"schema": target.nspname, "table": target.relname, "fields": {}}
                rules.append(exact[table])
            fields = exact[table].setdefault("fields", {})
            # the function from the prepared dict wins over the found one
            if target.column_name not in fields:
                fields[target.column_name] = function

        self._clean_partition_rules(rules)
        self._log_own_partition_rules(rules)
        # rules with masks keep their order and go after the rules for exact names
        exact_rules = [rule for rule in rules if _is_exact_rule(rule)]
        mask_rules = [rule for rule in rules if not _is_exact_rule(rule)]
        exact_rules.sort(key=lambda rule: (rule["schema"], rule["table"]))

        output = {"dictionary": exact_rules + mask_rules}
        if excludes := self.context.prepared_dictionary_obj.get("dictionary_exclude"):
            output["dictionary_exclude"] = excludes
        return output

    def _make_no_sens_dict(self, no_sens_fields: list[FieldInfo], sens_dict: dict) -> dict:
        """Build the no sens dict from the fields that are not in the sens dict; partitions go to their top table."""
        sens_fields: set[tuple[str, str, str]] = set()
        for rule in sens_dict["dictionary"]:
            if _is_exact_rule(rule):
                table = (rule["schema"], rule["table"])
                root = self._root_table(table)
                sens_fields.update((*root, field) for field in rule.get("fields", {}))

        targets: dict[str, FieldInfo] = {}
        for field_info in no_sens_fields:
            target = self._get_target_field(field_info)
            targets[target.obj_id] = target

        records: dict[Table, list[str]] = {}
        for target in sorted(targets.values(), key=_field_sort_key):
            if (target.nspname, target.relname, target.column_name) in sens_fields:
                continue
            records.setdefault((target.nspname, target.relname), []).append(target.column_name)

        return {
            "no_sens_dictionary": [
                {"schema": schema, "table": table, "fields": fields}
                for (schema, table), fields in sorted(records.items())
            ]
        }

    @staticmethod
    def _write_json(data: dict, file_name: str | None) -> None:
        path = Path.cwd() / (file_name or "")
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as file:
            file.write(json.dumps(data, indent=4, ensure_ascii=False))

    async def _create_dict(self) -> None:
        fields_list = await get_scan_fields_list(
            connection_params=self.context.connection_params, server_settings=self.context.server_settings
        )
        await self._load_table_relations(fields_list)
        fields_info: dict[str, FieldInfo] = self._get_fields_for_scan(fields_list)
        if not fields_info:
            raise PgAnonError(ErrorCode.NO_OBJECTS_FOR_SCAN, "No objects for scan!")

        # fill self.context.create_dict_sens_matches and self.context.create_dict_no_sens_matches
        self._scan_fields_by_names(fields_info)

        # partitioned tables have no rows of their own: their columns are checked by names only
        fields_to_read = [
            field for field in fields_info.values() if (field.nspname, field.relname) not in self._partitioned
        ]
        self._prepare_conditions(fields_to_read)
        scans = await self._run_scan_tasks(fields_to_read) if fields_to_read else []
        scanned_sens_fields = [scan.field_info for scan in scans if scan.best is not None]
        scanned_sens_fields.extend(self._find_by_all_partitions(scans))

        sens_fields = scanned_sens_fields + list(self.context.create_dict_sens_matches.values())
        chosen = self._choose_functions(sens_fields)
        kept_rules = self.context.prepared_dictionary_obj.get("dictionary") or []
        if kept_rules and not self.context.options.keep_unmatched_prepared_rules:
            kept_rules = self._get_kept_prepared_rules(await self._get_db_columns())
        sens_dict = self._make_sens_dict(chosen, kept_rules)
        self._write_json(sens_dict, self.context.options.output_sens_dict_file)

        if self.context.options.output_no_sens_dict_file:
            scanned_ids = {field.obj_id for field in scanned_sens_fields}
            no_sens_fields = [
                *self.context.create_dict_no_sens_matches.values(),
                *(field for field in fields_info.values() if field.obj_id not in scanned_ids),
            ]
            self._write_json(
                self._make_no_sens_dict(no_sens_fields, sens_dict), self.context.options.output_no_sens_dict_file
            )

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
            if self.context.prepared_dictionary_obj.get("dictionary_exclude"):
                self.context.logger.info(
                    "dictionary_exclude of the prepared sens dict does not limit the scan and goes to the output "
                    "sens dict as is. To skip tables in the scan, use skip_rules of the meta dict"
                )
            await self._create_dict()

            self._save_output_dicts_to_run_dir()
            self.context.logger.info("<------------- Finished create_dict mode")
        finally:
            if self.context.options.save_dicts:
                save_dicts_info_file(self.context.options)

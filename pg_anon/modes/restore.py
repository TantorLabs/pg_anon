import asyncio
import gzip
import json
import re
import shlex
import shutil
import subprocess
from collections import defaultdict
from copy import copy
from pathlib import Path
from typing import Any

import asyncpg
from asyncpg import Connection

from pg_anon.common.db_queries import (
    get_check_constraint_query,
    get_db_params,
    get_sequences_max_value_init_query,
    schemas_filter,
)
from pg_anon.common.db_utils import (
    check_db_is_empty,
    check_required_connections,
    count_foreign_keys_of_tables,
    create_connection,
    create_pool,
    get_available_extensions_map,
    get_available_schemas,
    get_extensions,
    get_foreign_descendants_map,
    get_invalid_partitioned_indexes,
    run_query_in_pool,
)
from pg_anon.common.dependencies import dependency_error, map_toc_entries
from pg_anon.common.dto import Metadata
from pg_anon.common.enums import AnonMode
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.toc import (
    CUSTOM_OBJECTS_ENTRY_RE,
    entry_id,
    entry_ids,
    entry_title,
    EXTENSION_ENTRY_RE,
    failed_entry_ids,
    is_table_entry,
    OPERATOR_CLASS_ENTRY_RE,
    PUBLIC_SCHEMA_ENTRY_RE,
    read_toc_entries,
    read_toc_lines,
    schema_entry_patterns,
    schemas_in_toc,
    table_entry_matches,
    TABLE_LINK_DESCS,
    TABLE_PART_DESCS,
    USER_MAPPING_ENTRY_RE,
    write_toc_list,
)
from pg_anon.common.utils import (
    build_pg_util_env,
    format_names_summary,
    get_major_version_parts,
    get_pg_util_version,
    log_rules_for_parent_tables,
    parse_version,
    resolve_schemas,
    save_dicts_info_file,
    table_matches_rules,
    warn_rules_without_tables,
)
from pg_anon.context import Context
from pg_anon.version import __version__


def _format_major(version: tuple[int, ...]) -> str:
    return ".".join(str(part) for part in version)


class RestoreMode:
    context: Context
    input_dir: Path

    metadata: Metadata
    metadata_file_name: str = "metadata.json"
    metadata_file_path: Path
    structure_file_name: str = "structure.json"
    structure_file_path: Path

    _toc_list_pre_data_file_name: str = "toc_pre_data_filtered.list"
    _toc_list_pre_data_file_path: Path | None = None
    _toc_list_post_data_file_name: str = "toc_post_data_filtered.list"
    _toc_list_post_data_file_path: Path | None = None
    _toc_list_post_data_head_file_name: str = "toc_post_data_head.list"
    _toc_list_post_data_head_file_path: Path | None = None
    _toc_list_post_data_tail_file_name: str = "toc_post_data_tail.list"
    _toc_list_post_data_tail_file_path: Path | None = None

    _db_must_be_empty: bool
    _skip_pre_data_restore: bool = False
    _skip_post_data_restore: bool = False

    _restored_schemas: list[str]

    _metadata_extensions: dict[str, dict[str, Any]]
    _available_extensions: dict[str, dict[str, Any]]
    _available_schemas: list[str]
    _installed_extensions: dict[str, str]
    _created_extensions: set[str]

    @property
    def _whitelist_active(self) -> bool:
        return bool(self.context.included_tables_rules)

    @property
    def _is_legacy_dump(self) -> bool:
        """Dumps of pg_anon 1.12.0 and older have no version and no dependencies of objects."""
        return self.metadata.pg_anon_version is None

    def __init__(self, context: Context) -> None:
        self.context = context
        self.input_dir = Path.cwd() / self.context.options.input_dir

        if not self.input_dir.exists():
            msg = f"ERROR: input directory {self.input_dir} does not exists"
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.INPUT_DIR_NOT_FOUND, msg)

        self._load_metadata()

        self._db_must_be_empty = self.context.options.mode in (AnonMode.RESTORE, AnonMode.SYNC_STRUCT_RESTORE) and not (
            self.context.options.clean_db or self.context.options.drop_db
        )
        self._skip_pre_data_restore = (
            self.context.options.mode == AnonMode.SYNC_DATA_RESTORE or self.metadata.dbg_stage_2_validate_data
        )
        self._skip_post_data_restore = (
            self.context.options.mode == AnonMode.SYNC_DATA_RESTORE
            or self.metadata.dbg_stage_2_validate_data
            or self.metadata.dbg_stage_3_validate_full
        )
        self._not_valid_fk: list[dict] = []
        self._table_ancestors: dict[tuple[str, str], list[tuple[str, str]]] = {}
        self._excluded_schemas: set[str] = set()
        self._dump_tables: set[tuple[str, str]] = set()
        self._structure_tables: set[tuple[str, str]] = set()
        self._excluded_tables: set[tuple[str, str]] = set()
        self._tables_without_rows: set[tuple[str, str]] = set()
        self._tables_with_new_rows: set[tuple[str, str]] = set()
        self._toc_dependencies_cache: dict[str, tuple[dict[int, list[tuple[str, str]]], set[int]]] = {}
        self._needed_tables: dict[tuple[str, str], set[tuple[str, str]]] | None = None
        self._dumped_rows: dict[tuple[str, str], int] = {
            (info["schema"], info["table"]): int(info.get("rows") or 0) for info in (self.metadata.files or {}).values()
        }
        self._partial_data_tables: set[tuple[str, str]] = {
            (info["schema"], info["table"]) for info in (self.metadata.files or {}).values() if info.get("partial_data")
        }

        if self.context.options.mode == AnonMode.SYNC_DATA_RESTORE and self.context.options.pg_restore_options:
            self.context.logger.warning(
                "--pg-restore-options is ignored in sync-data-restore: pg_restore is not used in this mode. "
                "To limit what is restored, use partial dictionaries."
            )

    @staticmethod
    def _read_dump_file(file_path: Path) -> dict | None:
        if not file_path.exists():
            return None
        return json.loads(file_path.read_text(encoding="utf-8"))

    def _load_metadata(self) -> None:
        """Read the descriptions of the dump and stop when the mode needs a file that the dump does not have."""
        self.metadata_file_path = self.input_dir / self.metadata_file_name
        self.structure_file_path = self.input_dir / self.structure_file_name
        self.metadata = Metadata()

        data = self._read_dump_file(self.metadata_file_path)
        structure = self._read_dump_file(self.structure_file_path)
        self._has_structure = structure is not None
        self._min_pg_anon_versions = {
            file_path.name: content.get("min_pg_anon_version")
            for file_path, content in ((self.structure_file_path, structure), (self.metadata_file_path, data))
            if content is not None
        }

        if structure is None and data is not None and "pg_anon_version" not in data:
            # a dump of pg_anon 1.12.0 or older keeps everything in metadata.json
            self.metadata.load_from_file(self.metadata_file_path)
            return

        mode = self.context.options.mode
        if structure is None and mode != AnonMode.SYNC_DATA_RESTORE:
            msg = (
                f"ERROR: {self.structure_file_name} is not found in {self.input_dir}. The mode {mode.value} needs "
                f"the structure files of a dump: pre_data.backup, post_data.backup and {self.structure_file_name}."
            )
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.DUMP_STRUCTURE_NOT_FOUND, msg)

        if mode != AnonMode.SYNC_STRUCT_RESTORE and not (data or {}).get("files"):
            msg = (
                f"ERROR: the dump in {self.input_dir} has no data: {self.metadata_file_name} is not found "
                f"or lists no data files. The mode {mode.value} needs the data files of a dump: "
                f"{self.metadata_file_name} and *.bin.gz. To restore only the structure, use sync-struct-restore."
            )
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.DUMP_DATA_NOT_FOUND, msg)

        self.metadata.load_from_dump_files(structure, data)

    def _generate_analyze_queries(self, foreign_parents: set[tuple[str, str]]) -> list[str]:
        analyze_queries = []
        for target in (self.metadata.files or {}).values():
            schema = target["schema"]
            table = target["table"]
            # ANALYZE of a parent table reads its foreign child tables, even with ONLY on PostgreSQL 18
            if not self._is_table_restored(schema, table) or (schema, table) in foreign_parents:
                continue

            analyze_queries.append(f'analyze "{schema}"."{table}"')
        return analyze_queries

    async def _find_foreign_parents(self, connection: Connection) -> set[tuple[str, str]]:
        """Find the restored tables with foreign child tables and warn about them."""
        restored = {
            (target["schema"], target["table"])
            for target in (self.metadata.files or {}).values()
            if self._is_table_restored(target["schema"], target["table"])
        }
        foreign_parents = set(await get_foreign_descendants_map(connection)) & restored
        if foreign_parents:
            self.context.logger.warning(
                "Foreign child tables found under %s. Without a user mapping, queries to these tables without ONLY "
                "fail. These tables are not analyzed and have no statistics.",
                format_names_summary([f"{schema}.{table}" for schema, table in foreign_parents], "table"),
            )
        return foreign_parents

    def _check_utils_version_for_dump(self) -> None:
        if self.context.options.disable_checks:
            return

        target_postgres_version = get_major_version_parts(self.context.pg_version)
        source_postgres_version = get_major_version_parts(self.metadata.pg_version)

        target_pg_restore_version = get_major_version_parts(get_pg_util_version(self.context.pg_restore))
        source_pg_dump_version = get_major_version_parts(self.metadata.pg_dump_version)

        if target_postgres_version < source_postgres_version:
            raise PgAnonError(
                ErrorCode.VERSION_INCOMPATIBLE,
                f"Target PostgreSQL major version {_format_major(target_postgres_version)} "
                f"is below than source {_format_major(source_postgres_version)}!",
            )

        if target_pg_restore_version < source_pg_dump_version:
            raise PgAnonError(
                ErrorCode.VERSION_INCOMPATIBLE,
                f"pg_restore major version {_format_major(target_pg_restore_version)} "
                f"is below than source pg_dump version {_format_major(source_pg_dump_version)}!",
            )

    async def _check_db_is_empty(self, connection: Connection) -> None:
        if not self._db_must_be_empty:
            return

        if not await check_db_is_empty(connection=connection):
            raise PgAnonError(
                ErrorCode.DB_NOT_EMPTY, f"Target DB {self.context.connection_params.database} is not empty!"
            )

    @staticmethod
    async def _fetch_target_tables(connection: Connection, only_roots: bool = False) -> set[tuple[str, str]]:
        """Read tables from the catalog, partitioned parents included, without the privilege filter of get_db_tables."""
        roots_only_clause = "AND p.oid IS NULL" if only_roots else ""
        rows = await connection.fetch(
            f"""
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_inherits inh ON inh.inhrelid = c.oid
            LEFT JOIN pg_class p ON p.oid = inh.inhparent AND p.relkind = 'p'
            WHERE c.relkind IN ('r', 'p')
              AND {schemas_filter("n.nspname")}
              {roots_only_clause}
            """  # noqa: S608
        )
        return {(row[0], row[1]) for row in rows}

    def _resolve_schemas(self) -> None:
        """Apply the schema name and mask options to the schemas of the dump."""
        options = self.context.options
        schema_filters = (
            options.schema_names,
            options.schema_masks,
            options.exclude_schema_names,
            options.exclude_schema_masks,
        )
        if not any(schema_filters):
            return

        dump_schemas = self.metadata.schemas
        if dump_schemas is None:
            # backward compatibility: old dumps have no "schemas" key, so the schemas are collected by hand
            table_schemas = {info["schema"] for info in (self.metadata.files or {}).values()}
            dump_schemas = sorted(table_schemas | self._extract_schemas_from_toc())

        kept_schemas = resolve_schemas(dump_schemas, *schema_filters)
        self._excluded_schemas = set(dump_schemas) - set(kept_schemas)
        self.context.logger.info("Restore includes %s of %s schemas", len(kept_schemas), len(dump_schemas))

    def _ddl_is_excluded_by_schema(self, ddl: str) -> bool:
        """Check if a DDL statement of a 1.12.0 or older partial dump creates its object in an excluded schema."""
        for schema in self._excluded_schemas:
            # the dump quotes a name only when it has to, so both forms are matched
            quoted = '"' + schema.replace('"', '""') + '"'
            names = f"(?:{re.escape(schema)}|{re.escape(quoted)})"
            if re.search(rf"CREATE\s+(?:OR\s+REPLACE\s+)?\w+\s+{names}\.", ddl):
                return True
        return False

    def _excluded_event_triggers(self) -> list[str]:
        """Return the event triggers whose function is in a schema excluded on dump or on restore."""
        if self.metadata.event_triggers is None:
            # dumps of older versions have only the triggers excluded on dump
            return self.metadata.excluded_event_triggers or []

        return [
            name
            for name, trigger in self.metadata.event_triggers.items()
            if trigger["is_excluded"] or trigger["function_schema"] in self._excluded_schemas
        ]

    def _is_table_restored(self, schema: str, table: str) -> bool:
        """Check the table against the excluded schemas and the partial restore lists, black list first."""
        if schema in self._excluded_schemas:
            return False

        if not self._is_legacy_dump and (schema, table) in self._dump_tables:
            # a parent table that only a whitelisted child needs is created, but its own rows are not loaded
            return (schema, table) not in self._excluded_tables and (schema, table) not in self._tables_without_rows

        if self.context.black_listed_tables and (schema, table) in self.context.black_listed_tables:
            return False

        return not (self._whitelist_active and (schema, table) not in self.context.white_listed_tables)

    async def _check_target_tables(self, connection: Connection) -> None:
        """Stop before loading data when the target database has no table to load it into."""
        if self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE or not self.metadata.files:
            return

        expected_tables = {
            (info["schema"], info["table"])
            for info in self.metadata.files.values()
            if self._is_table_restored(info["schema"], info["table"])
        }
        missing_tables = expected_tables - await self._fetch_target_tables(connection)
        if not missing_tables:
            return

        raise PgAnonError(
            ErrorCode.TARGET_MISSING_TABLES,
            f"Target database is missing "
            f"{format_names_summary([f'{schema}.{table}' for schema, table in missing_tables], 'table')} "
            f"from the dump. The dump and the target structure do not match. "
            f"Use partial dictionaries to restore a subset.",
        )

    def _tables_in_structure(self, tables: set[tuple[str, str]]) -> set[tuple[str, str]]:
        """Return the given tables that the dump creates."""
        tables_by_schema: dict[str, list[str]] = defaultdict(list)
        for schema, table in tables:
            tables_by_schema[schema].append(table)

        found = set()
        for schema, names in tables_by_schema.items():
            toc_lines = read_toc_lines(
                self.context.pg_restore,
                self.input_dir / "pre_data.backup",
                env=self._make_pg_restore_env(),
                schemas=[schema],
                tables=names,
            )
            found |= {
                (schema, name) for name in names if any(table_entry_matches(line, schema, name) for line in toc_lines)
            }
        return found

    async def _check_no_extra_tables_in_target(self, connection: Connection) -> None:
        if not self.context.options.clean_db:
            return
        if self.context.options.mode in (AnonMode.SYNC_DATA_RESTORE, AnonMode.SYNC_STRUCT_RESTORE):
            return

        # the lists choose tables of the dump, so a table that the dump does not have is extra with them too
        dumped_tables = {(info["schema"], info["table"]) for info in (self.metadata.files or {}).values()}

        target_tables = await self._fetch_target_tables(connection, only_roots=True)
        extras = target_tables - dumped_tables
        if extras:
            # partitioned roots and tables with excluded data have no data file, but the dump creates them
            extras -= self._tables_in_structure(extras)
        if not extras:
            return

        sorted_extras = sorted(extras)
        preview = ", ".join(f'"{s}"."{t}"' for s, t in sorted_extras[:10])
        if len(sorted_extras) > 10:
            preview += f", ... (+{len(sorted_extras) - 10} more)"
        raise PgAnonError(
            ErrorCode.TARGET_HAS_EXTRA_TABLES,
            f"Target DB has {len(sorted_extras)} table(s) not present in the dump: {preview}. "
            "--clean-db would leave them untouched — aborting to avoid silent data loss.",
        )

    def _common_toc_blacklist(self) -> list[re.Pattern[str]]:
        """Patterns of the entries that no restore takes from the TOC: excluded schemas, extensions and the like."""
        blacklist = schema_entry_patterns(self._excluded_schemas)

        if self._restored_schemas:
            blacklist.extend([re.compile(rf".*SCHEMA - {re.escape(schema)}") for schema in self._restored_schemas])

        for trigger_name in self._excluded_event_triggers():
            blacklist.append(re.compile(rf".*EVENT TRIGGER - {re.escape(trigger_name)}\b"))

        # Extensions are always saved in metadata (even for full dumps)
        # and created separately by _create_extensions.
        # Blacklist EXTENSION from TOC to avoid duplicates with metadata-based creation.
        if self.metadata.extensions:
            blacklist.append(EXTENSION_ENTRY_RE)

        if self.context.options.keep_fdw_user_mappings:
            self.context.logger.info("Keeping USER MAPPING in restore (--keep-fdw-user-mappings set)")
        else:
            blacklist.append(USER_MAPPING_ENTRY_RE)
            self.context.logger.info(
                "Removing USER MAPPING from restore by default "
                "(FDW remote-server credentials; restore also fails without OPTIONS)"
            )

        return blacklist

    def _make_filtered_toc_list(self) -> None:
        if self._is_legacy_dump:
            self._make_legacy_filtered_toc_list()
        else:
            self._make_filtered_toc_list_by_exclusions()

    def _toc_dependencies(self, section: str) -> tuple[dict[int, list[tuple[str, str]]], set[int]]:
        """Find the dump tables of each TOC entry of the section and the entries that the dump tells to skip."""
        if section not in self._toc_dependencies_cache:
            # the dump saves OIDs of the source objects, and the TOC keeps the same OIDs
            entries = read_toc_entries(self.context.pg_restore, self.input_dir / f"{section}.backup")
            self._toc_dependencies_cache[section] = map_toc_entries(
                entries, self.metadata.dependencies or {}, set(self.metadata.dump_dependents_to_skip or [])
            )
        return self._toc_dependencies_cache[section]

    def _make_filtered_toc_list_by_exclusions(self) -> None:
        """Cut from the TOC the excluded tables, everything that depends on them, and the dump's own skip list."""
        excluded = self._excluded_tables
        blacklist = self._common_toc_blacklist()
        blacklist.extend(self._not_valid_fk_patterns(excluded))
        lost: list[str] = []
        dropped_tables = set(excluded)
        for section in ["pre_data", "post_data"]:
            backup_path = self.input_dir / f"{section}.backup"
            if section == "pre_data":
                self._toc_list_pre_data_file_path = self.input_dir / self._toc_list_pre_data_file_name
                toc_file_path = self._toc_list_pre_data_file_path
            else:
                self._toc_list_post_data_file_path = self.input_dir / self._toc_list_post_data_file_name
                toc_file_path = self._toc_list_post_data_file_path

            entry_tables, skipped_ids = self._toc_dependencies(section)
            # names in the TOC text are not quoted, so the schema of an entry is asked from pg_restore
            schema_ids = (
                entry_ids(read_toc_lines(self.context.pg_restore, backup_path, schemas=sorted(self._excluded_schemas)))
                if self._excluded_schemas
                else set()
            )

            kept_lines: list[str] = []
            for toc_line in read_toc_lines(self.context.pg_restore, backup_path):
                entry = entry_id(toc_line)
                tables = entry_tables.get(entry, [])
                if entry in skipped_ids or any(table in excluded for table in tables):
                    if entry not in schema_ids:
                        self._note_lost_entry(toc_line, dropped_tables, lost)
                    self.context.logger.debug('TOC: Skip entry of excluded table - "%s" ', toc_line)
                    continue

                if entry in schema_ids or any(pattern.search(toc_line) for pattern in blacklist):
                    self.context.logger.debug('TOC: Skip by blacklist - "%s" ', toc_line)
                    continue

                # pg_dump before 11 writes SCHEMA public; --clean cannot drop it while extensions stay there
                if self.context.options.clean_db and PUBLIC_SCHEMA_ENTRY_RE.match(toc_line):
                    self.context.logger.debug('TOC: Skip the public schema, it is never dropped - "%s" ', toc_line)
                    continue

                kept_lines.append(toc_line)

            write_toc_list(toc_file_path, kept_lines)

        if lost:
            self.context.logger.info(
                "Objects that depend on %s and will not be restored: %s",
                "excluded tables and schemas" if self._excluded_schemas else "excluded tables",
                format_names_summary(lost, "object", limit=5),
            )

    @staticmethod
    def _note_lost_entry(toc_line: str, dropped_tables: set[tuple[str, str]], lost: list[str]) -> None:
        """Remember a skipped entry that is not a part of a skipped table, to name it in the log."""
        if not (title := entry_title(toc_line)):
            return
        desc, schema, name = title
        if is_table_entry(toc_line):
            dropped_tables.add((schema, name))
        elif desc in TABLE_LINK_DESCS:
            # the tag of a key or a trigger starts with the name of its own table
            if (schema, name.split(" ", 1)[0]) not in dropped_tables:
                lost.append(f"{desc.lower()} {name.split(' ', 1)[-1]} on {schema}.{name.split(' ', 1)[0]}")
        elif desc not in TABLE_PART_DESCS:
            lost.append(f"{desc.lower()} {schema}.{name}" if schema != "-" else f"{desc.lower()} {name}")

    def _not_valid_fk_patterns(self, excluded: set[tuple[str, str]]) -> list[re.Pattern[str]]:
        """Collect the foreign keys that pg_anon creates itself after post-data and return the patterns of their TOC entries."""
        patterns = []
        skipped_lines, dependent_lines = self._fk_lines_cut_from_toc(excluded)
        for constraint in (self.metadata.constraints or {}).values():
            ref_table = (constraint["table_schema_to"], constraint["table_name_to"])
            from_table = (constraint["table_schema_from"], constraint["table_name_from"])
            ref_leaves = [tuple(leaf) for leaf in constraint.get("referenced_partition_leaves") or [ref_table]]
            from_leaves = [tuple(leaf) for leaf in constraint.get("referrer_partition_leaves") or [from_table]]
            if ref_table in excluded or from_table in excluded:
                continue

            pattern = (
                rf".*FK CONSTRAINT {re.escape(constraint['table_schema_from'])} "
                rf"{re.escape(constraint['table_name_from'])} "
                rf"{re.escape(constraint['constraint_name'])}"
            )
            # the owner of the entry goes after the name of the key
            entry_pattern = re.compile(pattern + r"\s")
            skipped_by_dump = any(entry_pattern.match(line) for line in skipped_lines)
            cut_by_lists = any(entry_pattern.match(line) for line in dependent_lines)
            if self._fk_is_created_by_pg_anon(
                constraint, ref_leaves, from_leaves, excluded, skipped_by_dump, cut_by_lists
            ):
                self._not_valid_fk.append(constraint)
                patterns.append(re.compile(pattern))
        return patterns

    def _fk_lines_cut_from_toc(self, excluded: set[tuple[str, str]]) -> tuple[list[str], list[str]]:
        """Return the FK CONSTRAINT entries that the dump tells to skip and the ones that depend on excluded tables."""
        entry_tables, skipped_ids = self._toc_dependencies("post_data")
        skipped_lines, dependent_lines = [], []
        for toc_line in read_toc_lines(self.context.pg_restore, self.input_dir / "post_data.backup"):
            if " FK CONSTRAINT " not in toc_line:
                continue
            entry = entry_id(toc_line)
            if entry in skipped_ids:
                skipped_lines.append(toc_line)
            elif any(table in excluded for table in entry_tables.get(entry, [])):
                dependent_lines.append(toc_line)
        return skipped_lines, dependent_lines

    def _fk_is_created_by_pg_anon(
        self,
        constraint: dict,
        ref_leaves: list[tuple[str, str]],
        from_leaves: list[tuple[str, str]],
        excluded: set[tuple[str, str]],
        skipped_by_dump: bool,
        cut_by_lists: bool,
    ) -> bool:
        """Check that pg_restore may fail on the key: the referenced table lost a partition or has not all its rows."""
        if self._skip_post_data_restore or constraint.get("contype") != "f" or not constraint.get("definition"):
            return False

        if self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE:
            # the tables stay empty: pg_anon creates only the keys that pg_restore does not get
            return skipped_by_dump or cut_by_lists

        if not self._dumped_rows:
            return False

        # a structure dump has no mark for a key to a table with an excluded partition, only the skipped entry
        if constraint.get("partition_excluded") or skipped_by_dump or any(leaf in excluded for leaf in ref_leaves):
            return True

        referenced_incomplete = any(
            leaf not in self._dumped_rows or leaf in self._partial_data_tables for leaf in ref_leaves
        )
        referrer_with_rows = any(self._dumped_rows.get(leaf, 0) > 0 for leaf in from_leaves if leaf not in excluded)

        return referenced_incomplete and referrer_with_rows

    def _make_legacy_filtered_toc_list(self) -> None:  # noqa: C901, PLR0912, PLR0915
        whitelist: list[re.Pattern[str]] = []
        blacklist: list[re.Pattern[str]] = []

        for schema, table in self.context.black_listed_tables:
            # Make blacklist for TABLE, DEFAULT, TRIGGER, RULE
            blacklist.append(re.compile(rf".*(TABLE|DEFAULT|TRIGGER|RULE) {re.escape(schema)} {re.escape(table)}"))
            # Update blacklist for CONSTRAINT
            blacklist.append(re.compile(rf"(?<!FK )CONSTRAINT {re.escape(schema)} {re.escape(table)}"))

        for schema, table in self.context.white_listed_tables:
            # Make whitelist for TABLE, DEFAULT, CONSTRAINT, FK CONSTRAINT, TRIGGER, RULE
            whitelist.append(re.compile(rf".*(TABLE|DEFAULT|TRIGGER|RULE) {re.escape(schema)} {re.escape(table)}"))
            # Update whitelist for CONSTRAINT
            whitelist.append(re.compile(rf"(?<!FK )CONSTRAINT {re.escape(schema)} {re.escape(table)}"))

        if self.metadata.sequences_last_values:
            for seq in self.metadata.sequences_last_values.values():
                # Update blacklist for SEQUENCE, SEQUENCE OWNED BY
                if seq["is_excluded"] or (seq["schema"], seq["table"]) in self.context.black_listed_tables:
                    blacklist.append(
                        re.compile(rf".*SEQUENCE.*{re.escape(seq['schema'])} {re.escape(seq['seq_name'])}")
                    )

                # Update whitelist for SEQUENCE, SEQUENCE OWNED BY
                if not seq["is_excluded"] and (seq["schema"], seq["table"]) in self.context.white_listed_tables:
                    whitelist.append(
                        re.compile(rf".*SEQUENCE.*{re.escape(seq['schema'])} {re.escape(seq['seq_name'])}")
                    )

        if self.metadata.indexes:
            indexes_values = list(self.metadata.indexes.values())

            for index in indexes_values:
                # Update blacklist for INDEX
                if index["is_excluded"] or (index["schema"], index["table"]) in self.context.black_listed_tables:
                    blacklist.append(
                        re.compile(rf".*INDEX {re.escape(index['schema'])} {re.escape(index['index_name'])}")
                    )

                # Update whitelist for INDEX
                if not index["is_excluded"] and (index["schema"], index["table"]) in self.context.white_listed_tables:
                    whitelist.append(
                        re.compile(rf".*INDEX {re.escape(index['schema'])} {re.escape(index['index_name'])}")
                    )

            excluded_index_keys: set[tuple[str, str]] = {
                (idx["schema"], idx["index_name"])
                for idx in indexes_values
                if idx["is_excluded"] or (idx["schema"], idx["table"]) in self.context.black_listed_tables
            }
            for idx in indexes_values:
                parent_schema = idx.get("parent_index_schema")
                parent_name = idx.get("parent_index_name")
                if not parent_schema or not parent_name:
                    continue
                if (parent_schema, parent_name) in excluded_index_keys:
                    blacklist.append(
                        re.compile(
                            rf".*INDEX ATTACH {re.escape(idx['schema'])} "
                            rf"{re.escape(idx['index_name'])}"
                        )
                    )

        if self.metadata.views:
            for view in self.metadata.views.values():
                # Update blacklist for VIEW, MATERIALIZED VIEW
                if (
                    view["is_excluded"]
                    or (view["table_schema"], view["table_name"]) in self.context.black_listed_tables
                    or view["table_schema"] in self._excluded_schemas
                ):
                    blacklist.append(
                        re.compile(rf".*VIEW {re.escape(view['view_schema'])} {re.escape(view['view_name'])}")
                    )

                # Update whitelist for VIEW, MATERIALIZED VIEW
                if (
                    not view["is_excluded"]
                    and (view["table_schema"], view["table_name"]) in self.context.white_listed_tables
                ):
                    whitelist.append(
                        re.compile(rf".*VIEW {re.escape(view['view_schema'])} {re.escape(view['view_name'])}")
                    )

        if self.metadata.constraints:
            for constraint in self.metadata.constraints.values():
                constraint_table_to = (constraint["table_schema_to"], constraint["table_name_to"])
                constraint_table_from = (constraint["table_schema_from"], constraint["table_name_from"])

                ref_leaves_raw = constraint.get("referenced_partition_leaves") or [list(constraint_table_to)]
                from_leaves_raw = constraint.get("referrer_partition_leaves") or [list(constraint_table_from)]
                ref_leaves = [tuple(x) for x in ref_leaves_raw]
                from_leaves = [tuple(x) for x in from_leaves_raw]
                all_leaves = ref_leaves + from_leaves

                in_black = any(leaf in self.context.black_listed_tables for leaf in all_leaves)
                all_in_white = all(leaf in self.context.white_listed_tables for leaf in all_leaves)
                fully_dumped = not constraint.get("is_excluded", False)

                fk_regex = re.compile(
                    rf".*FK CONSTRAINT {re.escape(constraint['table_schema_from'])} "
                    rf"{re.escape(constraint['table_name_from'])} "
                    rf"{re.escape(constraint['constraint_name'])}"
                )

                in_excluded_schema = any(schema in self._excluded_schemas for schema, _ in all_leaves)
                if not fully_dumped or in_black or in_excluded_schema:
                    blacklist.append(fk_regex)
                    continue

                if self._whitelist_active and not all_in_white:
                    blacklist.append(fk_regex)
                    continue

                # created later as NOT VALID, so pg_restore must not create it
                if self._fk_needs_not_valid(constraint, ref_leaves, from_leaves):
                    blacklist.append(fk_regex)
                    self._not_valid_fk.append(constraint)
                    continue

                if self._whitelist_active:
                    whitelist.append(fk_regex)

        blacklist.extend(self._common_toc_blacklist())

        # backward compatibility: partial dumps of 1.12.0 and older keep these objects as DDL, not in the TOC
        has_custom_ddl = self._is_legacy_dump and bool(
            self.metadata.partial_dump_types
            or self.metadata.partial_dump_domains
            or self.metadata.partial_dump_ranges
            or self.metadata.partial_dump_functions
            or self.metadata.partial_dump_casts
            or self.metadata.partial_dump_operators
            or self.metadata.partial_dump_aggregates
        )
        if has_custom_ddl:
            blacklist.append(CUSTOM_OBJECTS_ENTRY_RE)

        for section in ["pre_data", "post_data"]:
            backup_path = self.input_dir / f"{section}.backup"
            if section == "pre_data":
                self._toc_list_pre_data_file_path = self.input_dir / self._toc_list_pre_data_file_name
                toc_file_path = self._toc_list_pre_data_file_path
            else:
                self._toc_list_post_data_file_path = self.input_dir / self._toc_list_post_data_file_name
                toc_file_path = self._toc_list_post_data_file_path

            # names in the TOC text are not quoted, so the schema of an entry is asked from pg_restore
            excluded_entry_ids = (
                entry_ids(read_toc_lines(self.context.pg_restore, backup_path, schemas=sorted(self._excluded_schemas)))
                if self._excluded_schemas
                else set()
            )

            kept_lines: list[str] = []
            for toc_line in read_toc_lines(self.context.pg_restore, backup_path):
                if entry_id(toc_line) in excluded_entry_ids:
                    self.context.logger.debug('TOC: Skip entry of excluded schema - "%s" ', toc_line)
                    continue

                is_blacklisted = blacklist and any(p.search(toc_line) for p in blacklist)
                if is_blacklisted:
                    self.context.logger.debug('TOC: Skip by blacklist - "%s" ', toc_line)
                    continue

                is_skipped_by_whitelist = self._whitelist_active and not any(p.search(toc_line) for p in whitelist)
                is_allowed_custom_object = (
                    not has_custom_ddl and CUSTOM_OBJECTS_ENTRY_RE.match(toc_line)
                ) or OPERATOR_CLASS_ENTRY_RE.match(toc_line)
                if is_skipped_by_whitelist and not is_allowed_custom_object:
                    self.context.logger.debug('PARTIAL RESTORE MODE. TOC: Skip by whitelist - "%s" ', toc_line)
                    continue

                kept_lines.append(toc_line)

            write_toc_list(toc_file_path, kept_lines)

    def _remove_toc_lists(self) -> None:
        if self.context.options.debug:
            return

        if self._toc_list_pre_data_file_path:
            self._toc_list_pre_data_file_path.unlink(missing_ok=True)
        if self._toc_list_post_data_file_path:
            self._toc_list_post_data_file_path.unlink(missing_ok=True)
        if self._toc_list_post_data_head_file_path:
            self._toc_list_post_data_head_file_path.unlink(missing_ok=True)
        if self._toc_list_post_data_tail_file_path:
            self._toc_list_post_data_tail_file_path.unlink(missing_ok=True)

    def _make_pg_restore_env(self) -> dict[str, str]:
        # LC_ALL=C is required so we can parse "from TOC entry NNN" markers from stderr
        # regardless of the system locale (default would localize pg_restore messages).
        env = build_pg_util_env(self.context.options)
        env["LC_ALL"] = "C"
        env["LC_MESSAGES"] = "C"
        return env

    def _build_pg_restore_command(
        self,
        section: str,
        parallelism: int,
        toc_override: Path | None = None,
    ) -> list[str]:
        command: list[str] = [
            self.context.pg_restore,
            "-h",
            self.context.options.db_host,
            "-p",
            str(self.context.options.db_port),
            "-v",
            "-w",
            "-U",
            self.context.options.db_user,
            "-d",
            self.context.options.db_name,
            "-j",
            str(parallelism),
            "--no-owner",
            str((self.input_dir / section.replace("-", "_")).with_suffix(".backup")),
        ]
        if not self.context.options.db_host:
            del command[command.index("-h") : command.index("-h") + 2]

        if not self.context.options.db_user:
            del command[command.index("-U") : command.index("-U") + 2]

        if self.context.options.clean_db:
            command.extend(["--clean", "--if-exists"])

        if self.context.options.ignore_privileges:
            command.append("--no-privileges")

        if toc_override is not None:
            command.extend(["-L", str(toc_override)])
        elif self._toc_list_pre_data_file_path:
            toc_path = (
                self._toc_list_pre_data_file_path if section == "pre-data" else self._toc_list_post_data_file_path
            )
            if toc_path:
                command.extend(["-L", str(toc_path)])

        if self.context.options.pg_restore_options:
            command.extend(shlex.split(self.context.options.pg_restore_options))

        return command

    def _invoke_pg_restore(self, command: list[str]) -> tuple[str, int]:
        env = self._make_pg_restore_env()
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        # pg_restore writes logs to stderr; stdout stays empty unless -f is used
        _, stderr_bytes = proc.communicate()
        return stderr_bytes.decode("utf-8", errors="replace"), proc.returncode

    def _build_retry_toc(self, section: str, failed_ids: set[int]) -> Path | None:
        # lines come from pg_restore -l, so the -L file keeps the canonical form
        backup_path = (self.input_dir / section.replace("-", "_")).with_suffix(".backup")
        toc_lines = read_toc_lines(self.context.pg_restore, backup_path, env=self._make_pg_restore_env())
        failed_lines = [line for line in toc_lines if entry_id(line) in failed_ids]
        if not failed_lines:
            return None

        return write_toc_list(self.input_dir / f"toc_{section.replace('-', '_')}_retry.list", failed_lines)

    def _split_post_data_segments(self, section: str) -> list[tuple[Path | None, int]]:
        """Split post-data into a parallel head and a serial tail starting at the first INDEX ATTACH.

        Attaching partitioned indexes concurrently can leave the parent index invalid, so
        everything from the first INDEX ATTACH onwards runs single-threaded.
        """
        parallelism = self.context.options.db_connections_per_process
        if section != "post-data":
            return [(None, parallelism)]

        toc_path = self._toc_list_post_data_file_path
        if toc_path is None or not Path(toc_path).exists():
            return [(None, parallelism)]

        lines = Path(toc_path).read_text(encoding="utf-8").splitlines()
        first = next((i for i, line in enumerate(lines) if "INDEX ATTACH" in line), None)
        if first is None or first == 0:
            return [(None, parallelism)]

        head_lines, tail_lines = lines[:first], lines[first:]
        if len(head_lines) + len(tail_lines) != len(lines):
            self.context.logger.warning("TOC split lost lines, falling back to a single pass")
            return [(None, parallelism)]

        self._toc_list_post_data_head_file_path = write_toc_list(
            self.input_dir / self._toc_list_post_data_head_file_name, head_lines
        )
        self._toc_list_post_data_tail_file_path = write_toc_list(
            self.input_dir / self._toc_list_post_data_tail_file_name, tail_lines
        )

        self.context.logger.info(
            "post-data split: head %d lines (-j %d), tail %d lines (-j 1)",
            len(head_lines),
            parallelism,
            len(tail_lines),
        )
        return [(self._toc_list_post_data_head_file_path, parallelism), (self._toc_list_post_data_tail_file_path, 1)]

    async def _run_pg_restore(self, section: str) -> None:
        for toc_override, parallelism in self._split_post_data_segments(section):
            await self._run_pg_restore_segment(section, parallelism, toc_override)

    async def _run_pg_restore_segment(self, section: str, parallelism: int, toc_override: Path | None = None) -> None:
        command = self._build_pg_restore_command(section, parallelism=parallelism, toc_override=toc_override)

        stderr_text, returncode = self._invoke_pg_restore(command)

        for log_line in stderr_text.split("\n"):
            self.context.logger.info(log_line)

        if returncode == 0:
            return

        failed_ids = failed_entry_ids(stderr_text)
        if not failed_ids:
            msg = "ERROR: database restore has failed!"
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.RESTORE_FAILED, msg)

        self.context.logger.warning(
            "Parallel %s restore reported %d failed TOC entries: %s. Retrying serially.",
            section,
            len(failed_ids),
            sorted(failed_ids),
        )

        retry_toc = self._build_retry_toc(section, failed_ids)
        if retry_toc is None:
            msg = "ERROR: database restore has failed; could not build retry TOC list!"
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.RESTORE_FAILED, msg)

        retry_command = self._build_pg_restore_command(
            section,
            parallelism=1,
            toc_override=retry_toc,
        )
        stderr_text, returncode = self._invoke_pg_restore(retry_command)

        for log_line in stderr_text.split("\n"):
            self.context.logger.info(log_line)

        if returncode != 0:
            retry_failed_ids = failed_entry_ids(stderr_text)
            msg = (
                "ERROR: database restore has failed! Serial retry could not recover "
                f"{len(retry_failed_ids)} TOC entries: {sorted(retry_failed_ids)}"
            )
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.RESTORE_FAILED, msg)

        self.context.logger.info(
            "Serial retry of %d TOC entries succeeded for %s.",
            len(failed_ids),
            section,
        )

    async def _sequences_init(self, connection: Connection) -> None:
        if self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE:
            return

        if self.context.options.seq_init_by_max_value:
            query = get_sequences_max_value_init_query()
            self.context.logger.debug(query)
            await connection.execute(query)
        else:
            target_sequences = {
                (row[0], row[1])
                for row in await connection.fetch(
                    "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relkind = 'S'"
                )
            }
            for sequence_data in (self.metadata.sequences_last_values or {}).values():
                if sequence_data.get("is_excluded"):
                    continue

                if not self._is_table_restored(sequence_data["schema"], sequence_data["table"]):
                    continue

                if (sequence_data["schema"], sequence_data["seq_name"]) not in target_sequences:
                    # the structure of the dump can come without this sequence
                    self.context.logger.warning(
                        "Sequence %s.%s is not in the target database, its value is not set",
                        sequence_data["schema"],
                        sequence_data["seq_name"],
                    )
                    continue

                schema = sequence_data["schema"].replace("'", "''")
                sequence_name = sequence_data["seq_name"].replace("'", "''")
                value = sequence_data["value"]
                query = f"SELECT setval(quote_ident('{schema}') || '.' || quote_ident('{sequence_name}'), {value} + 1);"
                self.context.logger.info(query)
                await connection.execute(query)

    def _extract_schemas_from_toc(self) -> set[str]:
        """Read the schemas the dump creates from the TOC of pre_data, for dumps that have no schemas in metadata."""
        return schemas_in_toc(read_toc_lines(self.context.pg_restore, self.input_dir / "pre_data.backup"))

    async def _create_schemas(self, connection: Connection) -> None:
        self._restored_schemas = []
        if not self._is_legacy_dump:
            # the TOC is not filtered by a whitelist, so pg_restore creates the schemas itself
            return

        if self.metadata.partial_dump_schemas:
            self._restored_schemas = copy(self.metadata.partial_dump_schemas)
        elif self.context.black_listed_tables or self._whitelist_active:
            table_schemas = {schema for schema, _ in self.context.tables}
            toc_schemas = self._extract_schemas_from_toc()
            self._restored_schemas = list(table_schemas | toc_schemas)

        self._restored_schemas = [schema for schema in self._restored_schemas if schema not in self._excluded_schemas]
        if not self._restored_schemas:
            return

        for schema in self._restored_schemas:
            query = f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
            self.context.logger.info(query)
            await connection.execute(query)

    async def _create_extensions(self, connection: Connection) -> None:
        """Create extensions from metadata, each in the schema it had in the source."""
        if not self.metadata.extensions:
            return

        self._metadata_extensions = self.metadata.extensions
        self._available_extensions = await get_available_extensions_map(connection)
        self._available_schemas = await get_available_schemas(connection)
        self._installed_extensions = {row["name"]: row["schema"] for row in await get_extensions(connection)}
        self._created_extensions = set()

        for extension_name in self._metadata_extensions:
            await self._create_extension(connection, extension_name)

    async def _create_extension(  # noqa: C901
        self, connection: Connection, extension_name: str, chain: tuple[str, ...] = ()
    ) -> None:
        if extension_name in self._created_extensions:
            return

        if extension_name in chain:
            raise PgAnonError(
                ErrorCode.EXTENSION_ERROR,
                f"Circular dependency between extensions: {' -> '.join([*chain, extension_name])}",
            )

        available_extension = self._available_extensions.get(extension_name)
        if not available_extension:
            raise PgAnonError(
                ErrorCode.EXTENSION_ERROR, f'Required EXTENSION "{extension_name}" is not available for creating'
            )

        # a dependency of a newer target version may be missing from the source metadata
        extension_data = self._metadata_extensions.get(extension_name, {})
        extension_schema = extension_data.get("schema")
        source_version = extension_data.get("version")

        installed_schema = self._installed_extensions.get(extension_name)
        if installed_schema is not None:
            if extension_schema and installed_schema != extension_schema:
                self.context.logger.warning(
                    'EXTENSION "%s" is already installed in SCHEMA "%s", but in the source it lived in SCHEMA "%s". '
                    "Objects referencing it by the source schema will fail to restore",
                    extension_name,
                    installed_schema,
                    extension_schema,
                )
            self._created_extensions.add(extension_name)
            return

        available_versions = available_extension["versions"]
        target_version = source_version if source_version in available_versions else None

        if target_version is None and source_version:
            self.context.logger.warning(
                'EXTENSION "%s" will be restored with the version defaulted by the target server, '
                'cause source version "%s" is not available there',
                extension_name,
                source_version,
            )

        requires_version = target_version or available_extension["default_version"]
        for required_extension in available_versions.get(requires_version, []):
            await self._create_extension(connection, required_extension, (*chain, extension_name))

        query_parts = [f'CREATE EXTENSION IF NOT EXISTS "{extension_name}"']

        if extension_schema:
            if extension_data.get("is_excluded_by_schema"):
                self.context.logger.warning(
                    'SCHEMA "%s" is excluded from the dump, but EXTENSION "%s" lives in it. The schema is created '
                    "on the target to install the extension. User objects and data of that schema are not restored, "
                    "they were never dumped. Drop the extension and the schema manually if they are not needed",
                    extension_schema,
                    extension_name,
                )
            await self._create_extension_schema(connection, extension_schema)
            query_parts.append(f'SCHEMA "{extension_schema}"')

        if target_version:
            query_parts.append(f"VERSION '{target_version}'")

        query = " ".join(query_parts)
        self.context.logger.info(query)
        try:
            await connection.execute(query)
        except asyncpg.exceptions.InsufficientPrivilegeError as ex:
            raise PgAnonError(
                ErrorCode.EXTENSION_ERROR,
                f'Not enough privileges to create EXTENSION "{extension_name}" in SCHEMA "{extension_schema}". '
                f"Before PostgreSQL 13 every extension requires a superuser, since 13 only trusted ones "
                f"can be created by the database owner: {ex}",
            ) from ex

        self._created_extensions.add(extension_name)

    async def _create_extension_schema(self, connection: Connection, schema: str) -> None:
        if schema in self._available_schemas:
            return

        # pg_ prefix is reserved: CREATE SCHEMA fails and such schemas always exist
        if schema.startswith("pg_"):
            return

        query = f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
        self.context.logger.info(query)
        await connection.execute(query)
        self._available_schemas.append(schema)

        if schema not in self._restored_schemas:
            self._restored_schemas.append(schema)

    async def _create_objects_from_ddl(self, connection: Connection) -> None:  # noqa: C901
        """Create custom objects from the DDL of a 1.12.0 or older partial dump, newer dumps have them in the TOC."""
        if not self._is_legacy_dump:
            return

        ddl_list = []

        if self.metadata.partial_dump_types:
            ddl_list.extend(self.metadata.partial_dump_types)

        if self.metadata.partial_dump_domains:
            ddl_list.extend(self.metadata.partial_dump_domains)

        if self.metadata.partial_dump_ranges:
            ddl_list.extend(self.metadata.partial_dump_ranges)

        if self.metadata.partial_dump_functions:
            ddl_list.extend(self.metadata.partial_dump_functions)

        if self.metadata.partial_dump_casts:
            ddl_list.extend(self.metadata.partial_dump_casts)

        if self.metadata.partial_dump_operators:
            ddl_list.extend(self.metadata.partial_dump_operators)

        if self.metadata.partial_dump_aggregates:
            ddl_list.extend(self.metadata.partial_dump_aggregates)

        remaining = [query for query in ddl_list if not self._ddl_is_excluded_by_schema(query)]

        # tables are created later by pg_restore, so function bodies must not be checked yet (as pg_dump does)
        await connection.execute("SET check_function_bodies = off")
        try:
            await self._execute_ddl_with_retries(connection, remaining)
        finally:
            await connection.execute("RESET check_function_bodies")

    async def _execute_ddl_with_retries(self, connection: Connection, remaining: list[str]) -> None:
        """Run DDL statements again and again while at least one of them succeeds on each pass."""
        while remaining:
            failed = []
            for query in remaining:
                try:
                    self.context.logger.info("PARTIAL RESTORE MODE: %s", query)
                    await connection.execute(query)
                except Exception as ex:
                    self.context.logger.warning("PARTIAL RESTORE MODE: DDL failed, will retry: %s", ex)
                    failed.append((query, ex))

            if not failed:
                break

            if len(failed) == len(remaining):
                error_details = "; ".join(f"{ex}" for _, ex in failed)
                raise PgAnonError(
                    ErrorCode.RESTORE_FAILED, f"Failed to execute {len(failed)} DDL statement(s): {error_details}"
                )

            remaining = [query for query, _ in failed]
            self.context.logger.info("PARTIAL RESTORE MODE: Retrying %s failed DDL(s)", len(remaining))

    def _fk_needs_not_valid(
        self,
        constraint: dict,
        ref_leaves: list[tuple[str, str]],
        from_leaves: list[tuple[str, str]],
    ) -> bool:
        """Check that the key cannot be validated: the dump has no data for the referenced table."""
        # a table with excluded data has no entry in "files", a table dumped with zero rows has one
        if self._skip_post_data_restore or constraint.get("contype") != "f" or not constraint.get("definition"):
            return False

        if not self._dumped_rows:
            return False

        referenced_without_data = any(leaf not in self._dumped_rows for leaf in ref_leaves)
        referrer_with_rows = any(self._dumped_rows.get(leaf, 0) > 0 for leaf in from_leaves)

        return referenced_without_data and referrer_with_rows

    async def _try_query(self, connection: Connection, query: str) -> bool:
        """Run the query and tell if the server accepted it."""
        self.context.logger.info(query)
        try:
            await connection.execute(query)
        except Exception as exc:  # noqa: BLE001
            self.context.logger.info("The server did not accept the query: %s", exc)
            return False
        return True

    async def _create_not_valid_fk(self, connection: Connection) -> None:
        """Create the collected foreign keys: in the usual way, then as NOT VALID if the usual way fails."""
        if not self._not_valid_fk:
            return

        target_tables = await self._fetch_target_tables(connection)
        not_valid_count = 0
        # sync-struct-restore loads no rows, so the usual way cannot fail on the rows
        struct_only = self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE

        for constraint in self._not_valid_fk:
            schema_from, table_from = constraint["table_schema_from"], constraint["table_name_from"]
            schema_to, table_to = constraint["table_schema_to"], constraint["table_name_to"]
            if (schema_from, table_from) not in target_tables or (schema_to, table_to) not in target_tables:
                continue

            name = constraint["constraint_name"]
            add_query = f'ALTER TABLE "{schema_from}"."{table_from}" ADD CONSTRAINT "{name}" {constraint["definition"]}'
            key = f"{schema_from}.{name} from {schema_from}.{table_from} to {schema_to}.{table_to}"

            if struct_only:
                await self._create_fk_on_empty_tables(connection, add_query, key)
                continue

            if await self._try_query(connection, add_query):
                continue

            if await self._try_query(connection, f"{add_query} NOT VALID"):
                not_valid_count += 1
                self.context.logger.warning(
                    "Foreign key %s is created as NOT VALID: the dump does not have all rows of the referenced table.",
                    key,
                )
                continue

            self.context.logger.warning(
                "Foreign key %s is not created: the server did not accept it, also as NOT VALID. "
                "Make the rows of these two tables match, then create the key.",
                key,
            )

        if not_valid_count:
            keys = "1 foreign key" if not_valid_count == 1 else f"{not_valid_count} foreign keys"
            self.context.logger.warning(
                "%s created as NOT VALID. Load the missing rows, then run for each of them: "
                "ALTER TABLE ... VALIDATE CONSTRAINT ...",
                keys,
            )

    async def _create_fk_on_empty_tables(self, connection: Connection, add_query: str, key: str) -> None:
        """Create a foreign key in the usual way and stop when the server does not accept it."""
        self.context.logger.info(add_query)
        try:
            await connection.execute(add_query)
        except Exception as exc:
            raise PgAnonError(ErrorCode.RESTORE_FAILED, f"Can't create foreign key {key}") from exc

    async def _warn_unchecked_fk(self, connection: Connection) -> None:
        """Tell how many foreign keys of the loaded tables sync-data-restore left without a new check."""
        if self.context.options.mode != AnonMode.SYNC_DATA_RESTORE or not self._tables_with_new_rows:
            return

        count = await count_foreign_keys_of_tables(connection, sorted(self._tables_with_new_rows))
        if count:
            keys = "foreign key on the loaded tables was" if count == 1 else "foreign keys on the loaded tables were"
            self.context.logger.warning(
                "Data was loaded with foreign key checks turned off. %s %s not checked again.", count, keys
            )

    async def _drop_constraints(self, connection: Connection) -> None:
        """Drop all CHECK constraints containing user-defined procedures."""
        if not self.context.options.drop_custom_check_constr:
            return

        check_constraints = await connection.fetch(get_check_constraint_query())

        if not check_constraints:
            return

        for check_constraint in check_constraints:
            schema = check_constraint[0]
            table = check_constraint[1]
            constraint = check_constraint[2]

            self.context.logger.info("Removing constraints: %s.%s -> %s", schema, table, constraint)
            query = f'ALTER TABLE "{schema}"."{table}" DROP CONSTRAINT IF EXISTS "{constraint}" CASCADE'
            await connection.execute(query)

    async def _restore_table_data(
        self,
        pool: asyncpg.Pool,
        dump_file: Path,
        schema_name: str,
        table_name: str,
        transaction_snapshot_id: str,
    ) -> None:
        self.context.logger.info("%s Started task copy_to_table %s.%s", ">" * 20, schema_name, table_name)

        try:
            async with pool.acquire() as connection:
                async with connection.transaction(isolation="repeatable_read"):
                    await connection.execute(f"SET TRANSACTION SNAPSHOT '{transaction_snapshot_id}';")
                    await connection.execute("SET LOCAL session_replication_role = 'replica';")

                    with gzip.open(dump_file, "rb") as source_file:
                        result = await connection.copy_to_table(
                            schema_name=schema_name,
                            table_name=table_name,
                            source=source_file,
                            format="binary",
                        )

                    loaded_rows = int(re.findall(r"(\d+)", result)[0])
                    self.context.total_rows += loaded_rows
                    if loaded_rows:
                        self._tables_with_new_rows.add((schema_name, table_name))
                    await connection.execute("COMMIT;")
        except Exception as exc:
            self.context.logger.exception(
                "Exception in RestoreMode._restore_table_data: schema_name=%s table_name=%s dump_file=%s",
                schema_name,
                table_name,
                dump_file,
            )
            raise PgAnonError(ErrorCode.RESTORE_FAILED, f"Can't restore data into {schema_name}.{table_name}") from exc

        self.context.logger.info("%s Finished task %s.%s", ">" * 20, schema_name, table_name)

    @staticmethod
    def _raise_first_task_exception(done: set[asyncio.Task], pending: set[asyncio.Task]) -> None:
        """Re-raise the first task failure, cancelling the tasks still in flight."""
        for task in done:
            exception = task.exception()
            if exception is None:
                continue

            for pending_task in pending:
                pending_task.cancel()
            raise exception

    async def _process_restore_data(self, transaction_snapshot_id: str) -> None:
        pool = await create_pool(
            connection_params=self.context.connection_params,
            server_settings=self.context.server_settings,
            min_size=self.context.options.db_connections_per_process,
            max_size=self.context.options.db_connections_per_process,
        )

        try:
            loop = asyncio.get_event_loop()
            tasks: set[asyncio.Task[None]] = set()
            for file_name, target in (self.metadata.files or {}).items():
                if not self._is_table_restored(target["schema"], target["table"]):
                    self.context.logger.info(
                        "Skipping restore data of table: %s", f'"{target["schema"]}"."{target["table"]}"'
                    )
                    continue

                full_path = self.input_dir / file_name
                if len(tasks) >= self.context.options.db_connections_per_process:
                    # Wait for some restore to finish before adding a new one
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    self._raise_first_task_exception(done, pending=tasks)
                tasks.add(
                    loop.create_task(
                        self._restore_table_data(
                            pool=pool,
                            dump_file=full_path,
                            schema_name=target["schema"],
                            table_name=target["table"],
                            transaction_snapshot_id=transaction_snapshot_id,
                        )
                    )
                )

            # Wait for the remaining restores to finish
            while tasks:
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                self._raise_first_task_exception(done, pending=tasks)
        finally:
            await pool.close()

    async def _restore_data(self, connection: Connection) -> None:
        if self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE:
            return

        async with connection.transaction(isolation="repeatable_read"):
            await connection.execute("SET CONSTRAINTS ALL DEFERRED;")
            transaction_snapshot_id = await connection.fetchval("select pg_export_snapshot()")
            await self._process_restore_data(transaction_snapshot_id)

        self._compare_rows_count()

    def _compare_rows_count(self) -> None:
        if self.context.black_listed_tables or self._whitelist_active or self._excluded_schemas:
            dumped_rows = sum(
                int(table_data["rows"])
                for table_data in (self.metadata.files or {}).values()
                if self._is_table_restored(table_data["schema"], table_data["table"])
            )
        else:
            dumped_rows = int(self.metadata.total_rows or 0)

        restored_rows = self.context.total_rows
        if restored_rows != dumped_rows:
            raise PgAnonError(
                ErrorCode.ROW_COUNT_MISMATCH,
                f"The number of restored rows ({restored_rows}) is different from the metadata ({dumped_rows})",
            )

    async def _restore_pre_data(self) -> None:
        if self._skip_pre_data_restore:
            self.context.logger.info("-------------> Skipped restore pre-data (pg_restore)")
            return

        self.context.logger.info("-------------> Started restore pre-data (pg_restore)")
        await self._run_pg_restore("pre-data")
        self.context.logger.info("<------------- Finished restore pre-data (pg_restore)")

    async def _restore_post_data(self) -> None:
        if self._skip_post_data_restore:
            self.context.logger.info("-------------> Skipped restore post-data (pg_restore)")
            return

        self.context.logger.info("-------------> Started restore post-data (pg_restore)")
        await self._run_pg_restore("post-data")
        self.context.logger.info("<------------- Finished restore post-data (pg_restore)")

    async def _check_partitioned_indexes(self, connection: Connection) -> None:
        """Fail when the restore left a partitioned index invalid."""
        invalid_indexes = await get_invalid_partitioned_indexes(connection)
        if not invalid_indexes:
            return

        msg = (
            f"restore left {len(invalid_indexes)} invalid partitioned index(es): "
            f"{', '.join(invalid_indexes)}. They are ignored by the planner and cannot be "
            f"referenced by foreign keys."
        )
        self.context.logger.error(msg)
        raise PgAnonError(ErrorCode.RESTORE_FAILED, msg)

    async def _drop_database(self) -> None:
        if not self.context.options.drop_db:
            return

        connection_params = copy(self.context.connection_params)
        connection_params.database = "template1"
        connection = await create_connection(connection_params, server_settings=self.context.server_settings)
        db_params = await connection.fetchrow(get_db_params(self.context.options.db_name))

        await connection.execute(
            f"""
            DROP DATABASE "{self.context.options.db_name}";
            """
        )

        await connection.execute(
            f"""
            CREATE DATABASE "{self.context.options.db_name}"
            WITH TEMPLATE template0
                 OWNER "{db_params[1]}"
                 ENCODING '{db_params[2]}'
                 LC_COLLATE '{db_params[3]}'
                 LC_CTYPE '{db_params[4]}';
            """
        )

        await connection.close()

    def _tables_needed_by_tables(self) -> dict[tuple[str, str], set[tuple[str, str]]]:
        """Return the tables that the structure files create, each with the dump tables it cannot exist without."""
        if self._needed_tables is None:
            entry_tables, _ = self._toc_dependencies("pre_data")
            self._needed_tables = {}
            for toc_line in read_toc_lines(self.context.pg_restore, self.input_dir / "pre_data.backup"):
                tables = entry_tables.get(entry_id(toc_line), [])
                # the table of a TABLE entry goes first in its list
                if is_table_entry(toc_line) and tables:
                    self._needed_tables[tables[0]] = set(tables[1:])
        return self._needed_tables

    def _prepare_tables_lists(self) -> None:
        struct_only = self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE
        if struct_only and self._is_legacy_dump:
            return

        if struct_only:
            # this mode does not read data files: the lists are matched with the tables of the structure
            tables = sorted(self._tables_needed_by_tables())
        else:
            tables = [(info["schema"], info["table"]) for info in (self.metadata.files or {}).values()]
        # structure.json describes the parents of the structure tables, metadata.json those of the data tables
        described = [
            *(self.metadata.partitions or []),
            *(self.metadata.inherits or []),
            *(self.metadata.files or {}).values(),
        ]
        self._table_ancestors = {
            (info["schema"], info["table"]): [(ancestor[0], ancestor[1]) for ancestor in info["ancestors"]]
            for info in described
            if info.get("ancestors")
        }
        self.context.set_tables_lists(tables, self._table_ancestors)

        if self._whitelist_active and not self.context.white_listed_tables:
            raise PgAnonError(ErrorCode.NO_TABLES_FOR_RESTORE, "None of the requested tables match the dump contents")

    def _prepare_excluded_tables(self) -> None:
        """Turn the partial lists and the schema options into excluded tables; stop when a kept table needs one."""
        if self._is_legacy_dump:
            return

        needed_tables = self._tables_needed_by_tables()
        # sync-struct-restore loads no rows, so the data files of a full dump do not take part
        struct_only = self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE
        data_tables = set() if struct_only else set(self._dumped_rows)
        self._structure_tables = set(needed_tables)
        self._dump_tables = data_tables | self._structure_tables
        ancestors = self._table_ancestors
        white_rules, black_rules = self.context.included_tables_rules, self.context.excluded_tables_rules
        # tables without rows of their own follow the lists too; an entry for a parent table covers its child tables
        black_listed = {table for table in self._dump_tables if table_matches_rules(black_rules, table, ancestors)}
        kept = self._dump_tables - black_listed
        if self._whitelist_active:
            kept = {table for table in kept if table_matches_rules(white_rules, table, ancestors)}
            # a partition or a child table cannot be created without its parents; the parents get no rows
            parents = {parent for table in kept for parent in ancestors.get(table, [])} & self._dump_tables
            self._tables_without_rows = (parents - kept) & data_tables
            kept |= parents

        # a rule for a parent table without its own data file covers its child tables too
        rule_tables = self._dump_tables | {parent for table in self._dump_tables for parent in ancestors.get(table, [])}
        warn_rules_without_tables(black_rules, rule_tables, "Exclude list", "the dump")
        warn_rules_without_tables(white_rules, rule_tables, "Include list", "the dump")
        log_rules_for_parent_tables(black_rules, self._dump_tables, ancestors, "excluded")
        log_rules_for_parent_tables(white_rules, self._dump_tables - black_listed, ancestors, "included")

        kept = {table for table in kept if table[0] not in self._excluded_schemas}
        self._excluded_tables = self._dump_tables - kept
        for table in self._tables_without_rows:
            # foreign keys are handled as if the dump had no rows of these tables
            self._dumped_rows.pop(table, None)
            self._partial_data_tables.discard(table)

        broken = {table: needed & self._excluded_tables for table, needed in needed_tables.items() if table in kept}
        broken = {table: needed for table, needed in broken.items() if needed}
        if not broken:
            if white_rules or black_rules or self._excluded_schemas:
                self.context.logger.info("Restore includes %s of %s tables", len(kept), len(self._dump_tables))
            return

        only_schemas = all(table[0] in self._excluded_schemas for needed in broken.values() for table in needed)
        raise dependency_error(
            ErrorCode.EXCLUDED_SCHEMA_DEPENDENCY if only_schemas else ErrorCode.EXCLUDED_TABLE_DEPENDENCY,
            [f"{schema}.{table}" for schema, table in broken],
        )

    def _save_input_dicts_to_run_dir(self) -> None:
        if not self.context.options.save_dicts:
            return

        input_dicts_dir = Path(self.context.options.run_dir) / "input"
        input_dicts_dir.mkdir(parents=True, exist_ok=True)

        input_dict_files = []
        if self.context.options.partial_tables_dict_files:
            input_dict_files.extend(self.context.options.partial_tables_dict_files)
        if self.context.options.partial_tables_exclude_dict_files:
            input_dict_files.extend(self.context.options.partial_tables_exclude_dict_files)

        for dict_file in input_dict_files:
            shutil.copy2(dict_file, input_dicts_dir / Path(dict_file).name)

    async def run_analyze(self) -> None:
        """Run ANALYZE on restored tables to update statistics."""
        if (
            self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE
            or self.metadata.dbg_stage_2_validate_data
            or self.metadata.dbg_stage_3_validate_full
        ):
            self.context.logger.info("-------------> Skipped analyze")
            return

        self.context.logger.info("-------------> Started analyze")
        pool = await create_pool(
            connection_params=self.context.connection_params,
            server_settings=self.context.server_settings,
            min_size=self.context.options.db_connections_per_process,
            max_size=self.context.options.db_connections_per_process,
        )

        loop = asyncio.get_event_loop()
        tasks: set[asyncio.Task[None]] = set()

        try:
            async with pool.acquire() as connection:
                foreign_parents = await self._find_foreign_parents(connection)

            for query in self._generate_analyze_queries(foreign_parents):
                if len(tasks) >= self.context.options.db_connections_per_process:
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    self._raise_first_task_exception(done, pending=tasks)

                tasks.add(loop.create_task(run_query_in_pool(pool, query)))

            # Wait for the remaining queries to finish
            while tasks:
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                self._raise_first_task_exception(done, pending=tasks)
        finally:
            await pool.close()

        self.context.logger.info("<------------- Finished analyze")

    @staticmethod
    async def validate_restore(context: Context) -> None:
        """Validate the restored data against the source database."""
        context.logger.info("-------------> Started validate_restore")

        context.read_prepared_dict()

        if "validate_tables" in context.prepared_dictionary_obj:
            connection = await create_connection(context.connection_params, server_settings=context.server_settings)
            db_objs = await connection.fetch(
                f"""
                select n.nspname, c.relname --, c.reltuples
                from pg_class c
                join pg_namespace n on c.relnamespace = n.oid
                where
                    c.relkind = 'r' and
                    {schemas_filter("n.nspname")} and
                    c.reltuples > 0
            """
            )
            await connection.close()
            tables_in_target_db = [item[0] + "." + item[1] for item in db_objs]

            tables_in_dict = [
                d["schema"] + "." + d["table"] for d in context.prepared_dictionary_obj["validate_tables"]
            ]

            diff_l = list(set(tables_in_target_db) - set(tables_in_dict))
            diff_r = list(set(tables_in_dict) - set(tables_in_target_db))

            if len(diff_r) > 0:
                msg = f"validate_tables: in target DB not found tables:\n{json.dumps(diff_r, indent=4)}"
                context.logger.error(msg)
                raise PgAnonError(ErrorCode.VALIDATION_FAILED, msg)

            if len(diff_l) > 0:
                msg = (
                    "validate_tables: non-empty tables were found in target database that"
                    f" are not described in the dictionary:\n{json.dumps(diff_l, indent=4)}"
                )
                context.logger.error(msg)
                raise PgAnonError(ErrorCode.VALIDATION_FAILED, msg)
        else:
            msg = "Section validate_tables is not found in dictionary!"
            context.logger.error(msg)
            raise PgAnonError(ErrorCode.VALIDATION_FAILED, msg)

        context.logger.info("<------------- Finished validate_restore")
        return

    def _check_dump_version(self) -> None:
        """Stop when a file of the dump needs a newer pg_anon than this one."""
        current = parse_version(__version__)
        for file_name, min_version in self._min_pg_anon_versions.items():
            required = parse_version(min_version)
            if required is None or current is None or required <= current:
                continue

            raise PgAnonError(
                ErrorCode.DUMP_VERSION_NOT_SUPPORTED,
                f"This dump needs pg_anon {min_version} or newer, and this is pg_anon {__version__}. "
                "Update pg_anon to restore it.",
            )

    def _check_archive_matches_structure(self) -> None:
        """Stop when pre_data.backup and structure.json are not from the same dump."""
        if self._is_legacy_dump or not self._has_structure:
            return

        entry_tables, skipped_ids = self._toc_dependencies("pre_data")
        table_lines = [
            toc_line
            for toc_line in read_toc_lines(self.context.pg_restore, self.input_dir / "pre_data.backup")
            if is_table_entry(toc_line) and entry_id(toc_line) not in skipped_ids
        ]
        # OIDs of two databases can be equal by chance, so the name of the table is compared too
        unknown = [
            toc_line
            for toc_line in table_lines
            if not (tables := entry_tables.get(entry_id(toc_line))) or not table_entry_matches(toc_line, *tables[0])
        ]
        if not unknown:
            return

        msg = (
            f"ERROR: pre_data.backup and {self.structure_file_name} in {self.input_dir} are not from the same dump: "
            f"{self.structure_file_name} does not describe {len(unknown)} of {len(table_lines)} tables of the archive. "
            f"Take pre_data.backup, post_data.backup and {self.structure_file_name} from one dump."
        )
        self.context.logger.error(msg)
        raise PgAnonError(ErrorCode.DUMP_STRUCTURE_MISMATCH, msg)

    def _check_data_tables_in_structure(self) -> None:
        """Stop when the dump has data for a table that its structure files do not create."""
        if self._is_legacy_dump or self.context.options.mode != AnonMode.RESTORE or self._skip_pre_data_restore:
            return

        missing = [
            f"{info['schema']}.{info['table']}"
            for info in (self.metadata.files or {}).values()
            if (info["schema"], info["table"]) not in self._structure_tables
            and not info.get("extension_table")
            and self._is_table_restored(info["schema"], info["table"])
        ]
        if not missing:
            return

        msg = (
            f"ERROR: the dump has data for {format_names_summary(missing, 'table')}, but its structure files "
            "do not create them. Exclude these tables with a partial dictionary, or use the structure files "
            "that have them."
        )
        self.context.logger.error(msg)
        raise PgAnonError(ErrorCode.DUMP_DATA_TABLES_NOT_IN_STRUCTURE, msg)

    async def run(self) -> None:
        """Run the restore mode to import data into the target database."""
        self.context.logger.info("-------------> Started restore")
        connection = None

        try:
            self._save_input_dicts_to_run_dir()
            self._check_dump_version()
            self._check_utils_version_for_dump()
            self._check_archive_matches_structure()

            # the lists are checked against the dump before --drop-db changes the target
            self.context.read_partial_tables_dicts()
            self._prepare_tables_lists()
            self._resolve_schemas()
            self._prepare_excluded_tables()
            self._check_data_tables_in_structure()

            await self._drop_database()
            connection = await create_connection(
                self.context.connection_params, server_settings=self.context.server_settings
            )

            if not self.context.options.disable_checks:
                # pg_restore -j N uses N workers and a control connection, alongside this one
                await check_required_connections(connection, self.context.options.db_connections_per_process + 2)

            await self._check_db_is_empty(connection)
            await self._check_no_extra_tables_in_target(connection)

            await self._create_schemas(connection)
            await self._create_extensions(connection)
            await self._create_objects_from_ddl(connection)
            self._make_filtered_toc_list()

            await self._restore_pre_data()
            await self._drop_constraints(connection)

            await self._check_target_tables(connection)
            await self._restore_data(connection)

            await self._restore_post_data()
            await self._create_not_valid_fk(connection)
            await self._check_partitioned_indexes(connection)
            await self._sequences_init(connection)

            await self.run_analyze()
            self._remove_toc_lists()
            await self._warn_unchecked_fk(connection)

            self.context.logger.info("<------------- Finished restore")
        finally:
            if connection:
                await connection.close()

            if self.context.options.save_dicts:
                save_dicts_info_file(self.context.options)

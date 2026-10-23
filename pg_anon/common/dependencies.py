from collections.abc import Collection, Iterable
from dataclasses import dataclass, field

from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.toc import TocEntry
from pg_anon.common.utils import format_names_summary

# OIDs of system catalogs are the same on every PostgreSQL version
PG_CLASS = 1259
PG_NAMESPACE = 2615

TABLE_KINDS = ("r", "p", "f")
EXCLUDABLE_RELATION_KINDS = ("v", "m", "S")

Node = tuple[int, int]
Table = tuple[str, str]


@dataclass
class DependencyGraph:
    """Objects of a database linked by pg_depend, with the object that pg_dump writes for each of them."""

    dependents: dict[Node, set[Node]] = field(default_factory=dict)
    direct_owners: dict[Node, Node] = field(default_factory=dict)
    auto_owners: dict[Node, set[Node]] = field(default_factory=dict)
    relations: dict[int, tuple[str, str, str]] = field(default_factory=dict)
    extension_members: set[Node] = field(default_factory=set)
    schemas: dict[str, int] = field(default_factory=dict)

    def owner(self, node: Node) -> Node:
        """Return the object that holds the given one: a view for its rule, a table for its row type, etc."""
        seen = {node}
        while (parent := self.direct_owners.get(node)) is not None and parent not in seen:
            seen.add(parent)
            node = parent
        return node

    def is_part_of(self, node: Node, dropped: Collection[Node]) -> bool:
        """Check if pg_dump drops the object together with a dropped one: an index or a key with its table, etc."""
        seen: set[Node] = set()
        queue = [node]
        while queue:
            current = queue.pop()
            if current in seen:
                continue
            seen.add(current)
            if current in dropped:
                return True
            if (owner := self.direct_owners.get(current)) is not None:
                queue.append(owner)
            queue.extend(self.auto_owners.get(current, ()))
        return False

    def relation_kind(self, node: Node) -> str | None:
        """Return relkind of a relation, or None for other objects."""
        if node[0] != PG_CLASS or node[1] not in self.relations:
            return None
        return self.relations[node[1]][0]

    def relation_name(self, node: Node) -> Table:
        """Return the schema and the name of a relation."""
        _, schema, name = self.relations[node[1]]
        return schema, name

    def tables(self) -> dict[Node, Table]:
        """Return all tables, partitioned and foreign ones too, except the tables of extensions."""
        return {
            (PG_CLASS, oid): (schema, name)
            for oid, (kind, schema, name) in self.relations.items()
            if kind in TABLE_KINDS and (PG_CLASS, oid) not in self.extension_members
        }

    def table_nodes(self, tables: Iterable[Table]) -> set[Node]:
        """Return the nodes of the given tables."""
        wanted = set(tables)
        return {node for node, table in self.tables().items() if table in wanted}

    def schema_nodes(self, schemas: Iterable[str]) -> set[Node]:
        """Return the nodes of the given schemas."""
        return {(PG_NAMESPACE, self.schemas[schema]) for schema in schemas if schema in self.schemas}

    def walk(self, roots: Iterable[Node], stop_tables: Collection[Node]) -> tuple[set[Node], set[Node]]:
        """Find the owners of all objects that depend on the roots; return them and the stop tables met on the way."""
        roots = set(roots)
        seen = set(roots)
        queue = list(roots)
        owners: set[Node] = set()
        met_tables: set[Node] = set()

        while queue:
            for dependent in self.dependents.get(queue.pop(), ()):
                owner = self.owner(dependent)
                if dependent in self.extension_members or owner in self.extension_members:
                    continue
                if owner in stop_tables and owner not in roots:
                    met_tables.add(owner)
                    continue

                owners.add(owner)
                for node in (dependent, owner):
                    if node not in seen:
                        seen.add(node)
                        queue.append(node)

        return owners - roots, met_tables


def tables_of_owners(graph: DependencyGraph, dump_tables: Collection[Node]) -> dict[Node, set[Node]]:
    """Map every object to the dump tables it depends on; a table depends on itself too."""
    result: dict[Node, set[Node]] = {}
    all_tables = set(graph.tables())
    for table in dump_tables:
        result.setdefault(table, set()).add(table)
        owners, met_tables = graph.walk({table}, all_tables)
        for node in owners | met_tables:
            result.setdefault(node, set()).add(table)
    return result


def dependency_error(error_code: ErrorCode, tables: Iterable[str]) -> PgAnonError:
    """Make the error about kept tables that need excluded schemas or excluded tables."""
    names = format_names_summary(tables, "table")
    if error_code == ErrorCode.EXCLUDED_SCHEMA_DEPENDENCY:
        message = (
            f"Excluded schemas are still needed by {names}: their types or parent tables are in use. "
            "Exclude the tables too or keep the schemas."
        )
    else:
        message = f"Excluded tables are still needed by {names}. Exclude these tables too or keep the excluded ones."
    return PgAnonError(error_code, message)


def object_key(classid: int, objid: int) -> str:
    """Make the metadata key of an object from the OID of its catalog and its own OID."""
    return f"{classid}:{objid}"


def describe_objects(
    graph: DependencyGraph,
    table_owners: dict[Node, set[Node]],
    skipped_owners: Collection[Node],
) -> tuple[dict[str, list[list[str]]], list[str]]:
    """Find the dump tables of each object and the objects to skip; the table of a table goes first."""
    dependencies: dict[str, list[list[str]]] = {}
    skipped: list[str] = []
    # parts of objects are listed too: a TOC entry can carry the OID of a part, like a column default
    for node in sorted(set(table_owners) | set(skipped_owners) | set(graph.direct_owners)):
        owner = graph.owner(node)
        if owner in skipped_owners:
            skipped.append(object_key(*node))
        elif tables := table_owners.get(owner):
            first = [owner] if owner in tables else []
            rest = sorted(graph.relation_name(table) for table in tables if table != owner)
            dependencies[object_key(*node)] = [list(graph.relation_name(table)) for table in first] + [
                list(table) for table in rest
            ]

    return dependencies, skipped


def pack_dependencies(dependencies: dict[str, list[list[str]]]) -> dict[str, list]:
    """Make the dump form of dependencies: each table name once, objects of the same tables in one group."""
    table_ids: dict[Table, int] = {}
    groups: dict[tuple[int, ...], dict[str, list[int]]] = {}
    for key, tables in dependencies.items():
        ids = tuple(table_ids.setdefault((schema, name), len(table_ids)) for schema, name in tables)
        classid, objid = key.split(":")
        groups.setdefault(ids, {}).setdefault(classid, []).append(int(objid))

    return {
        "tables": [list(table) for table in table_ids],
        "groups": [[list(ids), objects] for ids, objects in groups.items()],
    }


def unpack_dependencies(packed: dict[str, list]) -> dict[str, list[list[str]]]:
    """Read the dump form of dependencies back: object -> dump tables it depends on."""
    tables = packed["tables"]
    dependencies: dict[str, list[list[str]]] = {}
    for ids, objects in packed["groups"]:
        names = [list(tables[table_id]) for table_id in ids]
        for classid, objids in objects.items():
            for objid in objids:
                dependencies[object_key(int(classid), objid)] = names
    return dependencies


def map_toc_entries(
    entries: list[TocEntry],
    dependencies: dict[str, list[list[str]]],
    skipped: Collection[str],
) -> tuple[dict[int, list[Table]], set[int]]:
    """Find the dump tables of each TOC entry and the entries to skip, by the OIDs that the dump saved."""
    by_id = {entry.entry_id: entry for entry in entries}
    resolved: dict[int, tuple[list[Table], bool]] = {}

    def resolve(entry: TocEntry) -> tuple[list[Table], bool]:
        if entry.entry_id in resolved:
            return resolved[entry.entry_id]
        resolved[entry.entry_id] = ([], False)  # guards against loops

        if entry.objid:
            # ROW SECURITY has no catalog, but the OID of its table
            key = object_key(entry.classid or PG_CLASS, entry.objid)
            result = ([(schema, name) for schema, name in dependencies.get(key, [])], key in skipped)
        else:
            # COMMENT, ACL, SEQUENCE OWNED BY and the like follow the entries they belong to
            parents = [resolve(by_id[parent_id]) for parent_id in entry.dependencies if parent_id in by_id]
            tables = sorted({table for parent in parents for table in parent[0]})
            result = (tables, any(parent[1] for parent in parents))

        resolved[entry.entry_id] = result
        return result

    entry_tables: dict[int, list[Table]] = {}
    skipped_ids: set[int] = set()
    for entry in entries:
        tables, is_skipped = resolve(entry)
        if is_skipped:
            skipped_ids.add(entry.entry_id)
        elif tables:
            entry_tables[entry.entry_id] = tables

    return entry_tables, skipped_ids

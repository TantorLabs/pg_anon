import re
import subprocess
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

CUSTOM_OBJECTS_ENTRY_RE = re.compile(
    r"^\d+;\s+\d+\s+\d+\s+(DOMAIN|TYPE|FUNCTION|PROCEDURE|CAST|OPERATOR(?!\s+(?:CLASS|FAMILY)\b)|AGGREGATE)\b"
)
OPERATOR_CLASS_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+OPERATOR (?:CLASS|FAMILY)\b")
USER_MAPPING_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+USER MAPPING\b")
EXTENSION_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+EXTENSION\b")
# the public schema has the same OID on every PostgreSQL version
PUBLIC_SCHEMA_ENTRY_RE = re.compile(r"^\d+;\s+2615\s+2200\s+SCHEMA\s+-\s+public\s")

_ENTRY_ID_RE = re.compile(r"^(\d+);\s")
_FAILED_ENTRY_RE = re.compile(r"from TOC entry (\d+)")
_SCHEMA_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+SCHEMA\s+-\s+(?P<name_and_owner>\S.*)$")
_TOC_ENTRY_HEADER_RE = re.compile(r"^-- TOC entry (\d+) \(class (\d+) OID (\d+)\)$")
_TOC_DEPENDENCIES_RE = re.compile(r"^-- Dependencies:((?: \d+)+)$")
_TABLE_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+(?:FOREIGN )?TABLE\s+(?!ATTACH\s)")
# "<id>; <class> <oid> <TYPE> <schema> <name> <owner>": the type is written in capitals
_ENTRY_TITLE_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+(?P<desc>(?:[A-Z]+ )+)(?P<schema>\S+) (?P<rest>.*)$")

# entries that pg_dump writes as parts of a table, so they go away together with the table
TABLE_PART_DESCS = frozenset(
    {
        "ACL",
        "CHECK CONSTRAINT",
        "COMMENT",
        "CONSTRAINT",
        "DEFAULT",
        "FK CONSTRAINT",
        "FOREIGN TABLE",
        "INDEX",
        "INDEX ATTACH",
        "POLICY",
        "PUBLICATION TABLE",
        "ROW SECURITY",
        "RULE",
        "SECURITY LABEL",
        "SEQUENCE",
        "SEQUENCE OWNED BY",
        "SEQUENCE SET",
        "STATISTICS",
        "STATISTICS DATA",
        "TABLE",
        "TABLE ATTACH",
        "TABLE DATA",
        "TRIGGER",
    }
)
# parts of a table that can also depend on another table: they are lost when their own table is restored
TABLE_LINK_DESCS = frozenset({"FK CONSTRAINT", "TRIGGER"})


def read_toc_lines(
    pg_restore: str,
    backup_path: str | Path,
    env: dict[str, str] | None = None,
    schemas: list[str] | None = None,
    tables: list[str] | None = None,
) -> list[str]:
    """Read the TOC of a dump file as a list of entry lines, comments dropped, optionally for some schemas or tables."""
    backup_path = Path(backup_path)
    if not backup_path.exists():
        return []

    filter_args = [f"--schema={schema}" for schema in schemas or []] + [f"--table={table}" for table in tables or []]
    proc = subprocess.Popen([pg_restore, "-l", *filter_args, str(backup_path)], stdout=subprocess.PIPE, env=env)
    toc_bytes, _ = proc.communicate()
    toc_lines = toc_bytes.decode("utf-8", errors="replace").split("\n")

    return [line for line in toc_lines if line and not line.startswith(";")]


@dataclass
class TocEntry:
    entry_id: int
    classid: int
    objid: int
    dependencies: list[int] = field(default_factory=list)


def read_toc_entries(pg_restore: str, backup_path: str | Path, env: dict[str, str] | None = None) -> list[TocEntry]:
    """Read the catalog OIDs and the dependencies of TOC entries from the comments of a verbose pg_restore script."""
    backup_path = Path(backup_path)
    if not backup_path.exists():
        return []

    entries: list[TocEntry] = []
    # verbose mode can write a lot to stderr: a file, not a pipe, so pg_restore never blocks on it
    with tempfile.TemporaryFile() as stderr_file:
        proc = subprocess.Popen(  # noqa: S603
            [pg_restore, "-v", "-f", "-", str(backup_path)],
            stdout=subprocess.PIPE,
            stderr=stderr_file,
            env=env,
        )
        assert proc.stdout is not None  # noqa: S101
        for raw_line in proc.stdout:
            line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
            if match := _TOC_ENTRY_HEADER_RE.match(line):
                entries.append(TocEntry(*(int(value) for value in match.groups())))
            elif entries and (match := _TOC_DEPENDENCIES_RE.match(line)):
                entries[-1].dependencies = [int(value) for value in match.group(1).split()]

        # without the entries the restore would create objects that depend on excluded tables
        if proc.wait() != 0:
            stderr_file.seek(0)
            stderr = stderr_file.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"pg_restore could not read {backup_path}: {stderr[-2000:]}")

    return entries


def write_toc_list(path: Path, lines: list[str]) -> Path:
    """Write entry lines as a list file for pg_restore -L."""
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def entry_id(line: str) -> int | None:
    """Read the id of a TOC entry."""
    match = _ENTRY_ID_RE.match(line)
    return int(match.group(1)) if match else None


def entry_ids(lines: Iterable[str]) -> set[int]:
    """Read the ids of TOC entries."""
    return {entry for line in lines if (entry := entry_id(line)) is not None}


def failed_entry_ids(stderr_text: str) -> set[int]:
    """Read the ids of the TOC entries that pg_restore reports as failed."""
    return {int(entry) for entry in _FAILED_ENTRY_RE.findall(stderr_text)}


def schema_name_from_entry(line: str) -> str | None:
    """Read the schema name of a SCHEMA entry, where the owner closes the line."""
    match = _SCHEMA_ENTRY_RE.match(line)
    if not match:
        return None

    return match.group("name_and_owner").rsplit(" ", 1)[0]


def schemas_in_toc(lines: list[str]) -> set[str]:
    """Return the schemas the dump creates."""
    return {schema for line in lines if (schema := schema_name_from_entry(line))}


def schema_entry_patterns(schemas: Iterable[str]) -> list[re.Pattern[str]]:
    """Match the entries of the schemas themselves: SCHEMA and the COMMENT, ACL and SECURITY LABEL on it."""
    patterns = []
    for schema in schemas:
        name = re.escape(schema)
        patterns.extend(
            [
                re.compile(rf"^\d+;\s+\d+\s+\d+\s+SCHEMA\s+-\s+{name}(?:\s|$)"),
                re.compile(rf"^\d+;\s+\d+\s+\d+\s+(?:ACL|COMMENT|SECURITY LABEL)\s+-\s+SCHEMA\s+{name}(?:\s|$)"),
            ]
        )
    return patterns


def table_entry_matches(line: str, schema: str, table: str) -> bool:
    """Check that the line is the TABLE or FOREIGN TABLE entry of the given table."""
    return bool(re.match(rf"^\d+;\s+\d+\s+\d+\s+(?:FOREIGN )?TABLE\s+{re.escape(schema)}\s+{re.escape(table)}\s", line))


def is_table_entry(line: str) -> bool:
    """Check that the line is the TABLE or FOREIGN TABLE entry of some table."""
    return bool(_TABLE_ENTRY_RE.match(line))


def entry_title(line: str) -> tuple[str, str, str] | None:
    """Return the type, the schema ("-" for none) and the name without the owner of a TOC entry."""
    match = _ENTRY_TITLE_RE.match(line)
    if not match:
        return None
    return match["desc"].strip(), match["schema"], match["rest"].rsplit(" ", 1)[0]

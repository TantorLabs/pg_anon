import re
import subprocess
from collections.abc import Iterable
from pathlib import Path

CUSTOM_OBJECTS_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+(DOMAIN|TYPE|FUNCTION|PROCEDURE|CAST|OPERATOR|AGGREGATE)\b")
USER_MAPPING_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+USER MAPPING\b")
EXTENSION_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+EXTENSION\b")
PUBLICATION_TABLE_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+PUBLICATION TABLE(?:S IN SCHEMA)?\s+(?P<schema>\S+)")

_ENTRY_ID_RE = re.compile(r"^(\d+);\s")
_FAILED_ENTRY_RE = re.compile(r"from TOC entry (\d+)")
_SCHEMA_ENTRY_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+SCHEMA\s+-\s+(?P<name_and_owner>\S.*)$")


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
    """Check that the line is the TABLE entry of the given table."""
    return bool(re.match(rf"^\d+;\s+\d+\s+\d+\s+TABLE\s+{re.escape(schema)}\s+{re.escape(table)}\s", line))

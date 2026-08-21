from __future__ import annotations

import shutil
import subprocess
import sys
from importlib.metadata import distribution, entry_points, PackageNotFoundError
from pathlib import Path

import pytest

DIST_NAME = "pg_anon"

CONSOLE_SCRIPTS = {
    "pg_anon": "pg_anon.__main__:main",
    "pg_anon_api": "pg_anon.rest_api.__main__:main",
}

pytestmark = pytest.mark.packaging


@pytest.fixture(scope="session")
def dist():
    """The installed pg_anon distribution."""
    try:
        return distribution(DIST_NAME)
    except PackageNotFoundError:
        pytest.skip("pg_anon is not installed; run `pip install .` or `pip install -e .`")


def test_version_is_resolved_from_metadata(dist):
    """`pg_anon.version` falls back to "unknown" when metadata is missing."""
    from pg_anon.version import __version__

    assert __version__ != "unknown"
    assert __version__ == dist.version


def test_metadata_has_release_critical_fields(dist):
    meta = dist.metadata
    assert meta["Name"].replace("-", "_") == DIST_NAME
    assert meta["Summary"]
    assert meta["Requires-Python"]
    assert meta.get_all("Classifier")
    assert meta["License-Expression"] == "MIT"


def test_readme_is_attached_as_long_description(dist):
    """PyPI renders the project page from this field."""
    meta = dist.metadata
    assert meta["Description-Content-Type"] == "text/markdown"
    body = meta.get_payload() or meta["Description"] or ""
    assert "pg_anon" in body


def test_console_scripts_are_registered():
    registered = {ep.name: ep.value for ep in entry_points(group="console_scripts") if ep.name in CONSOLE_SCRIPTS}
    assert registered == CONSOLE_SCRIPTS


@pytest.mark.parametrize("script", sorted(CONSOLE_SCRIPTS))
def test_console_script_entry_point_is_importable(script):
    """A typo in pyproject would otherwise only surface at runtime."""
    (ep,) = (ep for ep in entry_points(group="console_scripts") if ep.name == script)
    assert ep.load() is not None


def test_init_sql_is_shipped(dist):
    """`pg_anon init` reads init.sql from the package directory."""
    shipped = {str(path) for path in (dist.files or [])}
    assert "pg_anon/init.sql" in shipped


def test_rest_api_subpackages_are_shipped(dist):
    """`packages.find` must pick up nested packages, not just the top level."""
    shipped = {str(path) for path in (dist.files or [])}
    for module in (
        "pg_anon/rest_api/api.py",
        "pg_anon/rest_api/runners/background/dump.py",
        "pg_anon/rest_api/runners/direct/view_data.py",
        "pg_anon/modes/dump.py",
        "pg_anon/common/db_utils.py",
    ):
        assert module in shipped, f"{module} is missing from the installed distribution"


def test_tests_are_not_installed(dist):
    """The test suite ships in the repository, never in the distribution."""
    leaked = [str(path) for path in (dist.files or []) if str(path).startswith(("tests/", "docs/", "demo/"))]
    assert not leaked


def test_runtime_dependencies_are_declared(dist):
    requires = dist.requires or []
    unconditional = {req.split(";")[0].split(">=")[0].split("<")[0].split("==")[0].strip() for req in requires}
    for package in ("asyncpg", "aioprocessing", "pyyaml", "prettytable"):
        assert package in unconditional, f"{package} is imported at runtime but not declared"


def test_api_extra_is_declared(dist):
    """`pip install pg_anon[api]` must pull FastAPI; the core install must not."""
    api_requirements = [req for req in (dist.requires or []) if 'extra == "api"' in req]
    assert any("fastapi" in req for req in api_requirements)
    assert not any("fastapi" in req and "extra" not in req for req in (dist.requires or []))


def _console_script_path(name: str) -> str | None:
    """Locate a console script near the interpreter, then on PATH.

    Tests usually run as `venv/bin/python -m pytest`, so the venv is not on PATH.
    """
    bindir = Path(sys.executable).parent
    for candidate in (bindir / name, bindir / f"{name}.exe", bindir / "Scripts" / f"{name}.exe"):
        if candidate.is_file():
            return str(candidate)
    return shutil.which(name)


@pytest.mark.parametrize("script", sorted(CONSOLE_SCRIPTS))
def test_console_script_is_installed_as_an_executable(script):
    assert _console_script_path(script) is not None, f"the {script} console script was not installed"


def test_installed_console_script_reports_version(dist):
    executable = _console_script_path("pg_anon")
    if executable is None:
        pytest.skip("the pg_anon console script is not installed")

    result = subprocess.run(
        [executable, "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert dist.version in result.stdout


def test_package_imports_without_optional_api_extra():
    """The CLI must not import FastAPI: it is an optional extra."""
    result = subprocess.run(
        [sys.executable, "-c", "import pg_anon.cli, sys; sys.exit('fastapi' in sys.modules)"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

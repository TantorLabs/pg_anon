from __future__ import annotations

import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

# Paths that must never leave the repository.
EXCLUDED_PREFIXES = ("tests/", "docs/", "demo/", "images/", ".github/")

pytestmark = pytest.mark.packaging


@pytest.fixture(scope="session")
def built_distributions(tmp_path_factory):
    """Build the sdist and wheel once and return their paths."""
    pytest.importorskip("build", reason="`pip install -e .[dev]` provides the build frontend")

    outdir = tmp_path_factory.mktemp("dist")
    result = subprocess.run(
        [sys.executable, "-m", "build", "--outdir", str(outdir)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(f"`python -m build` failed:\n{result.stdout}\n{result.stderr}")

    (sdist,) = outdir.glob("*.tar.gz")
    (wheel,) = outdir.glob("*.whl")
    return sdist, wheel


@pytest.fixture(scope="session")
def wheel_names(built_distributions):
    _, wheel = built_distributions
    with zipfile.ZipFile(wheel) as archive:
        return archive.namelist()


@pytest.fixture(scope="session")
def sdist_names(built_distributions):
    """Sdist member paths with the `pg_anon-<version>/` prefix stripped."""
    sdist, _ = built_distributions
    with tarfile.open(sdist) as archive:
        names = archive.getnames()
    return [name.partition("/")[2] for name in names if "/" in name]


def test_wheel_ships_init_sql(wheel_names):
    assert "pg_anon/init.sql" in wheel_names


def test_wheel_declares_both_console_scripts(built_distributions):
    _, wheel = built_distributions
    with zipfile.ZipFile(wheel) as archive:
        (name,) = [n for n in archive.namelist() if n.endswith(".dist-info/entry_points.txt")]
        entry_points = archive.read(name).decode()
    assert "pg_anon = pg_anon.__main__:main" in entry_points
    assert "pg_anon_api = pg_anon.rest_api.__main__:main" in entry_points


def test_wheel_ships_the_license(wheel_names):
    """`license-files` puts LICENSE into dist-info, where PyPI shows it."""
    assert any(name.endswith("LICENSE") and ".dist-info/" in name for name in wheel_names)


def test_wheel_contains_only_the_package(wheel_names):
    payload = [name for name in wheel_names if ".dist-info/" not in name]
    assert payload, "the wheel is empty"
    assert all(name.startswith("pg_anon/") for name in payload), payload


def test_wheel_excludes_repository_only_paths(wheel_names):
    leaked = [name for name in wheel_names if name.startswith(EXCLUDED_PREFIXES)]
    assert not leaked


def test_sdist_excludes_repository_only_paths(sdist_names):
    leaked = [name for name in sdist_names if name.startswith(EXCLUDED_PREFIXES)]
    assert not leaked


def test_sdist_is_buildable(sdist_names):
    """An sdist must carry what the backend needs to rebuild it."""
    for required in ("pyproject.toml", "README.md", "LICENSE", "PKG-INFO", "pg_anon/init.sql"):
        assert required in sdist_names, f"{required} is missing from the sdist"


def test_no_bytecode_or_caches_are_shipped(wheel_names, sdist_names):
    for names in (wheel_names, sdist_names):
        assert not [n for n in names if n.endswith(".pyc") or "__pycache__" in n]


def test_twine_check_passes(built_distributions):
    """The same gate release.yml runs before publishing."""
    pytest.importorskip("twine", reason="`pip install -e .[dev]` provides twine")

    sdist, wheel = built_distributions
    result = subprocess.run(
        [sys.executable, "-m", "twine", "check", "--strict", str(sdist), str(wheel)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"


def test_version_matches_pyproject(built_distributions):
    """The same invariant release.yml checks against the git tag."""
    import tomllib

    declared = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["version"]
    sdist, wheel = built_distributions
    assert sdist.name == f"pg_anon-{declared}.tar.gz"
    assert wheel.name.startswith(f"pg_anon-{declared}-")

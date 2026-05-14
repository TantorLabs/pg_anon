from __future__ import annotations

from pathlib import Path

from .conftest import input_dict, output_path
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode


def _common_args(db_params, db_name: str, mode: str = "dump") -> list[str]:
    return [
        mode,
        f"--db-host={db_params.test_db_host}",
        f"--db-name={db_name}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--config={db_params.test_config}",
        "--debug",
    ]


async def test_db_passfile_authenticates_dump(
    source_db, db_params, tmp_path,
):
    passfile = tmp_path / ".pgpass"
    line = (
        f"{db_params.test_db_host}:{db_params.test_db_port}:"
        f"{source_db}:{db_params.test_db_user}:{db_params.test_db_user_password}"
    )
    passfile.write_text(line + "\n", encoding="utf-8")
    passfile.chmod(0o600)

    out = output_path("db_passfile")
    args = _common_args(db_params, source_db, mode="dump") + [
        f"--db-passfile={passfile}",
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ]

    res = await PgAnonApp(build_run_options(args)).run()
    assert res.result_code == ResultCode.DONE, (
        "dump with --db-passfile (without --db-user-password) must succeed"
    )
    assert (Path(out) / "metadata.json").exists()


async def test_pgpassword_env_var_authenticates_dump(
    source_db, db_params, monkeypatch,
):
    monkeypatch.setenv("PGPASSWORD", db_params.test_db_user_password)

    out = output_path("pgpassword_env")
    args = _common_args(db_params, source_db, mode="dump") + [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ]

    res = await PgAnonApp(build_run_options(args)).run()
    assert res.result_code == ResultCode.DONE, (
        "dump must authenticate via PGPASSWORD env var when --db-user-password is not given"
    )
    assert (Path(out) / "metadata.json").exists()


async def test_ignore_privileges_strips_grants_on_target(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("ignore_privileges")
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--ignore-privileges",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
        "--ignore-privileges",
    ])
    assert res.result_code == ResultCode.DONE

    src_acl = await db_manager.fetch(source_db, """
        SELECT relacl::text AS acl FROM pg_class
        WHERE relname = 'public_facing' AND relnamespace = 'privs'::regnamespace
    """)
    tgt_acl = await db_manager.fetch(target_db, """
        SELECT relacl::text AS acl FROM pg_class
        WHERE relname = 'public_facing' AND relnamespace = 'privs'::regnamespace
    """)
    assert src_acl and src_acl[0]["acl"], (
        "fixture sanity: privs.public_facing must have ACL entries on source"
    )
    assert tgt_acl[0]["acl"] is None, (
        f"--ignore-privileges must strip table ACL on target; got {tgt_acl[0]['acl']!r}"
    )

    src_def = await db_manager.fetch(source_db, """
        SELECT defaclacl::text AS acl FROM pg_default_acl d
        JOIN pg_namespace n ON n.oid = d.defaclnamespace
        WHERE n.nspname = 'privs'
    """)
    tgt_def = await db_manager.fetch(target_db, """
        SELECT defaclacl::text AS acl FROM pg_default_acl d
        JOIN pg_namespace n ON n.oid = d.defaclnamespace
        WHERE n.nspname = 'privs'
    """)
    if src_def:
        assert not tgt_def, (
            f"--ignore-privileges must strip DEFAULT PRIVILEGES on target; got {tgt_def}"
        )


async def test_seq_init_by_max_value_resets_sequences(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("seq_init_by_max")
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
        "--seq-init-by-max-value",
    ])
    assert res.result_code == ResultCode.DONE

    src_max = await db_manager.fetch(source_db,
        "SELECT max(id) AS m FROM hr.employee")
    src_max_id = src_max[0]["m"]
    assert src_max_id is not None and src_max_id > 0

    inserted = await db_manager.fetch(target_db, """
        INSERT INTO hr.employee
            (first_name, last_name, email, phone, ssn, birth_date,
             department_id, salary)
        VALUES
            ('Probe', 'Probe', ('probe' || extract(epoch from now()) || '@example.com')::citext,
             '+70000000000', '00000000000', DATE '2000-01-01',
             (SELECT id FROM hr.department ORDER BY id LIMIT 1),
             10000)
        RETURNING id
    """)
    new_id = inserted[0]["id"]
    assert new_id > src_max_id, (
        f"--seq-init-by-max-value must reset hr.employee_id_seq past source max; "
        f"max(src.id)={src_max_id}, new id={new_id}"
    )

import asyncio
import os
from typing import Annotated, Optional

import typer

from pg_anon.restore import Restore
from pg_anon.context import Context, AnonMode

app = typer.Typer()


class ArgsRestore:
    pass


@app.command("full")
def restore_full(
    input_dir: Annotated[
        str,
        typer.Option(
            help="Input directory, with the dump files, created in dump mode",
            rich_help_panel="Performance config.",
        ),
    ],
    db_user_password: Annotated[
        str,
        typer.Option(
            prompt=True,
            hide_input=True,
            help="Database password.",
            rich_help_panel="Source DB connection config.",
        ),
    ],
    db_host: Annotated[
        str,
        typer.Option(
            help="Database host address.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = "127.0.0.1",
    db_name: Annotated[
        str,
        typer.Option(
            help="Database name.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = "test_source_db",
    db_port: Annotated[
        int,
        typer.Option(
            help="Database port.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = 5432,
    db_user: Annotated[
        str,
        typer.Option(
            help="Database user.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = "anon_test_user",
    seq_init_by_max_value: Annotated[
        bool,
        typer.Option(
            help="Initialize sequences based on maximum values. Otherwise, "
            "the sequences will be initialized based on the values of the source database.",
            rich_help_panel="Performance config.",
        ),
    ] = False,
    disable_checks: Annotated[
        bool,
        typer.Option(
            help="Disable checks of disk space and PostgreSQL version (default false)",
            rich_help_panel="Performance config.",
        ),
    ] = False,
    threads: Annotated[
        int,
        typer.Option(
            help="Amount of db connections to create.",
            rich_help_panel="Performance config.",
        ),
    ] = 8,
    pg_restore_path: Annotated[
        str,
        typer.Option(
            help="Path to the `pg_restore` Postgres tool.",
            rich_help_panel="Performance config.",
        ),
    ] = "/usr/bin/pg_restore",
    drop_custom_check_constr: Annotated[
        bool,
        typer.Option(
            help="Drop all CHECK constrains containing user-defined procedures to avoid performance "
            "degradation at the data loading stage.",
            rich_help_panel="Performance config.",
        ),
    ] = False,
):
    """Restores database structure using Postgres pg_restore tool and data from the dump to the target DB.
    Restore mode can separately restore database structure or data.

    After successful restore, program fills full all structure and anonymized data to the target DB.
    """

    conn_params = {
        "host": db_host,
        "database": db_name,
        "port": str(db_port),
        "user": db_user,
        "password": db_user_password,
    }

    sens_field_scanner = Restore(
        pg_restore_path=pg_restore_path,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_name=db_name,
        db_user_password=db_user_password,  # ToDo: create certification
        threads=threads,
        input_dir=input_dir,
        seq_init_by_max_value=seq_init_by_max_value,
        disable_checks=disable_checks,
        drop_custom_check_constr=drop_custom_check_constr,
        conn_params=conn_params,
        current_dir=os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        total_rows=0,
        mode=AnonMode.RESTORE,
    )
    asyncio.run(sens_field_scanner.run_mode_restore_asy())


@app.command("struct")
def restore_struct(
    input_dir: Annotated[
        str,
        typer.Option(
            help="Input directory, with the dump files, created in dump mode",
            rich_help_panel="Performance config.",
        ),
    ],
    db_user_password: Annotated[
        str,
        typer.Option(
            prompt=True,
            hide_input=True,
            help="Database password.",
            rich_help_panel="Source DB connection config.",
        ),
    ],
    db_host: Annotated[
        str,
        typer.Option(
            help="Database host address.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = "127.0.0.1",
    db_name: Annotated[
        str,
        typer.Option(
            help="Database name.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = "test_source_db",
    db_port: Annotated[
        int,
        typer.Option(
            help="Database port.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = 5432,
    db_user: Annotated[
        str,
        typer.Option(
            help="Database user.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = "anon_test_user",
    seq_init_by_max_value: Annotated[
        bool,
        typer.Option(
            help="Initialize sequences based on maximum values. Otherwise, "
            "the sequences will be initialized based on the values of the source database.",
            rich_help_panel="Performance config.",
        ),
    ] = False,
    disable_checks: Annotated[
        bool,
        typer.Option(
            help="Disable checks of disk space and PostgreSQL version (default false)",
            rich_help_panel="Performance config.",
        ),
    ] = False,
    threads: Annotated[
        int,
        typer.Option(
            help="Amount of db connections to create.",
            rich_help_panel="Performance config.",
        ),
    ] = 8,
    pg_restore_path: Annotated[
        str,
        typer.Option(
            help="Path to the `pg_restore` Postgres tool.",
            rich_help_panel="Performance config.",
        ),
    ] = "/usr/bin/pg_restore",
    drop_custom_check_constr: Annotated[
        bool,
        typer.Option(
            help="Drop all CHECK constrains containing user-defined procedures to avoid performance "
            "degradation at the data loading stage.",
            rich_help_panel="Performance config.",
        ),
    ] = False,
):
    """Restores database structure using Postgres pg_restore tool from the dump to the target DB.

    After successful restore struct, program fills target DB with all structure.
    """

    conn_params = {
        "host": db_host,
        "database": db_name,
        "port": str(db_port),
        "user": db_user,
        "password": db_user_password,
    }

    sens_field_scanner = Restore(
        pg_restore_path=pg_restore_path,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_name=db_name,
        db_user_password=db_user_password,  # ToDo: create certification
        threads=threads,
        input_dir=input_dir,
        seq_init_by_max_value=seq_init_by_max_value,
        disable_checks=disable_checks,
        drop_custom_check_constr=drop_custom_check_constr,
        conn_params=conn_params,
        current_dir=os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        total_rows=0,
        mode=AnonMode.SYNC_STRUCT_RESTORE,
    )
    asyncio.run(sens_field_scanner.run_mode_sync_struct())


@app.command("data")
def restore_data(
    input_dir: Annotated[
        str,
        typer.Option(
            help="Input directory, with the dump files, created in dump mode",
            rich_help_panel="Performance config.",
        ),
    ],
    db_user_password: Annotated[
        str,
        typer.Option(
            prompt=True,
            hide_input=True,
            help="Database password.",
            rich_help_panel="Source DB connection config.",
        ),
    ],
    db_host: Annotated[
        str,
        typer.Option(
            help="Database host address.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = "127.0.0.1",
    db_name: Annotated[
        str,
        typer.Option(
            help="Database name.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = "test_source_db",
    db_port: Annotated[
        int,
        typer.Option(
            help="Database port.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = 5432,
    db_user: Annotated[
        str,
        typer.Option(
            help="Database user.",
            rich_help_panel="Source DB connection config.",
        ),
    ] = "anon_test_user",
    seq_init_by_max_value: Annotated[
        bool,
        typer.Option(
            help="Initialize sequences based on maximum values. Otherwise, "
            "the sequences will be initialized based on the values of the source database.",
            rich_help_panel="Performance config.",
        ),
    ] = False,
    disable_checks: Annotated[
        bool,
        typer.Option(
            help="Disable checks of disk space and PostgreSQL version (default false)",
            rich_help_panel="Performance config.",
        ),
    ] = False,
    threads: Annotated[
        int,
        typer.Option(
            help="Amount of db connections to create.",
            rich_help_panel="Performance config.",
        ),
    ] = 8,
    pg_restore_path: Annotated[
        str,
        typer.Option(
            help="Path to the `pg_restore` Postgres tool.",
            rich_help_panel="Performance config.",
        ),
    ] = "/usr/bin/pg_restore",
    drop_custom_check_constr: Annotated[
        bool,
        typer.Option(
            help="Drop all CHECK constrains containing user-defined procedures to avoid performance "
            "degradation at the data loading stage.",
            rich_help_panel="Performance config.",
        ),
    ] = False,
):
    """Restores database data using Postgres pg_restore tool from the dump to the target DB.

    After successful restore-data, program fills anonymized data to the target DB.
    """

    conn_params = {
        "host": db_host,
        "database": db_name,
        "port": str(db_port),
        "user": db_user,
        "password": db_user_password,
    }

    sens_field_scanner = Restore(
        pg_restore_path=pg_restore_path,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_name=db_name,
        db_user_password=db_user_password,  # ToDo: create certification
        threads=threads,
        input_dir=input_dir,
        seq_init_by_max_value=seq_init_by_max_value,
        disable_checks=disable_checks,
        drop_custom_check_constr=drop_custom_check_constr,
        conn_params=conn_params,
        current_dir=os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        total_rows=0,
        mode=AnonMode.SYNC_DATA_RESTORE,
    )
    asyncio.run(sens_field_scanner.run_mode_sync_data())

import asyncio
import os
from typing import Annotated, Optional

import typer

from pg_anon.common import ScanMode
from pg_anon.create_dict import SensFieldScan

app = typer.Typer()


@app.command()
def run(
    db_password: Annotated[
        str,
        typer.Option(
            prompt=True,
            hide_input=True,
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
        str,
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
    output_dict_file: Annotated[
        str,
        typer.Option(
            help="Filename of output dict file. If scipped, <db_name>_<db_user>_meta.py name will be used."
        ),
    ] = "output-dict-file.py",
    dict_file_name: Annotated[
        str,
        typer.Option(
            help="Filename of manually created metadict.  If scipped, <db_name>_<db_user>.py name will be used.",
        ),
    ] = None,
    # scan_mode: ScanMode = ScanMode.PARTIAL,
    scan_partial_rows: Annotated[
        Optional[str],
        typer.Option(
            help="Count of rows from each table to scan. If skipped, will scan all rows in the db. It may take a lot of time.",
        ),
    ] = None,
    processes: Annotated[
        int,
        typer.Option(
            help="Amount of processes to use.",
            rich_help_panel="Performance config.",
        ),
    ] = 8,
    threads: Annotated[
        int,
        typer.Option(
            help="Amount of db connections to create.",
            rich_help_panel="Performance config.",
        ),
    ] = 8,
    full_scan: bool = typer.Option(
        default=False,
        prompt="Are you sure you want to delete ALL users?",
        help="Full database scan.",
    ),
):
    """Scan database for sensitive data based on metadict.

    After successful scan, create the dict file in dict folder with information about sensitive
    fields with suggested masking functions.
    """
    if scan_partial_rows:
        scan_mode = ScanMode.PARTIAL
    else:
        scan_mode = ScanMode.FULL
    conn_params = {
        "host": db_host,
        "database": db_name,
        "port": str(db_port),
        "user": db_user,
        "password": db_password,
    }

    if not output_dict_file:
        output_dict_file = f"{db_name}_{db_user}.py"

    sens_field_scanner = SensFieldScan(
        current_dir=os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        conn_params=conn_params,
        processes=processes,
        threads=threads,
        output_dict_file=output_dict_file,
        scan_mode=scan_mode,
        scan_partial_rows=True,
        dict_file_name=dict_file_name,
    )
    asyncio.run(sens_field_scanner.create_dict())

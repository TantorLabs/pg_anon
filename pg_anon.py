import asyncio

import typer

from pg_anon import restore_cli

app = typer.Typer()
app.add_typer(restore_cli.app, name="restore")


if __name__ == "__main__":
    app()

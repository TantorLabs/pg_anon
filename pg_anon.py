import asyncio

import typer

from pg_anon import create_dict_cli

app = typer.Typer()
app.add_typer(create_dict_cli.app, name="create-dict")


if __name__ == "__main__":
    app()

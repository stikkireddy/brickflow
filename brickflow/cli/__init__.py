import os
import subprocess
import webbrowser
from typing import Optional, Tuple

import click
from click import ClickException


def cdktf_command(base_command: Optional[str] = None) -> click.Command:
    @click.command(
        name="cdktf_cmd",
        short_help="CLI for deploying workflow projects.",
        context_settings={"ignore_unknown_options": True},
        add_help_option=False,
    )
    @click.argument("args", nargs=-1)
    def cmd(args: Tuple[str]) -> None:
        my_env = os.environ.copy()
        try:
            _args = list(args)
            # add a base command if its provided for proxying for brickflow deploy
            if base_command is not None:
                _args = [base_command] + _args
            subprocess.run(["cdktf", *_args], check=True, env=my_env)
        except subprocess.CalledProcessError as e:
            raise ClickException(str(e))

    return cmd


class CdktfCmd(click.Group):
    def get_command(self, ctx: click.Context, cmd_name: str) -> Optional[click.Command]:
        if cmd_name == "cdktf":
            return cdktf_command()
        elif cmd_name in ["deploy", "diff"]:
            return cdktf_command(cmd_name)
        else:
            rv = click.Group.get_command(self, ctx, cmd_name)
            if rv is not None:
                return rv
            raise ctx.fail(f"No such command '{cmd_name}'.")


@click.group(cls=CdktfCmd)
def cli() -> None:
    """CLI for managing Databricks Workflows"""


@cli.command
def init() -> None:
    """Initialize your project with Brickflows..."""
    click.echo("hello world")


@cli.command
def docs() -> None:
    """Use to open docs in your browser..."""
    webbrowser.open("https://github.com/stikkireddy/brickflow", new=2)
    click.echo("Opening browser for docs...")


@cli.command
def cdktf() -> None:
    """CLI for deploying workflow projects."""
    # Hack for having cdktf show up as a command in brickflow
    # with documentation.
    pass  # pragma: no cover


@cli.command
def deploy() -> None:
    """CLI for deploying workflow projects."""
    # Hack for having cdktf show up as a command in brickflow
    # with documentation.
    pass  # pragma: no cover


@cli.command
def diff() -> None:
    """CLI for deploying workflow projects."""
    # Hack for having cdktf show up as a command in brickflow
    # with documentation.
    pass  # pragma: no cover

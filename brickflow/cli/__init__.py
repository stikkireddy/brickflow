import os
import os.path
import subprocess
import webbrowser
from typing import Optional, Tuple

import click
from click import ClickException

from brickflow.cli.configure import (
    _check_git_dir,
    _gitignore_exists,
    GitIgnoreNotFoundError,
    _update_gitignore,
    _validate_package,
    GitNotFoundError,
    render_template,
    create_entry_point,
)
from brickflow.engine import get_git_remote_url_https


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


@click.group(invoke_without_command=True,no_args_is_help=True, cls=CdktfCmd)
@click.version_option(prog_name="brickflow")
def cli() -> None:
    """CLI for managing Databricks Workflows"""


@cli.command
@click.option("-n", "--project-name", type=str, prompt=True)
@click.option(
    "-p", "--git-provider", type=click.Choice(["github", "gitlab"]), prompt=True
)
@click.option(
    "-w", "--workflows-dir", type=click.Path(exists=True, file_okay=False), prompt=True
)
def init(project_name: str, git_provider: str, workflows_dir: str) -> None:
    """Initialize your project with Brickflows..."""
    try:
        _check_git_dir()
        if _gitignore_exists() is False:
            raise GitIgnoreNotFoundError
        git_https_url: str = get_git_remote_url_https() or ""
        _update_gitignore()
        create_entry_point(
            workflows_dir,
            render_template(
                project_name=project_name,
                git_provider=git_provider,
                git_https_url=git_https_url,
                pkg=_validate_package(workflows_dir),
            ),
        )
    except GitNotFoundError:
        raise ClickException(
            "Please make sure you are in the root directory of your project with git initialized."
        )
    except GitIgnoreNotFoundError:
        raise ClickException(
            "Please make sure you create a .gitignore file in the root directory."
        )


@cli.command
def docs() -> None:
    """Use to open docs in your browser..."""
    webbrowser.open("https://stikkireddy.github.io/brickflow/", new=2)
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

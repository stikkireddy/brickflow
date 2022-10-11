from __future__ import annotations

import os
import os.path
import subprocess
import webbrowser
from typing import Optional, Tuple, Callable, Union, List, Any

import click
from click import ClickException

from brickflow import log
from brickflow.cli.configure import (
    _check_git_dir,
    _gitignore_exists,
    GitIgnoreNotFoundError,
    _update_gitignore,
    _validate_package,
    GitNotFoundError,
    render_template,
    create_entry_point,
    idempotent_cdktf_out,
)
from brickflow.engine import get_git_remote_url_https, BrickflowEnvVars


def exec_cdktf_command(
    base_command: Optional[str], args: Union[Tuple[str] | List[str]]
) -> None:
    _check_git_dir()
    os.environ["PYTHONPATH"] = os.getcwd()
    my_env = os.environ.copy()
    try:
        _args = list(args)
        # add a base command if its provided for proxying for brickflow deploy
        if base_command is not None:
            _args = [base_command] + _args
        subprocess.run(["cdktf", *_args], check=True, env=my_env)
    except subprocess.CalledProcessError as e:
        raise ClickException(str(e))


def cdktf_command(base_command: Optional[str] = None) -> click.Command:
    @click.command(
        name="cdktf_cmd",
        short_help="CLI for deploying workflow projects.",
        context_settings={"ignore_unknown_options": True},
        add_help_option=False,
    )
    @click.argument("args", nargs=-1)
    def cmd(args: Tuple[str]) -> None:
        # check to make sure you are in project root and then set python path to whole dir
        exec_cdktf_command(base_command, args)

    return cmd


class CdktfCmd(click.Group):
    def get_command(self, ctx: click.Context, cmd_name: str) -> Optional[click.Command]:
        if cmd_name == "cdktf":
            return cdktf_command()
        # elif cmd_name in ["deploy", "diff"]:
        #     return cdktf_command(cmd_name)
        else:
            rv = click.Group.get_command(self, ctx, cmd_name)
            if rv is not None:
                return rv
            raise ctx.fail(f"No such command '{cmd_name}'.")


@click.group(invoke_without_command=True, no_args_is_help=True, cls=CdktfCmd)
@click.version_option(prog_name="brickflow")
def cli() -> None:
    """CLI for managing Databricks Workflows"""


@cli.command
@click.option("-n", "--project-name", type=str, prompt=True)
@click.option(
    "-p",
    "--git-provider",
    type=click.Choice(["github", "gitlab"]),
    default="github",
    prompt=True,
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
        idempotent_cdktf_out(workflows_dir)
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
    docs_site = "https://stikkireddy.github.io/brickflow/"
    webbrowser.open(docs_site, new=2)
    click.echo(f"Opening browser for docs... site: {docs_site}")


@cli.command
def cdktf() -> None:
    """CLI for deploying workflow projects."""
    # Hack for having cdktf show up as a command in brickflow
    # with documentation.
    pass  # pragma: no cover


def cdktf_env_set_options(f: Callable) -> Callable:
    def bind_env_var(env_var: str) -> Callable:
        def callback(ctx: click.Context, param: str, value: Any) -> None:  # NOQA
            if value is not None:
                log.info("Setting env var: %s to %s...", env_var, value)
                os.environ[env_var] = (
                    str(value).lower() if isinstance(value, bool) else value
                )

        return callback

    options = [
        click.option(
            "--local-mode",
            "-l",
            is_flag=True,
            default=True,
            callback=bind_env_var(BrickflowEnvVars.BRICKFLOW_LOCAL_MODE.value),
            help="TBD.",
        ),
        click.option(
            "--repo-url",
            "-r",
            default=None,
            type=str,
            callback=bind_env_var(BrickflowEnvVars.BRICKFLOW_GIT_REPO.value),
            help="TBD.",
        ),
        click.option(
            "--git-ref",
            default=None,
            type=str,
            callback=bind_env_var(BrickflowEnvVars.BRICKFLOW_GIT_REF.value),
            help="TBD.",
        ),
        click.option(
            "--git-provider",
            default=None,
            type=str,
            callback=bind_env_var(BrickflowEnvVars.BRICKFLOW_GIT_PROVIDER.value),
            help="TBD.",
        ),
        click.option(
            "--profile",
            "-p",
            default=None,
            type=str,
            callback=bind_env_var(
                BrickflowEnvVars.BRICKFLOW_DATABRICKS_CONFIG_PROFILE.value
            ),
            help="TBD.",
        ),
    ]
    for option in options:
        f = option(f)
    return f


@cli.command
@cdktf_env_set_options
def deploy(**_: Any) -> None:
    """CLI for deploying workflow projects."""
    # Hack for having cdktf show up as a command in brickflow
    # with documentation.
    exec_cdktf_command("deploy", [])
    # pass  # pragma: no cover


@cli.command
@cdktf_env_set_options
def diff(**_: Any) -> None:
    """CLI for deploying workflow projects."""
    # Hack for having cdktf show up as a command in brickflow
    # with documentation.
    exec_cdktf_command("diff", [])
    # pass  # pragma: no cover

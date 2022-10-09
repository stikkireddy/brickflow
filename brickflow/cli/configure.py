import importlib
import os
import re
from pathlib import Path

import click
from jinja2 import Environment, BaseLoader


class GitNotFoundError(Exception):
    pass


class GitIgnoreNotFoundError(Exception):
    pass


def _check_git_dir() -> None:
    if os.path.exists(".git") and os.path.isdir(".git"):
        return
    raise GitNotFoundError


def _gitignore_exists() -> bool:
    return os.path.exists(".gitignore") and os.path.isfile(".gitignore")


def _get_gitignore() -> str:
    with open(".gitignore", "r", encoding="utf-8") as f:
        return f.read()


def _get_gitignore_template() -> str:
    template = Path(__file__).parent.absolute() / "gitignore_template.txt"
    with open(str(template), "r", encoding="utf-8") as f:
        return f.read()


def _write_gitignore(data: str) -> None:
    with open(".gitignore", "w", encoding="utf-8") as f:
        f.write(data)
        f.flush()


def _update_gitignore() -> None:
    search_regex = r"(# GENERATED BY BRICKFLOW CLI --START--(.|\n)*# GENERATED BY BRICKFLOW CLI --END--)"

    git_ignore_data = _get_gitignore()
    git_ignore_template = _get_gitignore_template()
    search = re.findall(search_regex, git_ignore_data)
    if len(search) > 0:
        search_match = search[0][0]
        gitignore_file_data = git_ignore_data.replace(search_match, git_ignore_template)
    else:
        gitignore_file_data = "\n\n".join([git_ignore_data, git_ignore_template])
    _write_gitignore(gitignore_file_data)


def _validate_package(folder_path: str) -> str:
    if folder_path is None:
        raise ImportError(f"Invalid pkg error: {str(folder_path)}")
    folder_pkg_path = folder_path.replace("/", ".")
    for module in os.listdir(Path(folder_path).absolute()):
        # only find python files and ignore __init__.py
        if module == "__init__.py" or module[-3:] != ".py":
            continue
        module_name = module.replace(".py", "")
        # import all the modules into the mod object and not actually import them using __import__
        mod = importlib.import_module(f"{folder_pkg_path}.{module_name}")
        click.echo(f"Scanned module: {mod.__name__}")
    return folder_pkg_path


def render_template(**kwargs) -> str:  # type: ignore
    template = Path(__file__).parent.absolute() / "entrypoint.template"
    with template.open("r") as f:
        data = f.read()
        return Environment(loader=BaseLoader()).from_string(data).render(**kwargs)


def create_entry_point(working_dir: str, data: str) -> None:
    path = Path(working_dir) / "entrypoint.py"
    if path.exists():
        click.echo(f"Path: {str(path.absolute())} already exists...")
        path = Path(working_dir) / "entrypoint.py.new"

    click.echo(f"Creating file in path: {str(path.absolute())}...")
    with path.open("w") as f:
        f.write(data)
import os
import subprocess
import traceback
from pathlib import Path
from unittest.mock import patch, Mock

import click
from click.testing import CliRunner

from brickflow.cli import cli


def fake_run(*_, **__):
    click.echo("hello world")


def fake_run_with_error(*_, **__):
    raise subprocess.CalledProcessError(
        returncode=127,
        cmd="cdktf help",
        output="cdktf help error",
        stderr="cdktf help std error",
    )


class TestCli:
    @patch("subprocess.check_output")
    @patch("os.path")
    def test_init(self, path_mock: Mock, subproc_mock: Mock, tmp_path):
        test_dir = Path(tmp_path) / "test"
        test_dir.mkdir(exist_ok=True, parents=True)
        os.chdir(str(test_dir))
        test_git_ignore = test_dir / ".gitignore"
        with test_git_ignore.open("w") as f:
            f.write("")
        assert test_dir.exists() is True and test_dir.is_dir()
        subproc_mock.return_value = b"https://github.com/someorg/somerepo"
        path_mock.exists.return_value = True
        path_mock.isdir.return_value = True
        runner = CliRunner()
        result = runner.invoke(
            cli, ["init", "-n", "test-proj", "-p", "github", "-w", str(test_dir)]
        )  # noqa
        assert result.exit_code == 0, result.output

    def test_init_no_git_error(self, tmp_path):
        test_dir = Path(tmp_path) / "test"
        test_dir.mkdir(exist_ok=True, parents=True)
        runner = CliRunner()
        result = runner.invoke(
            cli, ["init", "-n", "test-proj", "-p", "github", "-w", str(test_dir)]
        )  # noqa
        assert result.exit_code == 1
        assert result.output.strip().startswith(
            "Error: Please make sure you are in the root "
            "directory of your project with git initialized."
        )

    @patch("os.path")
    def test_init_no_gitignore_error(self, path_mock: Mock, tmp_path):
        test_dir = Path(tmp_path) / "test"
        test_dir.mkdir(exist_ok=True, parents=True)
        os.chdir(str(test_dir))
        path_mock.exists.return_value = True
        path_mock.isdir.return_value = True
        path_mock.isfile.return_value = False
        runner = CliRunner()
        result = runner.invoke(
            cli, ["init", "-n", "test-proj", "-p", "github", "-w", str(test_dir)]
        )  # noqa
        assert result.exit_code == 1
        assert result.output.strip().startswith(
            "Error: Please make sure you create "
            "a .gitignore file in the root directory."
        )

    @patch("os.path")
    @patch("os.environ.copy", wraps=os.environ.copy)
    @patch("subprocess.run")
    def test_cdktf(self, run_mock: Mock, os_environ_mock: Mock, path_mock: Mock):
        runner = CliRunner()
        run_mock.side_effect = fake_run
        path_mock.exists.return_value = True
        path_mock.isdir.return_value = True
        result = runner.invoke(cli, ["cdktf", "help"])  # noqa
        assert result.exit_code == 0, traceback.print_exception(*result.exc_info)
        assert result.output.strip() == "hello world"
        run_mock.assert_called_once_with(
            ["cdktf", "help"], check=True, env=os.environ.copy()
        )
        os_environ_mock.assert_called()

    @patch("os.path")
    @patch("os.environ.copy", wraps=os.environ.copy)
    @patch("subprocess.run")
    def test_deploy(self, run_mock: Mock, os_environ_mock: Mock, path_mock: Mock):
        runner = CliRunner()
        run_mock.side_effect = fake_run
        path_mock.exists.return_value = True
        path_mock.isdir.return_value = True
        result = runner.invoke(cli, ["diff"])  # noqa
        assert result.exit_code == 0, traceback.print_exception(*result.exc_info)
        assert result.output.strip() == "hello world"
        run_mock.assert_called_once_with(
            ["cdktf", "diff"], check=True, env=os.environ.copy()
        )
        os_environ_mock.assert_called()

    @patch("os.path")
    @patch("os.environ.copy", wraps=os.environ.copy)
    @patch("subprocess.run")
    def test_cdktf_error(self, run_mock: Mock, os_environ_mock: Mock, path_mock: Mock):
        runner = CliRunner()
        run_mock.side_effect = fake_run_with_error
        path_mock.exists.return_value = True
        path_mock.isdir.return_value = True
        result = runner.invoke(cli, ["cdktf", "help"])  # noqa
        assert result.exit_code == 1, traceback.print_exception(*result.exc_info)
        assert (
            result.output.strip()
            == "Error: Command 'cdktf help' returned non-zero exit status 127."
        )
        run_mock.assert_called_once_with(
            ["cdktf", "help"], check=True, env=os.environ.copy()
        )
        os_environ_mock.assert_called()

    def test_no_command_error(self):
        runner = CliRunner()
        non_existent_command = "non_existent_command"
        result = runner.invoke(cli, ["non_existent_command"])  # noqa
        assert result.exit_code == 2
        assert result.output.strip().endswith(
            f"Error: No such command '{non_existent_command}'."
        )

    @patch("webbrowser.open")
    def test_docs(self, browser: Mock):
        runner = CliRunner()
        browser.return_value = None
        result = runner.invoke(cli, ["docs"])  # noqa
        assert result.exit_code == 0, traceback.print_exception(*result.exc_info)
        assert result.output.strip().startswith("Opening browser for docs...")
        browser.assert_called_once_with(
            "https://stikkireddy.github.io/brickflow/", new=2
        )

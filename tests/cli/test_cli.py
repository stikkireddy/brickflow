import os
import subprocess
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
    def test_init(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["init"])  # noqa
        assert result.exit_code == 0
        assert result.output.strip() == "hello world"

    @patch("os.environ.copy", wraps=os.environ.copy)
    @patch("subprocess.run")
    def test_cdktf(self, run_mock: Mock, os_environ_mock: Mock):
        runner = CliRunner()
        run_mock.side_effect = fake_run
        result = runner.invoke(cli, ["cdktf", "help"])  # noqa
        run_mock.assert_called_once_with(
            ["cdktf", "help"], check=True, env=os.environ.copy()
        )
        os_environ_mock.assert_called()
        assert result.exit_code == 0
        assert result.output.strip() == "hello world"

    @patch("os.environ.copy", wraps=os.environ.copy)
    @patch("subprocess.run")
    def test_deploy(self, run_mock: Mock, os_environ_mock: Mock):
        runner = CliRunner()
        run_mock.side_effect = fake_run
        result = runner.invoke(cli, ["diff"])  # noqa
        run_mock.assert_called_once_with(
            ["cdktf", "diff"], check=True, env=os.environ.copy()
        )
        os_environ_mock.assert_called()
        assert result.exit_code == 0
        assert result.output.strip() == "hello world"

    @patch("os.environ.copy", wraps=os.environ.copy)
    @patch("subprocess.run")
    def test_cdktf_error(self, run_mock: Mock, os_environ_mock: Mock):
        runner = CliRunner()
        run_mock.side_effect = fake_run_with_error
        result = runner.invoke(cli, ["cdktf", "help"])  # noqa
        run_mock.assert_called_once_with(
            ["cdktf", "help"], check=True, env=os.environ.copy()
        )
        os_environ_mock.assert_called()
        assert result.exit_code == 1
        assert (
            result.output.strip()
            == "Error: Command 'cdktf help' returned non-zero exit status 127."
        )

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
        browser.assert_called_once_with(
            "https://github.com/stikkireddy/brickflow", new=2
        )
        assert result.exit_code == 0
        assert result.output.strip().endswith("Opening browser for docs...")

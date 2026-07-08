"""Tests for CLI exit-code behavior (metacrafter/__main__.py)."""
import subprocess
import sys


def _run(args):
    return subprocess.run(
        [sys.executable, "-m", "metacrafter", *args],
        capture_output=True,
        text=True,
    )


def test_help_exits_zero():
    """`--help` should exit successfully."""
    result = _run(["--help"])
    assert result.returncode == 0


def test_invalid_command_exits_nonzero():
    """An unknown command must produce a non-zero exit code for scripting."""
    result = _run(["definitely-not-a-real-command"])
    assert result.returncode != 0


def test_missing_file_exits_nonzero():
    """Scanning a nonexistent file must exit non-zero rather than 0."""
    result = _run(["scan", "file", "/nonexistent/path/does_not_exist.csv"])
    assert result.returncode != 0

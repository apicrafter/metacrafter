#!/usr/bin/env python
"""The main entry point. Invoke as `metacrafter' or `python -m metacrafter`.

"""
import sys


def main():
    """Main entry point for metacrafter CLI."""
    try:
        # Import commands to register them with Typer app instances
        from .cli import commands  # noqa: F401
        
        from .core import app

        app()
    except KeyboardInterrupt:
        # 130 is the conventional exit code for termination by Ctrl-C (SIGINT).
        print("Ctrl-C pressed. Aborting", file=sys.stderr)
        sys.exit(130)
    except SystemExit:
        # Preserve exit codes raised by Typer/argparse (including 0 on success).
        raise
    except Exception as exc:  # noqa: BLE001 - top-level guard for non-zero exit
        # str(KeyError(0)) is "0": name the exception when its text alone says nothing.
        text = str(exc)
        if isinstance(exc, LookupError) or len(text) < 3:
            text = f"{type(exc).__name__}: {text}" if text else type(exc).__name__
        print(f"Error: {text}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

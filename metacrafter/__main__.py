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
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

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
        print("Ctrl-C pressed. Aborting")
    sys.exit(0)


if __name__ == "__main__":
    main()

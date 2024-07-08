#!/usr/bin/env python
"""The main entry point. Invoke as `metacrafter' or `python -m metacrafter`.

"""
import sys


def main():
    try:
        from .core import app

        exit_status = app()
    except KeyboardInterrupt:
        print("Ctrl-C pressed. Aborting")
    sys.exit(0)


if __name__ == "__main__":
    main()

"""Thin shim for backward compatibility.

Project metadata and dependencies now live in ``pyproject.toml``. This file is
retained so legacy ``python setup.py`` / editable-install tooling keeps working.
"""
from setuptools import setup

setup()

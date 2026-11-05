#!/usr/bin/env python3
"""Print a pip requirement for a package pinned in ecosystem.lock.

Usage: python scripts/ecosystem_ref.py metacrafter-rules
"""
import sys
from pathlib import Path

import yaml

lock = yaml.safe_load((Path(__file__).resolve().parent.parent / "ecosystem.lock").read_text())
name = sys.argv[1]
entry = lock[name]
print(f"{name} @ git+https://github.com/{entry['repo']}@{entry['ref']}")

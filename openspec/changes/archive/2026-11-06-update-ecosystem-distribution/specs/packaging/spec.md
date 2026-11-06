## REMOVED Requirements

### Requirement: Rules Optional Extra
**Reason**: The `rules` extra no longer installs a package: the rule pack is downloaded from a GitHub release (see `ecosystem-distribution`). The extra stays as an empty alias.
**Migration**: `pip install metacrafter`; the pack is fetched on first use or with `metacrafter rules fetch`.

### Requirement: Extended Rules Wheel Contents
**Reason**: Metacrafter no longer installs the metacrafter-rules wheel; it reads the rules from the release archive.
**Migration**: None for users; metacrafter-rules may keep shipping YAML in its own builds.

### Requirement: Required Rules Package
**Reason**: `metacrafter-rules` is not a Python dependency and is not published on PyPI.
**Migration**: The pinned GitHub release is downloaded and verified (`Rule Pack from GitHub Releases`).

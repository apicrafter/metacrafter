## REMOVED Requirements

### Requirement: Automatic Extended Rulepath
**Reason**: Replaced by `Deterministic Rulepath Order`: there are no built-in rules, and the default pack is resolved as described in `Rule Pack Source Order`.
**Migration**: `auto_rules: false` still disables the default pack.

### Requirement: Packaged defaults
**Reason**: Rule YAML is not vendored into the metacrafter package; the default pack comes from a pinned metacrafter-rules release.
**Migration**: Installs work outside a checkout; the pack is downloaded on first use or with `metacrafter rules fetch`.

### Requirement: Package-Resolved Default Rule Pack
**Reason**: The default pack no longer comes only from an installed package; see `Rule Pack Source Order` (rules_dir, installed `metacrafterext`, cache, verified download). Rules are still never loaded from the current directory implicitly (`Deterministic Rulepath Order`).
**Migration**: Use `rules_dir` for a local checkout.

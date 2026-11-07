# Change: Ship the pinned rule pack in the metacrafter package

## Why

The first scan on a machine (or in a fresh CI job, Docker image or virtualenv) blocks while
the pinned metacrafter-rules release downloads from `codeload.github.com`. GitHub builds the
archive on request, so the first byte takes 9–12 s. The 284 KB archive then transfers at
14–20 KB/s. Measured on 2026-11-06:

| Path | Time |
|---|---|
| Cached pack (`~/.cache/metacrafter/rules/v0.0.2`) | 0.003 s |
| Download of the pinned archive (3 runs) | 24 s, 25 s, 39 s |
| First `metacrafter scan file` with an empty cache | 70 s |
| Verify and unpack the same archive from a local file | 0.07 s |

The release is pinned in the package (`DEFAULT_RELEASE`, `DEFAULT_SHA256`), so a new rule
release already needs a new metacrafter release. A copy of the pinned archive in the
package changes no version behavior and removes the network from the default path.

## What Changes

- **Bundled archive.** The package includes the pinned release archive as package data:
  `metacrafter/rules/bundled/metacrafter-rules-<tag>.tar.gz`. It holds the exact bytes of
  the GitHub archive and is verified with the same pinned SHA-256. On a cache miss it is
  unpacked into the cache in the same way as a download.
- **Source order** for the default rule pack:
  1. `rules_dir` (a local checkout or unpacked pack);
  2. an installed `metacrafterext` package;
  3. the cache;
  4. the bundled archive (only for the pinned release and hash);
  5. a download (unless `rules_offline`).
- **Offline.** `rules_offline` works without `metacrafter rules fetch` for the pinned
  release.
- **`metacrafter rules fetch`** uses the bundled archive on a cache miss. `--force` still
  downloads from GitHub. `--release` with `--sha256` downloads as before.
- **Release bump.** `scripts/bundle_rule_pack.py` downloads the pinned archive, checks the
  hash and writes the bundled file. A test fails if the bundled file does not match
  `DEFAULT_SHA256` and `ecosystem.lock`.
- **CI** no longer needs `metacrafter rules fetch` before tests.

## Impact

- Affected specs: `ecosystem-distribution`
- Affected code:
  - `metacrafter/rules/pack.py`;
  - new `metacrafter/rules/bundled/` and `scripts/bundle_rule_pack.py`;
  - `pyproject.toml` (package data);
  - `tests/test_rule_pack.py`, CI workflows;
  - docs (installation, ecosystem), CHANGELOG, AGENTS.md.
- **Size:** the wheel and sdist grow by about 284 KB.
- **Network:** the default rule pack needs no network access. Registry data is unchanged.

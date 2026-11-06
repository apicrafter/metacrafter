# Design: Ship the pinned rule pack in the metacrafter package

## Context

`metacrafter/rules/pack.py` resolves the default pack from `rules_dir`, an installed
`metacrafterext`, the cache, or a download of
`https://github.com/apicrafter/metacrafter-rules/archive/refs/tags/<tag>.tar.gz`.
That URL redirects to `codeload.github.com`, which generates the tarball on request.

## Decision

Ship the exact bytes of the pinned GitHub archive in the package. Verify them with the pinned
SHA-256 and unpack them with the same code path as a download.

- The GitHub release stays the source of truth: the bundled file is a copy of it, and the
  same hash pins both.
- The archive stays compressed in the package (284 KB, not about 1.9 MB unpacked), and
  `metacrafterext` keeps loading only from a verified, unpacked archive in the cache.
- Tag archives on GitHub keep stable checksums; the pinned hash already depends on that.

## Alternatives considered

1. **Slim release asset on the GitHub CDN.** metacrafter-rules would attach an asset with
   only `rules/`, `metacrafterext/` and `LICENSE` (183 KB) to each release. Release assets
   skip the on-request archive build: a 1.9 MB asset took 27 s at 73 KB/s, with the first
   byte after 5.7 s. Expected first run: 5–10 s instead of 20–70 s. This needs a new
   metacrafter-rules release and a second pinned hash, and the first run still needs the
   network. Can be added later for `rules_release` overrides.
2. **Per-file download from `raw.githubusercontent.com`.** First byte after 0.7 s, but
   about 250 files and a per-file hash manifest. Slower in total and more complex.
3. **Download at build time** (build hook). Builds from sdist would need network access.

## Risks

- A release bump that forgets the bundled file: the test on the bundled hash fails.
- Package size: +284 KB per wheel.

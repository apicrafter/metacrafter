# Design: bundled registry snapshot and bounded downloads

## Alternatives

| Option | Cold start on a slow link | Result |
|---|---|---|
| gzip only | 5.6 times fewer bytes; about 40 s at 15 KB/s | still blocks; no behavior change |
| Overall deadline only | bounded, but the scan runs without registry data | every scan retries and fails again on a slow link |
| Bundled snapshot | no network | data is as old as the metacrafter release until the first refresh |

The bundled snapshot is the only option that removes the network from the first scan.
gzip and the deadline bound the later refresh, which still runs on the scan path.

## Decisions

- **Seed the cache, do not read the bundle in place.** Copying the bundled snapshot into the
  cache gives one code path for later scans (fresh cache, then a refresh after the TTL). A
  bundled snapshot read in place would either never be refreshed or would make every scan
  try the network first.
- **Only the default base URL.** The bundled data describes `registry.apicrafter.io`. A
  self-hosted registry may hold other records, so its cold cache still downloads.
- **Version from `ecosystem.lock`.** The lock already records the registry version the
  baselines were made against. The bundled snapshot is that version, so tests, CI and a new
  installation start from the same data.
- **Mark a failed refresh as checked.** Without this, a link that cannot finish the
  download within the deadline makes every scan wait for the deadline. The cache index
  `mtime` already means "checked against the network" (an unchanged manifest touches it).
- **Deadline by chunked reads.** `_http_get` reads in chunks and checks a monotonic
  deadline. The socket timeout of each read is the smaller of the per-read timeout and the
  time left. DNS lookup is not covered by the deadline.
- **Compression.** gzip, not xz, for the bundled `datatypes.jsonl`: 542 KB against 385 KB,
  but the same format the server sends, and both are in the standard library.

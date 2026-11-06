## Context

This change implements W05, W10; Q08, Q16 from the reviewed plan. It is authorized by the user request to create OpenSpecs and implement them.

## Decisions

Use shared Python helpers and validated plain report dictionaries for compatibility. Prefer deterministic local fixtures and fake external clients for verification. Keep optional integrations lazily imported and preserve existing public entry points.

## Migration

Retain legacy report readers and add versioned output with explicit source and sampling metadata. Document changed validation and privacy defaults. New product tools consume saved reports without external writes.

## Verification

Each normative scenario is covered by focused regression tests, followed by the isolated repository suite and a built-wheel smoke check. External catalog compatibility is verified only to the extent of mocked contract tests.

# Change: Registry-derived detectors from pattern records

## Why

Some datatypes are only recognisable through their patterns: ISO 639 language codes are registry
patterns (`iso639_1`, `iso639_2`, `semantic_type: language`), country codes are
`countrycode_alpha2/3` (`semantic_type: country`). `add_registry_detectors` used only
`kind: datatype` records, so GeoNode metadata columns `language` = `eng` and `owner.language` =
`en` got no match (apicrafter/metacrafter#39).

## What Changes

- Detectors are built from pattern records with a `semantic_type`, under the same `stable`/`all`
  rules as datatype records. Their key is the canonical `semantic_type`; matches carry the pattern
  id in `pattern`. A pattern covered by a data rule is skipped.
- The `semantic_type` id counts as field-name evidence of weight 1.
- Background broadness adds two classes: two-letter codes and letter-and-digit codes. Without them
  `^[A-Z]{2}$` looked distinctive, and the tightened `flightnumber` regexp matched `WGS84`, `EA1`.

Measured with a local registry build where `iso639_1`/`iso639_2` are stable: `armsis` 47 correct /
1 false (field recall 0.88 → 0.92); seven catalogues +8 matches (`language`, `wkt`), none removed.

## Impact

- Affected specs: `detection-engine`
- Affected code: `metacrafter/engine/registry_detectors.py`, `metacrafter/engine/background.py`,
  `metacrafter/classify/processor.py`
- Needs registry patterns marked stable to take effect for ISO 639 (metacrafter-registry PR) and a
  bundled snapshot that contains them.

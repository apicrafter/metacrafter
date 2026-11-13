# Change: Score field-name matches by how well the values fit

## Why

A field-name-only match scored `60 x priority weight` whatever the values held
(apicrafter/metacrafter#111). On metacrafter-testdata every name-rule label of priority 1 had
confidence 60, right or wrong, so a confidence threshold could only switch name rules off: a
field `email` of e-mail addresses and a key `year` that holds word positions (`58`, `226`)
scored the same. Precision of name-rule labels did not move with the threshold.

## What Changes

- A field-name match keeps `60 x priority weight` as its base and is moved by the fit of the
  sampled values to its datatype:
  - up to `+30` (`FIT_BONUS`) when a specific check of the datatype passes the values;
  - up to `-10` (`MISFIT_PENALTY`) when the values fail even a broad check of its shape.
- The checks of a datatype are its value rules (imprecise ones included), the regexps of its
  registry record and of the registry pattern records whose `semantic_type` is the datatype
  (with their checksums), and the validator of the field rule. Each check is run on up to 200
  distinct non-empty values.
- Each check has a background broadness: the share of a class of ordinary tokens (integers by
  length, plain and title-case words, acronyms, two-letter and letter-and-digit codes) that it
  accepts (`metacrafter/engine/background.py`, `check_broadness`).
- `support = max(pass rate x (1 - broadness))`;
  `misfit = (1 - share of values that pass any shape check) x (broadness of the broadest shape check)`.
  Dictionary rules (`match: text`) add support only.
- A datatype without checks (`name`, `category`, `id`) keeps `60 x priority weight`.
- The match evidence gains `value_fit` (`considered_count`, `checks`, `support`, `misfit`,
  `check`, `rate`, `broadness`, `shape_rate`, `shape_broadness`).
- `--legacy-scoring` is unchanged (100 for field-name matches).

Not breaking: the default threshold (50) keeps every priority 1 name match (a misfit lowers it
to 50 at most), and `--confidence 60` now drops the misfits.

## Impact

- Affected specs: `detection-engine` (Unified Confidence Scoring; new requirement Value Fit of
  Field-Name Matches)
- Affected code: `metacrafter/engine/scoring.py` (`field_score`), `metacrafter/engine/background.py`,
  `metacrafter/classify/processor.py` (`_value_fit`, `_fit_check`, `_registry_fit_checks`)
- Reports: confidence of field-name matches changes; `evidence.value_fit` is new (the report
  schema allows additional evidence properties)
- Measured on metacrafter-testdata (279 files, pinned rules b86d0b9): precision / recall at the
  default threshold 0.946 / 0.425 -> 0.951 / 0.424; at 60, 0.947 / 0.424 -> 0.957 / 0.418; at
  70, 0.972 / 0.231 -> 0.974 / 0.249
- Implemented in apicrafter/metacrafter#112

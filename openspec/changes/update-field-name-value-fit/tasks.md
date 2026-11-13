## 1. Scoring

- [x] 1.1 `field_score(priority, legacy, fit)`: `60 x priority weight + 30 x support - 10 x misfit`, clamped to 0..100
- [x] 1.2 `check_broadness`: background token classes and the share of each that a check accepts
- [x] 1.3 `RulesProcessor._value_fit`: checks from value rules (imprecise included), registry record and pattern regexps with checksums, the field rule validator; up to 200 distinct values
- [x] 1.4 `evidence.value_fit` on field-name matches; none for datatypes without checks

## 2. Tests and docs

- [x] 2.1 `tests/test_value_fit.py`: support, misfit, dictionary rules, registry regexps, legacy scoring, the formula
- [x] 2.2 CHANGELOG, scoring docs (`docs/docs`), module docstring of `metacrafter/engine/scoring.py`
- [x] 2.3 metacrafter-testdata precision / recall by confidence threshold before and after

## 3. Release

- [ ] 3.1 Archive this change after the release that ships it

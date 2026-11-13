## Context

Field-name rules are the larger half of metacrafter's labels on real files, and before this
change their confidence carried no information: `60 x priority weight`. Value rules already
scored by the share of values they matched. The registry has regexps and checksums for many
datatypes that have no value rule, and the rule packs hold imprecise value rules that are too
broad to label a field alone but can still confirm a name.

## Goals / Non-Goals

- Goals: a confidence of a name match that rises when the values confirm the datatype and
  falls when they contradict it; no lost labels at the default threshold; evidence that says
  which check decided.
- Non-Goals: dropping name matches by values (rules do that with a field-rule `validator`);
  scoring datatypes that have no value checks; changing value-rule scores.

## Decisions

- Decision: support uses `pass rate x (1 - broadness)`, not the pass rate alone. A check such
  as `\d{4}` passes most years but also most counters; its pass rate says little. Broadness
  is measured on classes of ordinary tokens, not on registry examples, because registry
  examples are long distinctive identifiers that make every short pattern look specific.
- Decision: misfit counts only values that fail every shape check, weighted by the broadness
  of the broadest one. A value that fails a narrow check is weak evidence (the datatype has
  other forms); a value that fails a check that accepts almost anything of that shape is
  strong evidence.
- Decision: the penalty is 10, the bonus 30. With a penalty of 30, 17 to 20 right labels of
  metacrafter-testdata fell below the default threshold: the checks miss valid forms (phone
  numbers in brackets, country names where the registry has country codes, decimal commas).
  With 10, a priority 1 misfit sits at 50: kept by default, dropped at `--confidence 60`.
- Decision: dictionary rules (`match: text`) add support only. A dictionary of city names that
  misses a value says little against the name `city`.
- Decision: up to 200 distinct values. The checks run once per distinct value; the head sample
  is already bounded by `--limit`.
- Alternatives considered: a learned weighting (no labelled corpus at release time besides
  the test data); treating registry regexps as value rules (they would label fields without
  a name and repeat the false positives the detector gates exist for).

## Risks / Trade-offs

- A datatype whose checks are all narrow (one national format) gives a misfit to valid
  values of another country. Mitigation: the misfit uses the broadest shape check, and the
  penalty is small.
- Broadness is computed once per check and cached per processor; the first scan pays for it
  (a few milliseconds per check).

## Migration Plan

None: report and API shapes are unchanged except the added `evidence.value_fit`. Users who
filtered name matches with `--confidence 60` to drop all of them should use `--confidence 61`
or rule filters instead.

## Open Questions

- Should the misfit penalty grow with the number of considered values (a misfit on 3 values
  is weaker evidence than on 200)?

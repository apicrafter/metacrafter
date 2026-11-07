## ADDED Requirements

### Requirement: Pattern-Record Detectors

The engine SHALL build registry-derived detectors from pattern records (`kind: pattern`) that have
a regexp and a `semantic_type`, under the same `--registry-detectors` rules as datatype records,
unless a data rule targets the pattern id. A pattern detector SHALL report the canonical
`semantic_type` as the datatype and the pattern id in the match's `pattern`. The `semantic_type`
id SHALL count as field-name evidence of weight 1.

Background broadness SHALL also include a class of two-letter codes and a class of
letter-and-digit codes.

#### Scenario: Language codes

- **WHEN** a stable pattern `iso639_2` with `semantic_type: language` has a generic three-letter regexp
- **AND** a column `language` contains `eng` and `rus`
- **THEN** the column reports datatype `language` with `pattern: iso639_2`

#### Scenario: Generic pattern without the datatype name

- **WHEN** the same detector runs on a column `workspace` with three-letter words
- **THEN** the column reports no match for it

#### Scenario: Letter-and-digit codes

- **WHEN** a column contains `WGS84` and `EA1` and its name does not name `flightnumber`
- **THEN** the column reports no `flightnumber` match

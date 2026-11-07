## ADDED Requirements

### Requirement: LLM Verification of Value Matches

When LLM verification is enabled (`--llm-verify`, `llm_verify: true`), Metacrafter SHALL send one LLM
request per table after rule matching. The request SHALL list all column paths and, for each column with
value matches from rules or registry detectors, its type, distinct-value count, value samples under the
sample policy and the candidate datatypes with their registry descriptions. The answer schema SHALL allow
only each column's candidate ids. Value matches whose datatype the answer does not keep SHALL be removed;
kept matches SHALL carry `evidence.llm_verified: true`; a column missing from the answer SHALL keep its
matches. Field-name matches SHALL NOT be sent or removed. Answers SHALL be cached, requests SHALL count
against `llm_max_requests`, and failures SHALL follow `llm_fallback`.

#### Scenario: Rejected value match

- **WHEN** a column `package_id` of GUIDs has a `uuid` value match and the verifier does not keep `uuid`
- **THEN** the report has no `uuid` match for the column
- **AND** with `--explain` the rejected candidates give the reason "Rejected by the LLM verifier"

#### Scenario: Names-only policy

- **WHEN** verification runs with `--sample-policy names-only`
- **THEN** the request contains column paths and candidates but no values

#### Scenario: Failure with rules fallback

- **WHEN** the verification request fails and `llm_fallback` is `rules`
- **THEN** the rule matches are kept and the report is marked incomplete

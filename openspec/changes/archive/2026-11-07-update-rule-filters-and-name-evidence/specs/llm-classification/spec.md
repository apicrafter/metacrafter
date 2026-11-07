## ADDED Requirements

### Requirement: LLM Request Budget

A scan SHALL make at most `llm_max_requests` LLM requests (default 100), set in
`.metacrafter` or with `--llm-max-requests`. Cached answers and fields without registry
candidates SHALL NOT count. When the fields to classify exceed the remaining budget, the
scan SHALL log a warning. When the budget is exhausted, a scan with `llm_fallback: error`
SHALL fail with a message that names the budget, `llm_max_requests` and
`--llm-fallback rules`; a scan with `llm_fallback: rules` SHALL classify the remaining
fields with rules.

#### Scenario: More fields than the budget

- **WHEN** a scan with `--llm-only` and `--llm-max-requests 1` needs two LLM requests
- **THEN** it fails with "LLM request budget of 1 requests exceeded"

#### Scenario: Budget from configuration

- **WHEN** `.metacrafter` sets `llm_max_requests: 250`
- **THEN** the scan allows 250 LLM requests unless `--llm-max-requests` is given

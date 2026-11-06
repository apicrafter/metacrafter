# Change: Opt-in LLM verification of rule value matches

## Why

Hybrid mode asks the LLM only for fields that rules left unlabelled; it cannot remove false matches
(apicrafter/metacrafter#40). Some value matches fit a pattern but not the context: a CKAN `state` is a
record status, `resources.size` values are not UIC codes. A prototype that let the model keep or reject
rule candidates removed every false match on hand-labelled GeoNode metadata for 0.001–0.025 USD per table.

## What Changes

- `--llm-verify` on `scan file`, `llm_verify` in `.metacrafter`, `CrafterCmd(llm_verify=True)`.
- After rules (and before hybrid fallback), one request per table: all column paths; for each column with
  value matches (`ruletype` `data` or `registry`) its type, distinct count, samples under the sample policy
  (names-only: none; redacted: masked; raw: masked when a candidate is PII and the provider is remote unless
  `llm_send_pii_values`) and the candidate datatypes with registry name and description.
- The answer schema allows only each column's candidate ids. Matches not kept are removed (listed in
  `--explain`); kept ones get `evidence.llm_verified: true`. A column missing from the answer keeps its
  matches. Field-name, field-type and schema matches are never sent or removed.
- Answers are cached by prompt, model and registry version; requests count against `llm_max_requests`;
  failures follow `llm_fallback`. The report gets an `llm_verifier` summary.
- `BaseLLMProvider.complete_json` returns any JSON object (classification answers keep `classify`).

## Impact

- Affected specs: `llm-classification`
- Affected code: `metacrafter/classify/llm/verifier.py` (new), `metacrafter/classify/llm/providers/base.py`,
  `metacrafter/scan_service.py`, `metacrafter/config.py`, `metacrafter/cli/commands.py`

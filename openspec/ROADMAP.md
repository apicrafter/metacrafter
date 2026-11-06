# Metacrafter Roadmap

Updated 2026-11-05. Source plan: [`devdocs/MODERNIZATION_PLAN.md`](../devdocs/MODERNIZATION_PLAN.md)
(registry-driven modernization, target release 1.0.0). Decisions in section 6 of that plan
were accepted on 2026-11-05.

This roadmap reconciles three planning rounds:

| Round | Where | State |
|---|---|---|
| July 2026 ecosystem review | 7 changes, untracked in the main checkout (`openspec/changes/`) | Superseded; see the table at the end |
| 2026-10-01 Codex repository review | [`REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md`](../REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md) and 8 changes, all in this branch (copied from branch `codex/repository-review-2026-10-01` and its worktree). Beads `metacrafter-2` … `metacrafter-13`. | Reliability fixes; kept as prerequisites |
| 2026-11-05 modernization plan | this branch, 7 changes | Registry, engine, report, agents, contract |

Beads epic: `metacrafter-76s`.

> **Uncommitted implementation.** As of 2026-11-05 the Codex worktree
> (`~/.codex/worktrees/repository-review/metacrafter`) holds about 2000 lines of
> uncommitted code for the Codex changes. It includes a deleted `core.py`, new `scan_service.py`, `sources.py`,
> `reports.py`, `sampling.py`, `privacy.py`, `product_tools.py`, and a vendored
> `metacrafter/builtin_rules/`. Their task lists are still unchecked. The vendored rules
> contradict accepted decision 2 (see `refactor-metacrafter-foundation`); drop them when
> that work is committed.

**Approval gate:** Do not implement a change until its proposal is approved.

## Execution plan (2026-11-05)

One ordered list that merges the July roadmap, the Codex review (its uncommitted
implementation is imported into this branch), the Beads issues and the November plan.
Each step ends with a green test suite and a commit.

| Step | Work | Changes | Beads |
|---|---|---|---|
| S1 | Import the Codex implementation; make tests hermetic and green | `update-hermetic-quality-gates`, July `improve-metacrafter-ci` | `metacrafter-10` |
| S2 | Foundation: Python >=3.10, pydantic v2, extras, `metacrafter-rules` required, no vendored rules | `refactor-metacrafter-foundation`, `update-rule-loading-and-packaging`, July `modernize-metacrafter-packaging` | `metacrafter-76s.1`, `metacrafter-3` |
| S3 | Verify and complete the Codex reliability changes against their specs | `improve-scan-result-integrity`, `update-source-scanning-and-sampling`, `refactor-scan-service-and-api`, `update-rule-loading-and-packaging` | `metacrafter-2`, `-4`, `-5`, `-6`, `-7`, `-8`, `-11` |
| S4 | Registry adapter, canonical ids, enrichment, PII, provenance | `add-registry-integration` | `metacrafter-76s.2`, `metacrafter-13` |
| S5 | Rule format v2, regex/registry match, checksums, derived detectors, scoring | `update-detection-engine` | `metacrafter-76s.3` |
| S6 | Report v2, schema, legacy format, crosswalk and Frictionless export; review product tools | `add-report-v2`, `add-report-and-rule-product-tools` | `metacrafter-76s.4`, `metacrafter-12` |
| S7 | FastAPI HTTP v2, MCP server, skills | `add-agent-interfaces` | `metacrafter-76s.5` |
| S8 | LLM: trust fixes, registry shortlist, constrained output, disclosure | `update-llm-trust-and-offline-index`, `update-llm-classification` | `metacrafter-9`, `metacrafter-76s.6` |
| S9 | Contract tests, `ecosystem.lock`, scheduled check, benchmark | `add-ecosystem-contract-tests`, July `add-ecosystem-alignment-workflow` | `metacrafter-76s.7` |
| S10 | Docs, CHANGELOG, version, archive delivered changes | all | epic `metacrafter-76s` |

**Status (2026-11-06):** steps S1–S10 and `update-ecosystem-distribution` are merged
(apicrafter/metacrafter#29, apicrafter/metacrafter#30). Every change listed below is archived in
`openspec/changes/archive/2026-11-06-*`; the current requirements are in `openspec/specs/`.
The superseded `update-registry-resolution` draft was deleted. Version `1.0.0` is prepared;
publishing it to PyPI is a maintainer action.

Open items (Beads):

| Item | Beads | Owner |
|---|---|---|
| Publish metacrafter 1.0.0 to PyPI and tag the release | `metacrafter-adq` | maintainer |
| Archive the `metacrafter-llm` repository | `metacrafter-adq` | maintainer |
| Remove the `codex/repository-review-2026-10-01` branch and the untracked July drafts in the main checkout | `metacrafter-adq` | maintainer |
| Fix unresolved rule keys, rules that accept negative examples, hyphenated ISBN, `esmonthbydatafiltered`; attach a release archive asset | `metacrafter-ych` | metacrafter-rules |
| `--pack` once the registry implements `add-datatype-packs` | `metacrafter-utm` | metacrafter-registry |

Distribution (decided 2026-11-05, `update-ecosystem-distribution`): neither
`metacrafter-rules` nor `metacrafter-registry` is published on PyPI. The rule pack is
downloaded from a pinned GitHub release; the registry is read from its website. Since
`update-bundle-registry-snapshot` (2026-11-06) the package ships the registry snapshot
recorded in `ecosystem.lock`, so the first scan does not download the registry.

Blocked outside this repository:
- **Archive the `metacrafter-llm` repository.** Owner action.
- **Fix the 32 unresolved rule keys** in `metacrafter-rules`.

## Phase 0 — Foundation and reliability

| Change | Origin | Beads | Notes |
|---|---|---|---|
| `update-hermetic-quality-gates` | Codex | `metacrafter-10` | Start here: deterministic tests gate everything else |
| `improve-scan-result-integrity` | Codex | `metacrafter-2`, `metacrafter-5` | Versioned report container used by `add-report-v2` |
| `update-rule-loading-and-packaging` | Codex | `metacrafter-3`, `metacrafter-7` | Its `Packaged defaults` requirement is superseded by `refactor-metacrafter-foundation` |
| `refactor-scan-service-and-api` | Codex | `metacrafter-6`, `metacrafter-11` | Shared scan service; HTTP is re-hosted on FastAPI in `add-agent-interfaces` |
| `update-source-scanning-and-sampling` | Codex | `metacrafter-4`, `metacrafter-8` | SQL correctness and bounded sampling |
| `refactor-metacrafter-foundation` | Nov plan | `metacrafter-76s.1` | Python ≥3.10, pydantic v2, extras, `metacrafter-rules` required, `rules/` removed |

## Phase 1 — Registry as source of truth

| Change | Origin | Beads | Notes |
|---|---|---|---|
| `add-registry-integration` | Nov plan | `metacrafter-76s.2` (covers `metacrafter-13`) | Supersedes Codex `update-registry-resolution`; needs the registry on PyPI |

## Phase 2 — Detection engine v2

| Change | Origin | Beads | Notes |
|---|---|---|---|
| `update-detection-engine` | Nov plan | `metacrafter-76s.3` | Rule format v2, `regex`/`registry` match, checksum validators, derived detectors, scoring with evidence |

## Phase 3 — Report v2 and exports

| Change | Origin | Beads | Notes |
|---|---|---|---|
| `add-report-v2` | Nov plan | `metacrafter-76s.4` | Typed report v2, schema, legacy format, crosswalk and Frictionless export |
| `add-report-and-rule-product-tools` | Codex | `metacrafter-12` | Report diff, PII inventory, policy checks, rule tooling; consumes report v2 |

## Phase 4 — Agents and LLM

| Change | Origin | Beads | Notes |
|---|---|---|---|
| `update-llm-trust-and-offline-index` | Codex | `metacrafter-9` | Result validation, secret redaction, local embeddings; prerequisite |
| `update-llm-classification` | Nov plan | `metacrafter-76s.6` | Registry shortlist, corpus fingerprint, constrained output, PII-aware disclosure |
| `add-agent-interfaces` | Nov plan | `metacrafter-76s.5` | MCP server, skills, FastAPI HTTP v2, llms.txt |

## Phase 5 — Ecosystem contract (parallel to phases 1–4)

| Change | Origin | Beads | Notes |
|---|---|---|---|
| `add-ecosystem-contract-tests` | Nov plan | `metacrafter-76s.7` | Key resolution gate, registry-example rule tests, `ecosystem.lock`, weekly check, benchmark |

## Releases

| Version | Content |
|---|---|
| 0.1.0 | Phase 0 |
| 0.2.0 | Phase 1 + key resolution gate |
| 0.3.0 | Phase 2 + example-based rule tests |
| 1.0.0 | Phases 3–4; report v2 by default |

## Cross-repository prerequisites

- **metacrafter-registry:**
  - publish the package on PyPI (owner action);
  - move site-build dependencies to a `build` extra;
  - release v2.0.0;
  - implement `add-datatype-packs`;
  - expose regexp specificity in the API.
- **metacrafter-rules:**
  - fix 32 unresolved keys;
  - adopt rule format v2;
  - move to `pyproject.toml`;
  - fix 10 failing validator tests;
  - commit the untracked CI workflow.

## Superseded and closing changes

| Change | Origin | Replaced by / action |
|---|---|---|
| `add-rules-optional-extra` | July (tracked) | 19/20 done; archive after the `metacrafter-rules` release. Built-in-first ordering superseded by `refactor-metacrafter-foundation`. |
| `stabilize-metacrafter-reliability` | July (untracked) | 9/10 done; commit `metacrafter/output.py` (task 4.1), then archive |
| `improve-metacrafter-ci` | July (untracked) | `update-hermetic-quality-gates` |
| `add-ecosystem-alignment-workflow` | July (untracked) | `add-ecosystem-contract-tests`; `scripts/ecosystem_check.py` folded into `rules check` |
| `refactor-metacrafter-core` | July (untracked) | `refactor-scan-service-and-api` |
| `modernize-metacrafter-packaging` | July (untracked) | `refactor-metacrafter-foundation` (extras, Python); `add-report-v2` (typed models) |
| `expand-metacrafter-llm-platform` | July (untracked) | `add-registry-integration`, `add-agent-interfaces`, `update-llm-classification` |
| `add-ecosystem-product-features` | July (untracked) | `add-report-and-rule-product-tools`, `add-ecosystem-contract-tests`, `update-detection-engine` |
| `update-registry-resolution` | Codex | `add-registry-integration`; do not implement, delete after `add-registry-integration` is approved |

## Working with OpenSpec

```bash
openspec list
openspec show add-registry-integration
openspec validate add-registry-integration --strict
```

After a change is deployed, archive it in a separate PR with `openspec archive <id>`.

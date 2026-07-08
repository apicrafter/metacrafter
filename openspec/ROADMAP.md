# Metacrafter Ecosystem Roadmap

Phased improvement plan derived from the comprehensive ecosystem review (July 2026). Each item maps to an OpenSpec change proposal in one of the three repositories.

## Phase 1: Stabilize (1–2 weeks)

Fix critical bugs, data integrity issues, and repository hygiene before adding features.

| # | Action | Change ID | Repository |
|---|--------|-----------|------------|
| 1 | Fix corrupted Spanish NIF rule | `stabilize-rules-critical-fixes` | metacrafter-rules |
| 2 | Resolve duplicate registry IDs | `stabilize-registry-data-integrity` | metacrafter-registry |
| 3 | Add Flask deps, sync requirements, commit Rich output | `stabilize-metacrafter-reliability` | metacrafter |
| 4 | Commit/revert pending rule changes, delete backups | `stabilize-rules-critical-fixes` | metacrafter-rules |
| 5 | Fix exit codes and API delimiter bug | `stabilize-metacrafter-reliability` | metacrafter |
| 6 | Extend countries.yaml / langs.yaml | `stabilize-registry-data-integrity` | metacrafter-registry |

## Phase 2: Automate (2–4 weeks)

Add CI, cross-repo alignment checks, and quality gates.

| # | Action | Change ID | Repository |
|---|--------|-----------|------------|
| 7 | Add CI to rules and registry repos | `automate-rules-ci`, `automate-registry-ci` | rules, registry |
| 8 | Cross-repo ID alignment check | `add-ecosystem-alignment-workflow` | metacrafter |
| 9 | Fix 127 rulekey collisions | `fix-rules-key-collisions` | metacrafter-rules |
| 10 | Enforce lint/coverage in metacrafter CI | `improve-metacrafter-ci` | metacrafter |
| 11 | Mock registry in metacrafter tests | `improve-metacrafter-ci` | metacrafter |

## Phase 3: Refactor (1–2 months)

Structural improvements, test coverage, and alignment gap closure.

| # | Action | Change ID | Repository |
|---|--------|-----------|------------|
| 12 | Split CrafterCmd god object | `refactor-metacrafter-core` | metacrafter |
| 13 | Resolve core.py / core/ naming | `refactor-metacrafter-core` | metacrafter |
| 14 | Migrate to pyproject.toml with extras | `modernize-metacrafter-packaging` | metacrafter |
| 14b | Optional `metacrafter[rules]` extra + auto rulepath | `add-rules-optional-extra` | metacrafter, metacrafter-rules |
| 15 | Add validator tests for RU/US | `improve-rules-test-coverage` | metacrafter-rules |
| 16 | Fix enrichment placeholder strategy | `improve-registry-data-quality` | metacrafter-registry |
| 17 | Close 107+43 alignment gaps | `close-registry-rules-gaps` | metacrafter-registry |

## Phase 4: Expand (ongoing)

New platform features and content expansion.

| # | Action | Change ID | Repository |
|---|--------|-----------|------------|
| 18 | LLM support in API server | `expand-metacrafter-llm-platform` | metacrafter |
| 19 | Local embedding backend | `expand-metacrafter-llm-platform` | metacrafter |
| 20 | Wire RegistryClient into scan output | `expand-metacrafter-llm-platform` | metacrafter |
| 21 | PII packs for DE, CA, IT | `expand-rules-content` | metacrafter-rules |
| 22 | Registry search API + web UI filters | `expand-registry-platform` | metacrafter-registry |
| 23 | Typed scan result models | `modernize-metacrafter-packaging` | metacrafter |
| 24 | Rule performance metrics | `expand-metacrafter-llm-platform` | metacrafter |

## Feature Opportunities (Section 6)

| Feature | Change ID | Repository |
|---------|-----------|------------|
| Scan report diff, batch PII report, plugins | `add-ecosystem-product-features` | metacrafter |
| Rule suggestion from samples | `add-ecosystem-product-features` | metacrafter |
| Registry-driven rule generation | `add-ecosystem-product-features` | metacrafter |

---

## All OpenSpec Changes by Repository

### metacrafter (`openspec/changes/`)

| Change ID | Phase | Summary |
|-----------|-------|---------|
| `stabilize-metacrafter-reliability` | 1 | Flask deps, exit codes, API fix, Rich output |
| `improve-metacrafter-ci` | 2 | Lint enforcement, coverage, mock registry |
| `add-ecosystem-alignment-workflow` | 2 | Cross-repo ID validation CI |
| `refactor-metacrafter-core` | 3 | Split god object, fix core naming |
| `modernize-metacrafter-packaging` | 3–4 | pyproject.toml, typed API, public exports |
| `add-rules-optional-extra` | 3 | `metacrafter[rules]` extra, auto rulepath discovery |
| `expand-metacrafter-llm-platform` | 4 | Server LLM, local embeddings, registry enrichment |
| `add-ecosystem-product-features` | 4+ | Scan diff, PII batch, plugins |

### metacrafter-rules (`openspec/changes/`)

| Change ID | Phase | Summary |
|-----------|-------|---------|
| `stabilize-rules-critical-fixes` | 1 | es_tax fix, backup cleanup, commit pending |
| `fix-rules-key-collisions` | 1–2 | 127 rulekey collisions |
| `automate-rules-ci` | 2 | pytest, YAML lint, compile check |
| `improve-rules-test-coverage` | 3 | RU/US validator tests |
| `expand-rules-content` | 3–4 | DE/CA/IT PII, README, registry sync |

### metacrafter-registry (`openspec/changes/`)

| Change ID | Phase | Summary |
|-----------|-------|---------|
| `stabilize-registry-data-integrity` | 1 | Duplicates, reference data, category fixes |
| `automate-registry-ci` | 2 | validate + build + diff CI |
| `improve-registry-data-quality` | 3 | Honest metrics, schema hardening |
| `close-registry-rules-gaps` | 3 | 107+43 alignment gaps |
| `expand-registry-platform` | 4 | Search API, health endpoint, UI |

---

## Working with OpenSpecs

```bash
# List all active changes in a repo
cd metacrafter && openspec list

# View a specific change
openspec show stabilize-metacrafter-reliability

# Validate before requesting approval
openspec validate stabilize-metacrafter-reliability --strict

# Implement (after approval): follow tasks.md in each change directory
```

**Approval gate:** Do not implement until the relevant change proposal is reviewed and approved.

**Implementation order:** Complete Phase 1 changes across all repos before starting Phase 2.

"""Offline report workflows, rule authoring, evaluation and dbt plans."""

import json
import time
from pathlib import Path
from typing import List, Optional

import typer
import yaml

from metacrafter.classify.processor import RulesProcessor
from metacrafter.reports import report_sources


def load_document(path):
    with open(path, encoding="utf8") as stream:
        return (
            yaml.safe_load(stream)
            if str(path).endswith((".yml", ".yaml"))
            else json.load(stream)
        )


def emit(payload, output=None):
    text = (
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            default=str,
            allow_nan=False,
        )
        + "\n"
    )
    if output:
        Path(output).write_text(text, encoding="utf8")
    else:
        typer.echo(text, nl=False)


def field_index(payload):
    result = {}
    for report in report_sources(payload):
        for field in report["data"]:
            key = (report["source"], field["field"])
            if key in result:
                raise ValueError("Duplicate source/field identity: " + str(key))
            result[key] = field
    return result


def diff_reports(old, new):
    left, right = field_index(old), field_index(new)
    changes = []
    for source, path in sorted(set(left) | set(right)):
        key = (source, path)
        base = {"source": source, "field": path}
        if key not in left or key not in right:
            changes.append(
                dict(base, kind="field_added" if key in right else "field_removed")
            )
            continue
        before, after = left[key], right[key]
        for attribute in ("ftype",):
            if before.get(attribute) != after.get(attribute):
                changes.append(
                    dict(
                        base,
                        kind=attribute,
                        before=before.get(attribute),
                        after=after.get(attribute),
                    )
                )
        previous = {m["dataclass"]: m for m in before.get("matches", [])}
        current = {m["dataclass"]: m for m in after.get("matches", [])}
        for datatype in sorted(set(previous) | set(current)):
            if datatype not in previous or datatype not in current:
                changes.append(
                    dict(
                        base,
                        kind=(
                            "datatype_added"
                            if datatype in current
                            else "datatype_removed"
                        ),
                        datatype=datatype,
                    )
                )
            elif bool(previous[datatype].get("is_pii")) != bool(
                current[datatype].get("is_pii")
            ):
                changes.append(
                    dict(
                        base,
                        kind="pii",
                        datatype=datatype,
                        before=bool(previous[datatype].get("is_pii")),
                        after=bool(current[datatype].get("is_pii")),
                    )
                )
        for name in sorted(set(before.get("stats", {})) | set(after.get("stats", {}))):
            a, b = before.get("stats", {}).get(name), after.get("stats", {}).get(name)
            if (
                isinstance(a, (int, float))
                and isinstance(b, (int, float))
                and not isinstance(a, bool)
                and not isinstance(b, bool)
                and a != b
            ):
                changes.append(
                    dict(
                        base,
                        kind="statistic",
                        statistic=name,
                        before=a,
                        after=b,
                        delta=b - a,
                    )
                )
    old_sources = {r["source"]: r for r in report_sources(old)}
    warnings = []
    for report in report_sources(new):
        prior = old_sources.get(report["source"])
        if prior:
            for key in ("sampling", "rules_version", "effective_mode"):
                if prior.get(key) != report.get(key):
                    warnings.append(
                        {"source": report["source"], "reason": "Incompatible " + key}
                    )
            if not prior.get("complete") or not report.get("complete"):
                warnings.append(
                    {"source": report["source"], "reason": "Incomplete scan"}
                )
    return {"changes": changes, "warnings": warnings, "changed": bool(changes)}


def pii_inventory(payload, min_confidence=0):
    from metacrafter.reports import validate_options

    validate_options(1, min_confidence)
    sources = report_sources(payload)
    rows = []
    for report in sources:
        for field in report["data"]:
            for match in field.get("matches", []):
                if match.get("is_pii") and match.get("confidence", 0) >= min_confidence:
                    rows.append(
                        {
                            "source": report["source"],
                            "field": field["field"],
                            "datatype": match["dataclass"],
                            "confidence": match["confidence"],
                            "evidence": {
                                "ruleid": match.get("ruleid"),
                                "reason": None,
                            },
                            "suggested_severity": (
                                "high" if match["confidence"] >= 90 else "review"
                            ),
                            "sampling": report.get("sampling"),
                            "complete": report["complete"],
                        }
                    )
    complete = all(r["complete"] for r in sources) and (
        not isinstance(payload, dict) or payload.get("complete", True)
    )
    return {
        "pii_fields": sorted(
            rows, key=lambda r: (r["source"], r["field"], r["datatype"])
        ),
        "complete": complete,
        "errors": [e for r in sources for e in r.get("errors", [])]
        or (payload.get("errors", []) if isinstance(payload, dict) else []),
    }


def check_policy(payload, policy):
    if not isinstance(policy, dict):
        raise ValueError("Policy must be an object")
    allowed = {
        "prohibit_pii",
        "prohibited_datatypes",
        "required_labels",
        "min_confidence",
        "require_complete",
        "unknown",
    }
    unknown_keys = set(policy) - allowed
    if unknown_keys:
        raise ValueError("Unknown policy keys: " + ", ".join(sorted(unknown_keys)))
    for key in ("prohibit_pii", "require_complete"):
        if key in policy and not isinstance(policy[key], bool):
            raise ValueError(key + " must be boolean")
    from metacrafter.reports import validate_options

    minimum = policy.get("min_confidence", 0)
    validate_options(1, minimum)
    if policy.get("unknown", "allow") not in ("allow", "fail"):
        raise ValueError("unknown must be allow or fail")
    required = policy.get("required_labels", {})
    prohibited = policy.get("prohibited_datatypes", [])
    if not isinstance(required, dict) or not isinstance(prohibited, list):
        raise ValueError(
            "required_labels must be a mapping and prohibited_datatypes an array"
        )
    violations = []
    sources = report_sources(payload)
    if policy.get("require_complete") and (
        not sources
        or any(not r["complete"] for r in sources)
        or isinstance(payload, dict)
        and not payload.get("complete", True)
    ):
        violations.append({"code": "incomplete"})
    for report in sources:
        fields = {field["field"]: field for field in report["data"]}
        for path, labels in required.items():
            labels = [labels] if isinstance(labels, str) else labels
            if not isinstance(labels, list) or any(
                not isinstance(label, str) for label in labels
            ):
                raise ValueError("Required labels must be strings or arrays of strings")
            matches = fields.get(path, {}).get("matches", [])
            present = {m["dataclass"] for m in matches if m["confidence"] >= minimum}
            for label in sorted(set(labels) - present):
                violations.append(
                    {
                        "code": "required_label",
                        "source": report["source"],
                        "field": path,
                        "datatype": label,
                    }
                )
        for field in report["data"]:
            base = {"source": report["source"], "field": field["field"]}
            matches = field.get("matches", [])
            if not matches and policy.get("unknown") == "fail":
                violations.append(dict(base, code="unknown"))
            for match in matches:
                if match["confidence"] < minimum:
                    violations.append(
                        dict(base, code="confidence", datatype=match["dataclass"])
                    )
                if (
                    match["dataclass"] in prohibited
                    or policy.get("prohibit_pii")
                    and match.get("is_pii")
                ):
                    violations.append(
                        dict(base, code="prohibited", datatype=match["dataclass"])
                    )
    if not sources and required:
        violations.append({"code": "required_label", "message": "No sources available"})
    return {"passed": not violations, "violations": violations}


def lint_rules(path):
    from metacrafter.classify.processor import RulesProcessor

    processor = RulesProcessor()
    files = sorted(Path(path).rglob("*.yaml")) if Path(path).is_dir() else [Path(path)]
    diagnostics, identifiers = [], set()
    for filename in files:
        try:
            document = load_document(filename)
            for identifier in document.get("rules", {}):
                if identifier in identifiers:
                    diagnostics.append(
                        {
                            "ruleid": identifier,
                            "source": str(filename),
                            "error": "Duplicate rule ID",
                        }
                    )
                identifiers.add(identifier)
            processor.import_rules(str(filename))
        except (
            ValueError,
            TypeError,
            OSError,
            yaml.YAMLError,
            AttributeError,
        ) as error:
            diagnostics.append({"source": str(filename), "error": str(error)})
    diagnostics.extend(processor.diagnostics)
    return processor, diagnostics


def test_rule_examples(path, examples):
    processor, diagnostics = lint_rules(path)
    if not isinstance(examples, list):
        raise ValueError("Examples must be an array")
    failures, tested = list(diagnostics), 0
    for example in examples:
        identifier = example["ruleid"]
        rule = next(
            (
                r
                for r in processor.data_rules + processor.field_rules
                if r["id"] == identifier
            ),
            None,
        )
        if rule is None:
            failures.append({"ruleid": identifier, "error": "Rule unavailable"})
            continue
        for expected, key in ((True, "positive"), (False, "negative")):
            for value in example.get(key, []):
                tested += 1
                text = str(value)
                try:
                    matched = RulesProcessor._value_check(rule, text)[1]
                except Exception:
                    matched = False
                if matched != expected:
                    failures.append(
                        {
                            "ruleid": identifier,
                            "case": key,
                            "index": tested,
                            "error": "Unexpected match outcome",
                        }
                    )
    return {
        "passed": not failures and tested > 0,
        "tested": tested,
        "failures": failures,
    }


def draft_rule(datatype, samples, field_names=None):
    if (
        not isinstance(samples, list)
        or any(not isinstance(s, str) for s in samples)
        or not samples
    ):
        raise ValueError("Draft samples must be a non-empty array of strings")
    if not datatype or not datatype.replace("_", "").replace("-", "").isalnum():
        raise ValueError("Invalid datatype identifier")
    # A field-name draft never asserts a checksum or generalizes sample values.
    return {
        "name": "Review-required " + datatype,
        "description": "Draft; review examples and add a tested validator before activation",
        "lang": "common",
        "context": "common",
        "review_required": True,
        "provenance": {
            "sample_count": len(samples),
            "min_length": min(map(len, samples)),
            "max_length": max(map(len, samples)),
            "charset": "".join(sorted(set("".join(samples)))),
            "leading_zero": any(s.startswith("0") for s in samples),
        },
        "rules": {
            "draft_"
            + datatype: {
                "key": datatype,
                "type": "field",
                "match": "text",
                "rule": ",".join(field_names or [datatype]),
                "disabled": True,
            }
        },
        "examples": {"positive": samples, "negative": []},
    }


def evaluate_fixture(service, fixture):
    if not isinstance(fixture, list):
        raise ValueError("Evaluation fixture must be an array of labeled cases")
    metrics, abstentions = {}, 0
    started = time.perf_counter()
    rule_version = None
    for case in fixture:
        report = service.scan_data(
            case["items"], parse_dates=False, fields=case.get("fields")
        )
        rule_version = report.get("rules_version")
        predicted = {
            field["field"]: {match.get("datatype") or match["dataclass"] for match in field.get("matches", [])}
            for field in report["data"]
        }
        expected = case["labels"]
        if not isinstance(expected, dict):
            raise ValueError("labels must map field paths to datatype arrays")
        for path in set(predicted) | set(expected):
            truth = set(expected.get(path, []))
            actual = predicted.get(path, set())
            if not actual:
                abstentions += 1
            for label in truth | actual:
                score = metrics.setdefault(
                    label,
                    {"true_positive": 0, "false_positive": 0, "false_negative": 0},
                )
                score[
                    (
                        "true_positive"
                        if label in truth and label in actual
                        else "false_positive" if label in actual else "false_negative"
                    )
                ] += 1
    for score in metrics.values():
        tp, fp, fn = (
            score["true_positive"],
            score["false_positive"],
            score["false_negative"],
        )
        score.update(
            precision=tp / (tp + fp) if tp + fp else None,
            recall=tp / (tp + fn) if tp + fn else None,
        )
    return {
        "datatypes": metrics,
        "abstentions": abstentions,
        "cases": len(fixture),
        "elapsed_seconds": time.perf_counter() - started,
        "classification_mode": service.classification_mode,
        "rules_version": rule_version,
        "confidence_semantics": "evidence share, not calibrated probability",
    }


def dbt_plan(manifest, payload):
    if not isinstance(manifest, dict) or not isinstance(manifest.get("nodes"), dict):
        raise ValueError("dbt manifest requires a nodes mapping")
    indexed = field_index(payload)
    updates = []
    for identifier, node in sorted(manifest["nodes"].items()):
        if not isinstance(node, dict) or node.get("resource_type") not in (
            "model",
            "seed",
            "snapshot",
        ):
            continue
        source = (
            node.get("schema", "") + "." + (node.get("alias") or node.get("name", ""))
        )
        columns = node.get("columns", {})
        if not isinstance(columns, dict):
            raise ValueError("dbt columns must be a mapping")
        for path, column in sorted(columns.items()):
            if not isinstance(column, dict) or not isinstance(
                column.get("meta", {}), dict
            ):
                raise ValueError("dbt column and meta must be objects")
            field = indexed.get((source, path))
            if field:
                meta = dict(column.get("meta", {}))
                prior = meta.get("metacrafter", {})
                if not isinstance(prior, dict):
                    raise ValueError("Existing metacrafter metadata must be an object")
                meta["metacrafter"] = dict(
                    prior,
                    datatypes=sorted(
                        {m["dataclass"] for m in field.get("matches", [])}
                    ),
                    is_pii=any(m.get("is_pii") for m in field.get("matches", [])),
                )
                updates.append(
                    {
                        "node": identifier,
                        "column": path,
                        "declared_type": column.get("data_type"),
                        "meta": meta,
                    }
                )
    return {
        "dry_run": True,
        "adapter": "dbt-manifest",
        "manifest_version": manifest.get("metadata", {}).get("dbt_schema_version"),
        "updates": updates,
    }


def register_product_commands(app, rules_app, scan_app, export_app):
    report_app = typer.Typer(help="Compare reports and consolidate PII inventories")
    app.add_typer(report_app, name="report")

    @report_app.command("diff")
    def report_diff(
        old: Path,
        new: Path,
        output: Optional[Path] = None,
        fail_on_change: bool = False,
        fail_on_incompatible: bool = False,
        output_format: str = "json",
    ):
        result = diff_reports(load_document(old), load_document(new))
        if output_format == "json":
            emit(result, output)
        elif output_format == "table":
            from tabulate import tabulate
            rows = [[r.get("source"), r.get("field"), r.get("kind"), r.get("datatype", r.get("statistic", "")), r.get("before", ""), r.get("after", "")] for r in result["changes"]]
            rendered = tabulate(rows, headers=["Source", "Field", "Change", "Datatype/statistic", "Before", "After"]) + "\n"
            if output:
                output.write_text(rendered, encoding="utf8")
            else:
                typer.echo(rendered, nl=False)
            for warning in result["warnings"]:
                typer.echo(warning["reason"] + ": " + warning["source"], err=True)
        else:
            raise ValueError("output_format must be json or table")
        if (
            fail_on_change
            and result["changed"]
            or fail_on_incompatible
            and result["warnings"]
        ):
            raise typer.Exit(1)

    @report_app.command("pii")
    def report_pii(
        report: Path, output: Optional[Path] = None, min_confidence: float = 0
    ):
        emit(pii_inventory(load_document(report), min_confidence), output)

    @app.command("check")
    def check(
        report: Path, policy: Path = typer.Option(...), output: Optional[Path] = None
    ):
        result = check_policy(load_document(report), load_document(policy))
        emit(result, output)
        if not result["passed"]:
            raise typer.Exit(1)

    @rules_app.command("lint")
    def rules_lint(path: Path):
        processor, diagnostics = lint_rules(path)
        emit(
            {
                "passed": not diagnostics,
                "rules_loaded": len(processor.field_rules) + len(processor.data_rules),
                "diagnostics": diagnostics,
            }
        )
        if diagnostics:
            raise typer.Exit(1)

    @rules_app.command("test")
    def rules_test(
        path: Optional[Path] = typer.Argument(None, help="Rule file or directory"),
        examples: Optional[Path] = typer.Option(None, "--examples", help="Examples file (rule-local tests)"),
        registry_examples: bool = typer.Option(False, "--registry-examples", help="Test rules on registry positive and negative examples"),
        rule: Optional[List[str]] = typer.Option(None, "--rule", help="Only these rule ids (repeatable)"),
        rulepath: Optional[str] = typer.Option(None, "--rulepath", help="Extra rule paths, comma-separated"),
        registry_path: Optional[str] = typer.Option(None, "--registry-path", help="Registry snapshot or checkout data/ directory"),
        registry_detectors: str = typer.Option("stable", "--registry-detectors", help="off, checksum, stable or all (default: stable, to test registry coverage)"),
        thresholds: Optional[Path] = typer.Option(None, "--thresholds", help="Fail on regressions against this thresholds file"),
        write_thresholds: Optional[Path] = typer.Option(None, "--write-thresholds", help="Write current results as the thresholds baseline"),
        output: Optional[Path] = typer.Option(None, "--output", "-o"),
    ):
        """Test rules on examples: a local examples file, or the registry's examples (--registry-examples)."""
        if not registry_examples:
            if path is None or examples is None:
                raise typer.BadParameter("PATH and --examples are required unless --registry-examples is set")
            result = test_rule_examples(path, load_document(examples))
            emit(result, output)
            if not result["passed"]:
                raise typer.Exit(1)
            return
        from metacrafter.core import CrafterCmd
        from metacrafter.ecosystem import check_thresholds, registry_example_tests, thresholds_from_results

        paths = rulepath.split(",") if rulepath else []
        if path is not None:
            paths.append(str(path))
        service = CrafterCmd(
            rulepath=paths or None,
            quiet=True,
            registry_path=registry_path,
            registry_detectors=registry_detectors,
        )
        if not service.registry.available:
            typer.echo("Registry data is not available; pass --registry-path", err=True)
            raise typer.Exit(2)
        result = registry_example_tests(service.processor, service.registry, rule)
        if write_thresholds:
            write_thresholds.write_text(
                yaml.safe_dump(thresholds_from_results(result), sort_keys=False, allow_unicode=True),
                encoding="utf8",
            )
        failures = check_thresholds(result, load_document(thresholds)) if thresholds else []
        result["failures"] = failures
        emit(result, output)
        if failures:
            typer.echo(f"{len(failures)} rule example check(s) failed", err=True)
            raise typer.Exit(1)

    @rules_app.command("draft")
    def rules_draft(
        datatype: str,
        samples: Path = typer.Option(...),
        output: Path = typer.Option(...),
        fields: Optional[str] = None,
    ):
        payload = draft_rule(
            datatype, load_document(samples), fields.split(",") if fields else None
        )
        output.write_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
            encoding="utf8",
        )
        typer.echo(str(output))

    @scan_app.command("explain")
    def scan_explain(report: Path, include_reasons: bool = False):
        sources = report_sources(load_document(report))
        emit(
            {
                "sources": [
                    {
                        "source": r["source"],
                        "rules_version": r.get("rules_version"),
                        "sampling": r.get("sampling"),
                        "fields": [
                            {
                                "field": f["field"],
                                "rejected_candidates": f.get("rejected_candidates", []),
                                "evidence": [
                                    dict(reason=m.get("reason") if include_reasons else None, **{
                                        k: m.get(k)
                                        for k in (
                                            "ruleid",
                                            "dataclass",
                                            "confidence",
                                            "ruletype",
                                            "is_pii",
                                            "evidence",
                                        )
                                    })
                                    for m in f.get("matches", [])
                                ],
                            }
                            for f in r["data"]
                        ],
                    }
                    for r in sources
                ]
            }
        )

    @app.command("evaluate")
    def evaluate(
        fixture: Path,
        rulepath: Optional[str] = None,
        output: Optional[Path] = None,
        baseline: Optional[Path] = typer.Option(None, "--baseline", help="Fail when precision or recall drops below this baseline"),
        write_baseline: Optional[Path] = typer.Option(None, "--write-baseline", help="Write the current precision and recall as the baseline"),
        tolerance: float = typer.Option(0.02, "--tolerance", help="Allowed drop before --baseline fails"),
    ):
        """Evaluate detection on a labelled fixture (per-datatype precision, recall, time)."""
        from metacrafter.core import CrafterCmd
        from metacrafter.ecosystem import benchmark_baseline, compare_benchmark

        service = CrafterCmd(
            rulepath=rulepath.split(",") if rulepath else None, quiet=True
        )
        result = evaluate_fixture(service, load_document(fixture))
        if write_baseline:
            write_baseline.write_text(
                json.dumps(benchmark_baseline(result), indent=2, sort_keys=True) + "\n", encoding="utf8"
            )
        regressions = compare_benchmark(result, load_document(baseline), tolerance) if baseline else []
        result["regressions"] = regressions
        emit(result, output)
        if regressions:
            typer.echo(f"{len(regressions)} benchmark regression(s)", err=True)
            raise typer.Exit(1)

    @export_app.command("dbt")
    def export_dbt(
        report: Path, manifest: Path = typer.Option(...), output: Optional[Path] = None
    ):
        emit(dbt_plan(load_document(manifest), load_document(report)), output)

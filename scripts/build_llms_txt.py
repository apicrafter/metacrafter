#!/usr/bin/env python3
"""Generate llms.txt, llms-full.txt and Markdown page twins for the docs site.

Writes into docs/static/ so Docusaurus serves them as-is:
  /llms.txt          index of pages (title, description, link to the .md twin)
  /llms-full.txt     all pages concatenated
  /<page>.md         Markdown source of every page without front matter
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs" / "docs"
STATIC = ROOT / "docs" / "static"
SITE = "https://apicrafter.github.io/metacrafter"
FRONT = re.compile(r"^---\n(.*?)\n---\n", re.S)


def parse(path: Path):
    text = path.read_text(encoding="utf-8")
    meta = {}
    match = FRONT.match(text)
    if match:
        for line in match.group(1).splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                meta[key.strip()] = value.strip().strip('"')
        text = text[match.end():]
    return meta, text.strip() + "\n"


def main():
    pages = []
    for path in sorted(DOCS.rglob("*.md")):
        rel = path.relative_to(DOCS).with_suffix("")
        meta, body = parse(path)
        route = str(rel).replace("\\", "/")
        if route.endswith("/index"):
            route = route[: -len("/index")]
        twin = STATIC / (route + ".md")
        twin.parent.mkdir(parents=True, exist_ok=True)
        twin.write_text(body, encoding="utf-8")
        pages.append((route, meta.get("title", route), meta.get("description", ""), body))
    lines = [
        "# Metacrafter",
        "",
        "> Python CLI and library that labels columns of files and databases with semantic",
        "> datatypes from the Metacrafter Registry (registry.apicrafter.io) and finds personal data.",
        "",
        "Agent entry points: the MCP server (`metacrafter mcp`), the HTTP API (`/v2/classify`),",
        f"and the skills in the repository's skills/ directory. Report schema: {SITE}/schema/report-v3.schema.json",
        "",
        "## Docs",
        "",
    ]
    for route, title, description, _ in pages:
        lines.append(f"- [{title}]({SITE}/{route}.md){': ' + description if description else ''}")
    (STATIC / "llms.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    full = ["# Metacrafter documentation", ""]
    for route, title, _, body in pages:
        full += [f"<!-- {SITE}/{route} -->", body]
    (STATIC / "llms-full.txt").write_text("\n".join(full), encoding="utf-8")
    print(f"{len(pages)} pages")


if __name__ == "__main__":
    main()

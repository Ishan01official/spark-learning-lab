from __future__ import annotations

import ast
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULES = [
    "00_setup",
    "01_fundamentals",
    "02_pyspark_core",
    "03_optimization",
    "04_delta_lake",
    "05_streaming",
    "06_real_projects",
    "07_interview_prep",
    "08_notes_from_books",
    "09_latest_updates",
    "10_architecture",
    "11_case_studies",
    "12_certification_prep",
    "13_debugging_playbook",
    "14_spark_ui_lab",
    "15_databricks_production",
    "16_cloud_lakehouse",
    "17_sql_for_spark",
    "18_python_for_pyspark",
    "19_resources",
    "20_learning_strategy",
]


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def fail(errors: list[str], message: str) -> None:
    errors.append(message)


def check_required_files(errors: list[str]) -> None:
    required = [
        "README.md",
        "LICENSE",
        "BOOK_MAP.md",
        "RESOURCE_MAP.md",
        "INTERVIEW_BANK.md",
        "PROJECT_INDEX.md",
        "LEARNING_STRATEGY.md",
        "ROADMAP.md",
        "CONVENTIONS.md",
        "CONTRIBUTING.md",
        "TROUBLESHOOTING.md",
        "requirements.txt",
    ]
    for item in required:
        if not (ROOT / item).is_file():
            fail(errors, f"missing required file: {item}")

    for module in MODULES:
        readme = ROOT / module / "README.md"
        if not readme.is_file():
            fail(errors, f"missing module README: {module}/README.md")


def check_empty_markdown(errors: list[str]) -> None:
    for path in ROOT.rglob("*.md"):
        if ".git" in path.parts:
            continue
        if path.stat().st_size == 0:
            fail(errors, f"empty markdown file: {rel(path)}")


def check_python_syntax(errors: list[str]) -> None:
    for path in ROOT.rglob("*.py"):
        if any(part in {".git", ".venv", "venv", "__pycache__"} for part in path.parts):
            continue
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=rel(path))
        except SyntaxError as exc:
            fail(errors, f"python syntax error in {rel(path)}:{exc.lineno}: {exc.msg}")


def check_markdown_links(errors: list[str]) -> None:
    link_pattern = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
    for path in ROOT.rglob("*.md"):
        if ".git" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        for match in link_pattern.finditer(text):
            target = match.group(1).strip()
            if (
                target.startswith(("http://", "https://", "mailto:", "#"))
                or "://" in target
                or target == ""
            ):
                continue
            target_path = target.split("#", 1)[0]
            if not target_path:
                continue
            resolved = (path.parent / target_path).resolve()
            try:
                resolved.relative_to(ROOT)
            except ValueError:
                fail(errors, f"markdown link leaves repo in {rel(path)}: {target}")
                continue
            if not resolved.exists():
                fail(errors, f"broken markdown link in {rel(path)}: {target}")


def check_mermaid_files(errors: list[str]) -> None:
    allowed_starts = ("flowchart ", "graph ", "sequenceDiagram", "classDiagram", "erDiagram", "gantt")
    for path in ROOT.rglob("*.mmd"):
        if ".git" in path.parts:
            continue
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            fail(errors, f"empty mermaid file: {rel(path)}")
            continue
        first_line = text.splitlines()[0].strip()
        if not first_line.startswith(allowed_starts):
            fail(errors, f"mermaid file has unexpected first line in {rel(path)}: {first_line}")


def main() -> int:
    errors: list[str] = []
    check_required_files(errors)
    check_empty_markdown(errors)
    check_python_syntax(errors)
    check_markdown_links(errors)
    check_mermaid_files(errors)

    if errors:
        print("Repository validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Repository validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""실행 가능한 SQL과 schema, 검증 근거의 문서 링크를 확인한다."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

SQL_DIRS = (ROOT / "sql" / "checks", ROOT / "sql" / "lookups")
SQL_FILES = sorted(f for d in SQL_DIRS for f in d.glob("*.sql"))
DOCS = ROOT / "docs"


def _datacatalog_tables(models: object) -> list[object]:
    """공유 Base에 등록된 서비스 카탈로그 테이블은 제외한다."""
    return [
        value.__table__
        for value in vars(models).values()
        if isinstance(value, type)
        and value.__module__ == models.__name__
        and hasattr(value, "__table__")
    ]


def _sql_blocks(path: Path) -> list[str]:
    return [b.rstrip() for b in re.findall(r"```sql\n(.*?)\n```", path.read_text("utf-8"), re.S)]


@pytest.mark.parametrize("sql_path", SQL_FILES, ids=lambda p: p.stem)
def test_문서에_실린_질의는_실제_파일과_같다(sql_path: Path):
    """문서 블록이 옛 버전이면 붙여넣어도 돌지 않고, 설계 근거도 다른 질의의 것이 된다."""
    blocks = _sql_blocks(DOCS / "sql-quality-checks.md")
    assert sql_path.read_text("utf-8").rstrip() in blocks, (
        f"{sql_path.name} 이 sql-quality-checks.md 의 어느 블록과도 일치하지 않는다"
    )


def test_er_다이어그램이_모든_테이블을_담는다():
    from domains.datacatalog import models

    doc = (DOCS / "metadata-catalog.md").read_text("utf-8")
    diagram = re.search(r"```mermaid\nerDiagram(.*?)```", doc, re.S)
    assert diagram, "metadata-catalog.md 에 erDiagram 이 없다"
    drawn = set(re.findall(r"\b([a-z][a-z_]+)\b", diagram.group(1)))
    actual = {t.name[len("catalog_") :] for t in _datacatalog_tables(models)}
    assert not actual - drawn, f"다이어그램에 빠진 테이블: {sorted(actual - drawn)}"


def test_검증표가_참조하는_테스트가_실제로_있다():
    """없는 테스트를 인용하면 검증했다는 주장 자체가 근거를 잃는다."""
    doc = (DOCS / "catalog-api.md").read_text("utf-8")
    referenced = set(re.findall(r"`(test_[a-z_]+\.py)::(test_[^`\s]+)`", doc))
    assert referenced, "검증표에 테스트 참조가 없다"
    for filename, func in referenced:
        path = Path(__file__).parent / filename
        assert path.exists(), f"{filename} 이 없다"
        assert f"def {func}(" in path.read_text("utf-8"), f"{filename} 에 {func} 가 없다"

"""DAG 가 파이프라인 함수를 올바른 타입으로 부르는지.

이 파일이 없어서 DAG 가 `Path` 를 `CatalogSource` 자리에 넘기는 것을 못 잡았다.
보관 원본이 있는 날짜에서는 조기 반환으로 넘어가고 없는 날짜에서만 죽는다 —
콜드 스타트에서만 터져서 로컬 재현에서는 보이지 않았다.

저장소에서 테스트가 없던 유일한 파일에 버그가 있었다. 우연이 아니다.
"""

from __future__ import annotations

import ast
import inspect
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

DAG_FILE = ROOT / "dags" / "catalog_reconciliation_daily.py"


def test_dag이_extract_source에_어댑터를_넘긴다():
    """세 번째 인자는 경로가 아니라 CatalogSource 다."""
    tree = ast.parse(DAG_FILE.read_text("utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "extract_source"
    ]
    assert calls, "DAG 에 extract_source 호출이 없다"
    for call in calls:
        third = call.args[2]
        assert isinstance(third, ast.Call), "세 번째 인자가 어댑터 생성이 아니다"
        name = third.func.id if isinstance(third.func, ast.Name) else third.func.attr
        assert name.endswith("Source"), f"CatalogSource 가 아니라 {name} 을 넘긴다"


def test_어댑터가_프로토콜을_만족한다():
    from domains.datacatalog.sources import CollectedSource, FixtureSource

    for cls in (FixtureSource, CollectedSource):
        assert hasattr(cls, "fetch"), f"{cls.__name__} 에 fetch 가 없다"
        assert hasattr(cls, "mode"), f"{cls.__name__} 에 mode 가 없다"


def test_extract_source_시그니처가_바뀌면_알린다():
    """DAG 는 위치 인자로 부른다. 순서가 바뀌면 조용히 잘못된 값이 들어간다."""
    from domains.datacatalog.pipeline import extract_source

    params = list(inspect.signature(extract_source).parameters)
    assert params[:4] == ["source_id", "logical_date", "source", "archive_root"]

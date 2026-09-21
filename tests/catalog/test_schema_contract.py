"""필드 계약 추출과 드리프트 판정.

기능이 되는지가 아니라 어떤 사고를 막았는지로 짠다.
"""

from __future__ import annotations

from domains.datacatalog.schema_contract import (
    contract_diff,
    contract_from_payload,
    json_type_name,
    schema_hash,
)


def test_판별_불가_타입은_null_이라는_이름을_갖지_않는다():
    """None 을 "null" 로 이름 붙이면 판별 불가가 정상 타입으로 둔갑한다.

    타입을 판별하지 못한 상태가 가장 유력한 드리프트 신호다.
    """
    assert json_type_name(None) is None
    assert json_type_name("x") == "string"
    assert json_type_name(True) == "boolean"


def test_같은_계약이면_해시가_같다():
    a = contract_from_payload({"b": 1, "a": "x"})
    b = contract_from_payload({"a": "x", "b": 1})
    assert schema_hash(a) == schema_hash(b)


def test_타입이_바뀌면_해시가_달라진다():
    a = contract_from_payload({"replicas": 3})
    b = contract_from_payload({"replicas": "3"})
    assert schema_hash(a) != schema_hash(b)


def test_필드_추가와_삭제를_양방향으로_잡는다():
    """한쪽만 보면 등록에 없는데 실제로 생긴 필드를 놓친다."""
    declared = {"a": "string", "b": "integer"}
    observed = {"a": "string", "c": "integer"}
    kinds = {field: drift for field, drift, _, _ in contract_diff(declared, observed)}
    assert kinds["b"] == "MISSING_FIELD"
    assert kinds["c"] == "UNDECLARED_FIELD"


def test_타입이_판별_불가로_바뀐_경우도_드리프트다():
    """파이썬 != 비교는 SQL 의 IS DISTINCT FROM 과 같다.

    <> 를 쓰면 NULL 앞에서 NULL 이 반환돼 이 경우가 통과한다.
    """
    findings = contract_diff({"x": "string"}, {"x": None})
    assert findings == [("x", "TYPE_CHANGED", "string", None)]


def test_중첩과_배열을_경로로_펼친다():
    contract = dict(contract_from_payload({"m": {"n": 1}, "arr": [{"k": "v"}]}))
    assert contract["m.n"] == "integer"
    assert contract["arr"] == "array"
    assert contract["arr[].k"] == "string"

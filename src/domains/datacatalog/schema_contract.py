"""실제 payload 에서 필드 계약을 추출하고 결정적 해시를 만든다.

계약 해시가 필요한 이유는 04번 문서에 있다. 스키마가 바뀌면 버전을 올리는
것이 규칙인데, 규칙을 지키지 않은 변경이 진짜 문제다. 버전은 같은데 해시가
다르면 등록되지 않은 변경이 있었다는 뜻이다.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from typing import Any

# JSON 타입 이름 --------------------------------------------------------

_TYPE_NAMES: dict[type, str] = {
    bool: "boolean",
    int: "integer",
    float: "number",
    str: "string",
    list: "array",
    dict: "object",
}


def json_type_name(value: Any) -> str | None:
    """값의 타입 이름. None 은 판별 불가로 남긴다.

    None 을 "null" 로 이름 붙이지 않는 것이 중요하다. 타입을 판별하지
    못한 상태가 가장 유력한 드리프트 신호이고, 03번 검사가
    IS DISTINCT FROM 으로 그걸 잡는다. 여기서 "null" 이라는 이름을
    붙여 버리면 판별 불가가 하나의 정상 타입으로 둔갑한다.
    """
    if value is None:
        return None
    return _TYPE_NAMES.get(type(value), "string")


def flatten_fields(payload: Any, prefix: str = "") -> Iterator[tuple[str, str | None]]:
    """중첩 payload 를 (field_path, data_type) 으로 펼친다.

    배열은 원소 타입을 대표로 쓰되 첫 원소만 본다. 배열 안에서 타입이
    섞이는 경우는 이 프로젝트 자산에 없고, 전수 검사는 비용이 크다.
    이 결정의 한계는 문서에 남겨 두었다.
    """
    if isinstance(payload, dict):
        for key, value in payload.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, dict):
                yield from flatten_fields(value, path)
            elif isinstance(value, list):
                yield path, "array"
                if value:
                    yield from flatten_fields(value[0], f"{path}[]")
            else:
                yield path, json_type_name(value)
    elif isinstance(payload, list):
        if payload:
            yield from flatten_fields(payload[0], prefix)


def contract_from_payload(payload: Any) -> list[tuple[str, str | None]]:
    """payload 하나에서 필드 계약을 뽑는다. 경로 기준 정렬."""
    seen: dict[str, str | None] = {}
    for path, dtype in flatten_fields(payload):
        # 같은 경로가 여러 번 나오면 타입이 갈릴 수 있다.
        # 먼저 본 타입을 유지하되, 판별 불가(None)는 실제 타입으로 덮는다.
        if path not in seen or seen[path] is None:
            seen[path] = dtype
    return sorted(seen.items())


def schema_hash(contract: list[tuple[str, str | None]]) -> str:
    """필드 계약의 결정적 해시.

    정렬된 (경로, 타입) 목록만으로 계산한다. 값이나 순서가 아니라
    계약의 모양만 본다. 같은 계약이면 언제 계산해도 같은 해시가 나온다.
    """
    canonical = json.dumps(
        [[path, dtype] for path, dtype in contract],
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def contract_diff(
    declared: dict[str, str | None], observed: dict[str, str | None]
) -> list[tuple[str, str, str | None, str | None]]:
    """등록 계약과 관측 계약을 양방향으로 대조한다.

    (field_path, drift_type, declared_type, observed_type) 을 돌려준다.

    한쪽만 보면 안 되는 이유: LEFT JOIN 만 쓰면 "등록에 없는데 실제로 생긴
    필드"를 놓친다. 03번 SQL 이 FULL OUTER JOIN 을 쓰는 것과 같은 이유다.
    """
    findings: list[tuple[str, str, str | None, str | None]] = []
    for path in sorted(set(declared) | set(observed)):
        if path not in observed:
            findings.append((path, "MISSING_FIELD", declared[path], None))
        elif path not in declared:
            findings.append((path, "UNDECLARED_FIELD", None, observed[path]))
        elif declared[path] != observed[path]:
            # None 비교를 파이썬 != 로 하는 것은 SQL 의 IS DISTINCT FROM 과 같다.
            findings.append((path, "TYPE_CHANGED", declared[path], observed[path]))
    return findings

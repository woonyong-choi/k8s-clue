"""카탈로그 응답의 사유 코드. 닫힌 열거다.

처음에는 "SOURCE_FAILED:loki" 같은 문자열이었다. 그러면 소비자가 전부
split(":") 을 쓰게 되고, 그 문자열은 LLM 이 읽는 제어 필드이기도 하다.
코드와 대상을 분리했다.

열거를 닫아 두는 이유: 01번 문서의 원칙이 "소비자는 응답만 보고 결정한다"
인데, 열린 집합에는 switch 를 쓸 수 없다.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ReasonCode(StrEnum):
    """열거에 있는 값은 전부 어딘가에서 실제로 나온다.

    한때 PARTIAL_CHECK_COVERAGE · RESULT_TRUNCATED · NO_SOURCE_DATA 도 있었다.
    정의만 있고 내보내는 곳이 없었다. 닫힌 열거를 주는 이유가 소비자가 switch 를
    쓸 수 있게 하기 위해서인데, 아무도 내보내지 않는 값은 소비자에게 도달할 수
    없는 분기를 만들게 한다. 그래서 뺐다.

    NO_SOURCE_DATA 는 "과거 날짜인데 원본이 없어 재생 불가" 를 뜻하려던 값이다.
    지금 두 원천은 모두 저장된 것을 logical_date 로 걸러 읽으므로 그 상태가
    생기지 않는다. 원천을 직접 조회하는 수집기가 생기면 그때 다시 넣는다.
    """

    # 수집
    SOURCE_FAILED = "SOURCE_FAILED"
    SOURCE_TRUNCATED = "SOURCE_TRUNCATED"
    # 검사
    NEVER_RUN = "NEVER_RUN"
    # 응답
    REASON_CODES_TRUNCATED = "REASON_CODES_TRUNCATED"


COLLECTION_STATUS_REASONS: dict[str, ReasonCode] = {
    "FAILED": ReasonCode.SOURCE_FAILED,
    "TRUNCATED": ReasonCode.SOURCE_TRUNCATED,
}
"""수집 상태를 응답 사유로 옮기는 표.

이 표가 없을 때 조회 API 는 FAILED 와 TRUNCATED 를 한데 묶어 전부
SOURCE_FAILED 로 내보냈다. 잘림을 실패로 합치지 않겠다는 것이 이 프로젝트의
출발점인데, 정작 응답에서 다시 합치고 있었다.
"""


@dataclass(frozen=True)
class Reason:
    """사유 하나. 코드와 대상을 분리해서 담는다."""

    code: ReasonCode
    source: str | None = None
    asset_id: str | None = None

    def as_dict(self) -> dict[str, str]:
        payload = {"code": str(self.code)}
        if self.source:
            payload["source"] = self.source
        if self.asset_id:
            payload["asset_id"] = self.asset_id
        return payload


MAX_REASON_CODES = 16
"""사유 목록 상한.

사유가 계속 쌓이면 그것대로 응답을 밀어낸다. 다만 잘렸다는 사실은
REASON_CODES_TRUNCATED 로 남긴다. 잘림을 숨기지 않겠다면서 잘렸다고
알려주는 필드 자체를 조용히 자르면 앞뒤가 맞지 않는다.
"""


def bound_reasons(reasons: list[Reason]) -> tuple[list[dict[str, str]], bool]:
    if len(reasons) <= MAX_REASON_CODES:
        return [r.as_dict() for r in reasons], False
    kept = [r.as_dict() for r in reasons[: MAX_REASON_CODES - 1]]
    kept.append(Reason(ReasonCode.REASON_CODES_TRUNCATED).as_dict())
    return kept, True

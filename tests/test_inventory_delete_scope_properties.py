"""부분 관측 스냅샷이 삭제를 추론할 수 있는 범위의 속성 테스트.

수집이 완전하지 않은 스냅샷에서 "이번에 안 보였다"는 "지워졌다"가 아니다.
`inventory_deletion_scopes` 는 **실제로 끝까지 관측된 범위**에서만 삭제 권한을 내줘야 한다.
잘못되면 살아 있는 리소스가 대량으로 deleted 표시되는 쪽으로 틀리므로,
입력을 손으로 고른 예제 몇 개로 두지 않고 조합 전체에 대해 고정한다.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from domains.inventory.coverage import (
    CLUSTER_COLLECTIONS,
    DELETE_SAFE_COLLECTIONS,
    NAMESPACED_COLLECTIONS,
    RESOURCE_TYPES_BY_COLLECTION,
    inventory_deletion_scopes,
)

ALL_COLLECTIONS = sorted(RESOURCE_TYPES_BY_COLLECTION)

entries = st.fixed_dictionaries(
    {
        "collection": st.sampled_from(ALL_COLLECTIONS),
        "scope": st.sampled_from(("namespace", "cluster", "")),
        "namespace": st.sampled_from(("payments", "default", "", None)),
        "label_selector": st.sampled_from((None, "", "app=payment-api")),
        "observed": st.booleans(),
        "complete": st.booleans(),
        "delete_safe": st.booleans(),
        "truncated": st.booleans(),
        "reason_codes": st.lists(
            st.sampled_from(("collection_truncated", "collection_not_observed")), max_size=2
        ),
    }
)

summaries = st.fixed_dictionaries(
    {
        "live_inventory": st.booleans(),
        "collection_coverage": st.lists(entries, max_size=6),
    }
)


def authoritative(entry: dict) -> bool:
    return (
        entry["observed"] is True
        and entry["complete"] is True
        and entry["delete_safe"] is True
        and entry["truncated"] is not True
        and not (entry["label_selector"] or "").strip()
        and not entry["reason_codes"]
    )


@given(summary=summaries)
def test_a_delete_scope_only_comes_from_a_fully_observed_collection(summary: dict) -> None:
    scopes = inventory_deletion_scopes(summary)

    if summary["live_inventory"] is not True:
        assert scopes == (), "live 스냅샷이 아니면 어떤 삭제 범위도 나오면 안 된다"
        return

    for scope in scopes:
        supporting = [
            entry
            for entry in summary["collection_coverage"]
            if authoritative(entry)
            and entry["collection"] in DELETE_SAFE_COLLECTIONS
            and scope.resource_type in RESOURCE_TYPES_BY_COLLECTION[entry["collection"]]
            and (
                (
                    entry["collection"] in NAMESPACED_COLLECTIONS
                    and entry["scope"] == "namespace"
                    and scope.namespace == (str(entry["namespace"]).strip() or None)
                    and scope.namespace is not None
                )
                or (
                    entry["collection"] in CLUSTER_COLLECTIONS
                    and entry["scope"] == "cluster"
                    and scope.namespace is None
                )
            )
        ]
        assert supporting, f"{scope} 를 정당화하는 완전 관측 항목이 없다"


@given(summary=summaries)
def test_truncation_or_label_selector_always_revokes_the_delete_scope(summary: dict) -> None:
    """한 항목이라도 잘렸거나 label selector 로 좁혀졌으면 그 범위는 삭제 권한을 잃는다."""
    narrowed = {
        **summary,
        "live_inventory": True,
        "collection_coverage": [
            {**entry, "truncated": True, "label_selector": "app=payment-api"}
            for entry in summary["collection_coverage"]
        ],
    }

    assert inventory_deletion_scopes(narrowed) == ()


@given(summary=summaries)
def test_events_never_grant_a_delete_scope(summary: dict) -> None:
    """Event 는 보존 기간이 지나면 사라진다 — 부재가 삭제를 뜻하지 않는다."""
    events_only = {
        "live_inventory": True,
        "collection_coverage": [
            {
                **entry,
                "collection": "events",
                "scope": "namespace",
                "namespace": "payments",
                "observed": True,
                "complete": True,
                "delete_safe": True,
                "truncated": False,
                "label_selector": None,
                "reason_codes": [],
            }
            for entry in summary["collection_coverage"]
        ],
    }

    assert inventory_deletion_scopes(events_only) == ()

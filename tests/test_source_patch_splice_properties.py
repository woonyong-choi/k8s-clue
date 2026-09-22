"""승인 원문 byte 보존 치환의 속성 테스트.

`materialize_scalar_patch` 는 YAML 을 다시 덤프하지 않고 대상 scalar node 의
`start_mark ~ end_mark` 구간만 갈아끼운다. 그래서 검증해야 하는 것은
"원하는 값으로 바뀌었다"가 아니라 **그 밖의 어떤 byte 도 움직이지 않았다**이다.
예제 하나로는 들여쓰기·주석·인용 방식의 조합을 덮을 수 없어 속성으로 고정한다.
"""

from __future__ import annotations

import yaml
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from domains.gitops.source_patch import (
    ManifestScalarPatchPlan,
    ScalarFieldReplacement,
    canonical_manifest_digest,
    materialize_scalar_patch,
    materialize_scalar_rollback,
)

COMMENT = "# 운영 검토 메모 — 이 줄은 패치 후에도 그대로 남아야 한다"

indents = st.sampled_from(("  ", "    "))
comments = st.lists(st.just(COMMENT), min_size=0, max_size=3)
replica_pairs = st.integers(min_value=1, max_value=9).flatmap(
    lambda current: st.tuples(st.just(current), st.integers(min_value=current + 1, max_value=10))
)


def deployment_source(*, indent: str, leading: list[str], replicas: int, image: str) -> str:
    head = "".join(f"{line}\n" for line in leading)
    return (
        f"{head}"
        "apiVersion: apps/v1\n"
        "kind: Deployment\n"
        "metadata:\n"
        f"{indent}name: payment-api\n"
        f"{indent}namespace: payments\n"
        f"{indent}labels:\n"
        f"{indent}{indent}app: payment-api\n"
        "spec:\n"
        f"{indent}replicas: {replicas}   {COMMENT}\n"
        f"{indent}selector:\n"
        f"{indent}{indent}matchLabels:\n"
        f"{indent}{indent}{indent}app: payment-api\n"
        f"{indent}template:\n"
        f"{indent}{indent}metadata:\n"
        f"{indent}{indent}{indent}labels:\n"
        f"{indent}{indent}{indent}{indent}app: payment-api\n"
        f"{indent}{indent}spec:\n"
        f"{indent}{indent}{indent}containers:\n"
        f"{indent}{indent}{indent}{indent}- name: api\n"
        f"{indent}{indent}{indent}{indent}  image: {image}\n"
    )


def replica_plan(current: int, desired: int, source: str) -> ManifestScalarPatchPlan:
    return ManifestScalarPatchPlan(
        action_type="replica_scale",
        source_type="raw-yaml",
        source_manifest_sha256=canonical_manifest_digest(yaml.safe_load(source)),
        expected_base_sha="a" * 40,
        manifest_path="deploy/payment-api.yaml",
        replacements=(ScalarFieldReplacement("spec.replicas", current, desired),),
        rollback_replacements=(ScalarFieldReplacement("spec.replicas", desired, current),),
    )


# deadline 없음 — 여기서 고정하려는 것은 속도가 아니라 byte 보존이다.
@settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    indent=indents,
    leading=comments,
    pair=replica_pairs,
    image=st.sampled_from(
        ("registry/payment-api:v1", '"registry/payment-api:v1"', "'registry/payment-api:v1'")
    ),
)
def test_replica_patch_moves_exactly_one_line_and_keeps_every_other_byte(
    indent: str, leading: list[str], pair: tuple[int, int], image: str
) -> None:
    current, desired = pair
    source = deployment_source(indent=indent, leading=leading, replicas=current, image=image)

    patched = materialize_scalar_patch(source, replica_plan(current, desired, source))

    source_lines = source.splitlines()
    patched_lines = patched.splitlines()
    assert len(source_lines) == len(patched_lines)
    changed = [i for i, line in enumerate(source_lines) if line != patched_lines[i]]
    assert len(changed) == 1, "치환은 정확히 한 줄만 움직여야 한다"
    assert patched_lines[changed[0]] == f"{indent}replicas: {desired}   {COMMENT}"
    assert patched.count(COMMENT) == source.count(COMMENT)
    assert yaml.safe_load(patched)["spec"]["replicas"] == desired


# deadline 없음 — 여기서 고정하려는 것은 속도가 아니라 byte 보존이다.
@settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    indent=indents,
    leading=comments,
    pair=replica_pairs,
    image=st.sampled_from(
        ("registry/payment-api:v1", '"registry/payment-api:v1"', "'registry/payment-api:v1'")
    ),
)
def test_rollback_restores_the_approved_source_byte_for_byte(
    indent: str, leading: list[str], pair: tuple[int, int], image: str
) -> None:
    current, desired = pair
    source = deployment_source(indent=indent, leading=leading, replicas=current, image=image)
    plan = replica_plan(current, desired, source)

    patched = materialize_scalar_patch(source, plan)
    restored = materialize_scalar_rollback(
        patched,
        plan,
        expected_source_sha256=canonical_manifest_digest(yaml.safe_load(patched)),
    )

    assert restored == source


# deadline 없음 — 여기서 고정하려는 것은 속도가 아니라 byte 보존이다.
@settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    indent=indents,
    pair=replica_pairs,
    tag=st.sampled_from(("v1", "v2", "20260901-abc123")),
)
def test_image_patch_never_touches_the_replica_count(
    indent: str, pair: tuple[int, int], tag: str
) -> None:
    current, _ = pair
    source = deployment_source(
        indent=indent, leading=[], replicas=current, image=f"registry/payment-api:{tag}"
    )
    field_path = "spec.template.spec.containers[name=api].image"
    plan = ManifestScalarPatchPlan(
        action_type="image_tag_fix",
        source_type="raw-yaml",
        source_manifest_sha256=canonical_manifest_digest(yaml.safe_load(source)),
        expected_base_sha="a" * 40,
        manifest_path="deploy/payment-api.yaml",
        replacements=(
            ScalarFieldReplacement(
                field_path, f"registry/payment-api:{tag}", "registry/payment-api:fixed"
            ),
        ),
        rollback_replacements=(
            ScalarFieldReplacement(
                field_path, "registry/payment-api:fixed", f"registry/payment-api:{tag}"
            ),
        ),
    )

    patched = materialize_scalar_patch(source, plan)

    assert yaml.safe_load(patched)["spec"]["replicas"] == current
    assert f"{indent}replicas: {current}   {COMMENT}" in patched

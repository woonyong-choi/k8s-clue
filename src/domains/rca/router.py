"""rca 도메인 HTTP 라우터 — agent evidence 수신(라우터 단위 agent 가드)."""

from __future__ import annotations

import hashlib
import json
import math
import secrets
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from domains.gitops.events import GitWebhookReceivedBody
from domains.identity.dependencies import (
    ClusterAgentIdentity,
    hash_agent_token,
    require_cluster_access,
    require_cluster_agent,
    require_session,
)
from domains.rca.events import (
    ClusterEvidenceReceivedBody,
    RcaActionRequiredBody,
    RecoveryActionCandidate,
    RecoveryActionSelectedBody,
    RecoveryPlan,
    RecoveryRetryRequestedBody,
    compact_cluster_evidence_payload,
)
from domains.rca.history_ingest import evidence_inventory_payload
from domains.rca.recovery_verification import (
    DEFAULT_MAXIMUM_SECONDS,
    normalized_utc,
    standard_sli_series_identity,
    verification_deadline,
)
from packages.ai.rule_catalog import validate_catalog_yaml
from packages.config.settings import env
from packages.contracts.auth import Actor
from packages.contracts.gateway import routes as gateway_routes
from packages.contracts.gateway.requests import (
    AgentEvidenceRequest,
    AlertmanagerAlert,
    AlertmanagerWebhookRequest,
    RcaRuleValidateRequest,
    RecoveryActionSelectByCorrelationRequest,
    RecoveryActionSelectRequest,
    RecoveryRetryRequest,
)
from packages.contracts.gateway.responses import (
    AcceptedResponse,
    RcaRuleCandidateItem,
    RcaRuleCatalogItem,
    RcaRuleCatalogResponse,
    RcaRuleValidateResponse,
    RecoveryActionCandidateItem,
    RecoveryPlanStatusResponse,
    ValidationErrorItem,
)
from packages.contracts.gitops import (
    DEFAULT_APPLICATION_ID,
    DEFAULT_DEPLOYMENT_BINDING_ID,
    DEFAULT_ENVIRONMENT,
    DEFAULT_WORKFLOW_RUN_ID,
    ApprovalStatus,
)
from packages.contracts.identity import DEFAULT_WORKSPACE_ID, Permission, ResourceRole
from packages.events.envelope import event
from packages.runtime.dependencies import get_db, get_events
from packages.storage.engine import unit_of_work_or_null
from packages.storage.retry import to_thread_db_retry

# per-cluster 토큰 인증 — evidence 의 workspace/cluster 는 토큰 identity 에서만 취함.
router = APIRouter()
DEFAULT_EVIDENCE_SOURCE_ID = "cluster-snapshot"
RECOVERY_PLAN_NOT_FOUND = "recovery plan not found"
RECOVERY_ACTION_NOT_FOUND = "recovery action not found"
RECOVERY_PLAN_ALREADY_RESOLVED = "recovery plan already resolved"
RECOVERY_PLAN_CHANGED = "recovery plan changed"
RECOVERY_SELECTION_ACCESS_DENIED = "recovery selection access denied"
RECOVERY_RETRY_UNAVAILABLE = "recovery retry is unavailable"
RECOVERY_RETRY_IDENTITY_INVALID = "recovery retry identity is invalid"
HTTP_NOT_FOUND = 404
HTTP_CONFLICT = 409
HTTP_UNAUTHORIZED = 401
SAFE_PR_ROUTES = frozenset({"draft_pr", "safe_pr"})
PREFLIGHT_REQUIRED_ROUTES = SAFE_PR_ROUTES | {"auto"}
RECOVERY_STATUS_DEPLOY_PENDING = "deploy_pending"
RECOVERY_STATUS_VERIFICATION_PENDING = "verification_pending"
RECOVERY_STATUS_FAILED = "failed"
RECOVERY_DEPLOY_FAILED_REASON = "recovery_deploy_failed"
RECOVERY_VERIFICATION_EXPIRED_REASON = "verification_window_expired"
RECOVERY_SAFE_PR_FAILED_STAGE = "safe_pr"


class RcaRuleCandidateView(Protocol):
    candidate_id: str
    title: str
    expected_evidence: tuple[str, ...]
    signals: tuple[object, ...]


class RcaRuleProfileView(Protocol):
    rule_id: str | None
    symptoms: tuple[str, ...]
    required_sources: tuple[str, ...]
    candidate_specs: tuple[RcaRuleCandidateView, ...]


class RecoveryActionPreflightPort(Protocol):
    async def prepare(
        self,
        evt: RecoveryActionSelectedBody,
        correlation_id: str,
    ) -> RecoveryActionCandidate | RcaActionRequiredBody: ...


def get_rca_rule_profiles(request: Request) -> tuple[RcaRuleProfileView, ...]:
    """Gateway composition이 주입한 AI rule profile read port를 반환한다."""
    profiles = getattr(request.app.state, "rca_rule_profiles", None)
    if profiles is None:
        raise RuntimeError("RCA rule catalog provider is not configured")
    return tuple(profiles)


def get_recovery_action_preflight(request: Request) -> RecoveryActionPreflightPort | None:
    configured = getattr(request.app.state, "recovery_action_preflight", None)
    return configured


@router.post(gateway_routes.RCA_RULES_VALIDATE_PATH, response_model=RcaRuleValidateResponse)
async def validate_rca_rule_catalog(
    payload: RcaRuleValidateRequest,
    _current: Any = Depends(require_session),
) -> RcaRuleValidateResponse:
    result = validate_catalog_yaml(payload.yaml_text)
    if not result.valid:
        return RcaRuleValidateResponse(
            valid=False,
            errors=[
                ValidationErrorItem(code=issue.code, detail=issue.detail, line=issue.line)
                for issue in result.errors
            ],
        )
    first_rule = result.rules[0] if result.rules else None
    return RcaRuleValidateResponse(
        valid=True,
        matched_symptom=first_rule.symptoms[0] if first_rule else None,
        candidates_count=sum(len(rule.candidates) for rule in result.rules),
    )


@router.get(gateway_routes.RCA_RULES_PATH, response_model=RcaRuleCatalogResponse)
async def list_rca_rule_catalog(
    _current: Any = Depends(require_session),
    profiles: tuple[RcaRuleProfileView, ...] = Depends(get_rca_rule_profiles),
) -> RcaRuleCatalogResponse:
    items = [
        RcaRuleCatalogItem(
            rule_id=profile.rule_id or "",
            symptoms=list(profile.symptoms),
            required_sources=list(profile.required_sources),
            candidates=[
                RcaRuleCandidateItem(
                    candidate_id=candidate.candidate_id,
                    title=candidate.title,
                    expected_evidence=list(candidate.expected_evidence),
                    signals_count=len(candidate.signals),
                )
                for candidate in profile.candidate_specs
            ],
        )
        for profile in profiles
    ]
    return RcaRuleCatalogResponse(
        items=items,
        rules_count=len(items),
        candidates_count=sum(len(item.candidates) for item in items),
    )


def scoped_evidence_key(identity: ClusterAgentIdentity, evidence_key: str | None) -> str | None:
    """agent 가 만든 evidence_key 를 신뢰된 identity 로 네임스페이스.

    evidence_windows 의 PK 가 evidence_key 단일이라, 접두사 없이는 다른 워크스페이스 agent 가
    키를 선점/충돌시켜 증거 억제·event_id/correlation_id 테넌트 누수 가능. 토큰 identity 로
    키 공간을 워크스페이스/클러스터로 분리(body 의 문자열 신뢰 X).
    """
    if not evidence_key:
        return None
    return f"{identity.workspace_id}:{identity.cluster_id}:{evidence_key}"


def agent_evidence_key(identity: ClusterAgentIdentity, payload: AgentEvidenceRequest) -> str:
    scoped_key = scoped_evidence_key(identity, payload.evidence_key)
    if scoped_key:
        return scoped_key
    # 구형 agent 가 evidence_key 를 보내지 않아도 full payload 이벤트 발행은 금지한다.
    # 신뢰된 identity + payload digest 로 안정 키를 만들고 원문은 evidence_windows 에만 둔다.
    data = payload.model_dump(exclude={"correlation_id"})
    data["workspace_id"] = identity.workspace_id
    data["cluster_id"] = identity.cluster_id
    data["evidence_key"] = None
    encoded = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode()).hexdigest()[:32]
    source_id = payload.source_id or DEFAULT_EVIDENCE_SOURCE_ID
    window_start = payload.window_start or payload.correlation_id or "adhoc"
    return f"{identity.workspace_id}:{identity.cluster_id}:{source_id}:{window_start}:{digest}"


def build_cluster_evidence_body(
    payload: AgentEvidenceRequest, identity: ClusterAgentIdentity
) -> ClusterEvidenceReceivedBody:
    # body 의 workspace_id/cluster_id 는 무시하고 토큰 identity 로 덮어씀(테넌트 위조 차단).
    data = payload.model_dump(exclude={"correlation_id"})
    data["workspace_id"] = identity.workspace_id
    data["cluster_id"] = identity.cluster_id
    data["evidence_key"] = agent_evidence_key(identity, payload)
    data["correlation_id"] = payload.correlation_id
    return ClusterEvidenceReceivedBody(**data)


@router.post(gateway_routes.AGENT_EVIDENCE_PATH, response_model=AcceptedResponse)
async def agent_evidence(
    payload: AgentEvidenceRequest,
    identity: ClusterAgentIdentity = Depends(require_cluster_agent),
    events: Any = Depends(get_events),
    db: Any = Depends(get_db),
) -> AcceptedResponse:
    evidence_key = agent_evidence_key(identity, payload)
    evidence_body = build_cluster_evidence_body(payload, identity)
    event_envelope = event(
        evidence_body.__subject__,
        getattr(events, "source", "api-gateway"),
        compact_cluster_evidence_payload(evidence_body, payload.correlation_id),
        payload.correlation_id,
    )
    existing = await db_call(db.get_evidence_window, evidence_key)
    if existing:
        return AcceptedResponse(
            accepted=True,
            event_id=existing["event_id"],
            correlation_id=existing["correlation_id"],
        )
    recorded = await db_call(
        db.record_evidence_event_once,
        evidence_key=evidence_key,
        workspace_id=identity.workspace_id,
        cluster_id=identity.cluster_id,
        source_id=evidence_body.source_id or DEFAULT_EVIDENCE_SOURCE_ID,
        window_start=evidence_body.window_start or evidence_body.evidence_key or evidence_key,
        agent_id=evidence_body.agent_id,
        event_envelope=event_envelope,
        payload=evidence_body.to_body(),
        inventory_payload=evidence_inventory_payload(payload.kubernetes),
    )
    return AcceptedResponse(
        accepted=True,
        event_id=recorded["event_id"],
        correlation_id=recorded["correlation_id"],
    )


# 외부 모니터링 웹훅 — Alertmanager 가 firing 알림을 보내면 인시던트 파이프라인을 연다.
ALERTMANAGER_WEBHOOK_TOKEN_ENV = "ALERTMANAGER_WEBHOOK_TOKEN"
ALERTMANAGER_SOURCE_ID = "alertmanager-webhook"
ALERTMANAGER_REOPEN_DISPOSITIONS = frozenset({"orphan", "terminal"})
WEBHOOK_TOKEN_INVALID = "invalid webhook token"
CLUSTER_NOT_REGISTERED = "cluster is not registered"
STANDARD_SLI_ALERT_NAME = "KyroSliFailureRatioHigh"
STANDARD_SLI_REQUIRED_LABELS = (
    "kyro_namespace",
    "kyro_resource_kind",
    "kyro_resource_name",
    "kyro_service",
    "kyro_sli",
    "kyro_symptom",
)
STANDARD_SLI_LABELS_INVALID = "standard SLI alert is missing required resource labels"
STANDARD_SLI_MEASUREMENT_INVALID = (
    "standard SLI alert is missing valid machine-readable measurements"
)
HTTP_UNAUTHORIZED = 401


async def require_alertmanager_token(
    request: Request,
    db: Any,
    *,
    workspace_id: str,
    cluster_id: str,
) -> None:
    """Alertmanager Bearer 인증 — 전역 secret 또는 정확히 일치하는 cluster agent.

    전역 webhook secret은 기존 설치와의 호환 경로다. 클러스터 설치기가 쓰는
    per-cluster agent token은 저장된 해시로만 인증하고, 인증 결과의
    workspace/cluster가 query scope와 정확히 같을 때만 허용한다.
    """
    configured = env(ALERTMANAGER_WEBHOOK_TOKEN_ENV, "").strip()
    scheme, separator, raw_token = request.headers.get("authorization", "").partition(" ")
    supplied = raw_token.strip() if separator and scheme.casefold() == "bearer" else ""
    if not supplied:
        raise HTTPException(status_code=HTTP_UNAUTHORIZED, detail=WEBHOOK_TOKEN_INVALID)
    if configured and secrets.compare_digest(supplied, configured):
        return

    authenticate = getattr(db, "authenticate_cluster_agent", None)
    if not callable(authenticate):
        raise HTTPException(status_code=HTTP_UNAUTHORIZED, detail=WEBHOOK_TOKEN_INVALID)
    identity = await db_call(authenticate, hash_agent_token(supplied))
    if (
        not isinstance(identity, dict)
        or str(identity.get("workspace_id") or "") != workspace_id
        or str(identity.get("cluster_id") or "") != cluster_id
    ):
        # 토큰이 다른 tenant/cluster에 속하는지도 외부에 노출하지 않는다.
        raise HTTPException(status_code=HTTP_UNAUTHORIZED, detail=WEBHOOK_TOKEN_INVALID)


def validate_alertmanager_sli_labels(payload: AlertmanagerWebhookRequest) -> None:
    """표준 SLI 알림은 RCA 대상 신원을 빈 문자열 없이 제공해야 한다."""
    for alert in payload.alerts:
        labels = alert.labels if isinstance(alert.labels, dict) else {}
        if str(labels.get("alertname") or "").strip() != STANDARD_SLI_ALERT_NAME:
            continue
        if any(not str(labels.get(key) or "").strip() for key in STANDARD_SLI_REQUIRED_LABELS):
            raise HTTPException(status_code=422, detail=STANDARD_SLI_LABELS_INVALID)
        measurements = standard_sli_measurements(alert)
        if (
            measurements is None
            or (
                alert.status.strip().lower() == "firing"
                and measurements[0] <= measurements[1]
            )
        ):
            raise HTTPException(status_code=422, detail=STANDARD_SLI_MEASUREMENT_INVALID)


def standard_sli_measurements(
    alert: AlertmanagerAlert,
) -> tuple[float, float] | None:
    """Read bounded numeric evidence only from dedicated annotations."""

    labels = alert.labels if isinstance(alert.labels, dict) else {}
    if str(labels.get("alertname") or "").strip() != STANDARD_SLI_ALERT_NAME:
        return None
    annotations = alert.annotations if isinstance(alert.annotations, dict) else {}
    values: list[float] = []
    for key in ("kyro_observed_value", "kyro_threshold"):
        raw = annotations.get(key)
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(value) or value < 0 or value > 1:
            return None
        values.append(value)
    return values[0], values[1]


def alertmanager_evidence_key(
    workspace_id: str, cluster_id: str, payload: AlertmanagerWebhookRequest
) -> str:
    """같은 알림 그룹의 반복 통지(repeat_interval)는 같은 키 → 인시던트 1건으로 dedup.

    새 알림이 그룹에 추가되거나 알림 시작 시각이 바뀌면 키가 바뀌어 새 인시던트가 열린다.
    """
    firing = sorted(
        f"{alert.fingerprint}@{alert.startsAt}"
        for alert in payload.alerts
        if alert.status == "firing"
    )
    raw = "|".join([payload.groupKey, *firing])
    digest = hashlib.sha256(raw.encode()).hexdigest()[:32]
    return f"{workspace_id}:{cluster_id}:alertmanager:{digest}"


def alertmanager_kubernetes_hint(payload: AlertmanagerWebhookRequest) -> dict[str, Any]:
    """firing 알림의 라벨/주석을 파이프라인의 명시 계약(symptom/resource)으로 승격한다.

    incident 분류는 "명시 > 유도 > unknown" 계약(pipeline/symptom.py)을 따르는데,
    Alertmanager evidence 는 kubernetes snapshot 이 없어 종전에는 항상 unknown 으로
    빠져 원인 룰에 도달하지 못했다. 알림 룰이 선언한 kyro_* 라벨(관측 대상)과
    alertname 은 Prometheus 가 실제로 평가한 관측 결과이므로, 합성이 아니라
    수신한 계약 데이터의 승격이다. 힌트가 없으면 alertname 만 symptom 으로 쓴다.
    """
    firing = [alert for alert in payload.alerts if alert.status == "firing"]
    if not firing:
        return {}
    alert = firing[0]
    labels = alert.labels if isinstance(alert.labels, dict) else {}
    annotations = alert.annotations if isinstance(alert.annotations, dict) else {}

    def text(value: object) -> str:
        return str(value or "").strip()

    symptom = text(annotations.get("kyro_symptom")) or text(labels.get("kyro_symptom")) or text(
        labels.get("alertname")
    )
    hint: dict[str, Any] = {}
    if symptom:
        hint["symptom"] = symptom
    resource_name = text(labels.get("kyro_resource_name"))
    if resource_name:
        hint["resource"] = {
            "kind": text(labels.get("kyro_resource_kind")) or "Deployment",
            "name": resource_name,
            "namespace": text(labels.get("kyro_namespace")) or text(labels.get("namespace"))
            or None,
        }
    severity = text(labels.get("severity"))
    if severity:
        hint["severity"] = severity
    if hint:
        hint.setdefault("category", "application_runtime")
    return hint


def build_alertmanager_evidence_body(
    workspace_id: str,
    cluster_id: str,
    payload: AlertmanagerWebhookRequest,
    evidence_key: str,
) -> ClusterEvidenceReceivedBody:
    firing = [alert.model_dump() for alert in payload.alerts if alert.status == "firing"]
    window_start = min(
        (alert.startsAt for alert in payload.alerts if alert.status == "firing" and alert.startsAt),
        default=None,
    )
    return ClusterEvidenceReceivedBody(
        cluster_id=cluster_id,
        workspace_id=workspace_id,
        kubernetes=alertmanager_kubernetes_hint(payload),
        metrics={
            "alertmanager": {
                "group_key": payload.groupKey,
                "receiver": payload.receiver,
                "alerts": firing,
            }
        },
        logs=[],
        traces={},
        source_id=ALERTMANAGER_SOURCE_ID,
        window_start=window_start,
        evidence_key=evidence_key,
    )


def alertmanager_alert_event_id(
    workspace_id: str,
    cluster_id: str,
    alert: AlertmanagerAlert,
) -> str:
    labels = alert.labels
    identity = "|".join(
        (
            workspace_id,
            cluster_id,
            alert.fingerprint,
            alert.startsAt,
            str(labels.get("alertname") or ""),
        )
    )
    return f"ale-am-{hashlib.sha256(identity.encode()).hexdigest()[:32]}"


def build_alertmanager_alert_event(
    workspace_id: str,
    cluster_id: str,
    alert: AlertmanagerAlert,
    *,
    incident_id: str | None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    labels = {str(key): str(value) for key, value in alert.labels.items()}
    annotations = {str(key): str(value) for key, value in alert.annotations.items()}
    alert_name = (labels.get("alertname") or "External alert")[:120]
    namespace = (
        labels.get("kyro_namespace") or labels.get("namespace") or ""
    ).strip()[:253] or None
    if labels.get("kyro_resource_name"):
        kind = labels.get("kyro_resource_kind") or "Workload"
        name = labels["kyro_resource_name"]
    elif labels.get("pod"):
        kind, name = "Pod", labels["pod"]
    elif labels.get("deployment"):
        kind, name = "Deployment", labels["deployment"]
    elif labels.get("statefulset"):
        kind, name = "StatefulSet", labels["statefulset"]
    elif labels.get("service"):
        kind, name = "Service", labels["service"]
    elif labels.get("room"):
        kind, name = "GameRoom", labels["room"]
    else:
        kind = labels.get("kind") or "Workload"
        name = labels.get("instance") or alert_name
    subject = {
        "cluster": cluster_id[:512],
        "namespace": namespace,
        "kind": kind[:253],
        "name": name[:253],
    }
    severity = (labels.get("severity") or "warning").strip().lower()
    if severity not in {"critical", "high", "medium", "low", "warning", "info"}:
        severity = "warning"
    status = "resolved" if alert.status.strip().lower() == "resolved" else "firing"
    fired_at = _alertmanager_timestamp(alert.startsAt) or observed_at or datetime.now(UTC)
    resolved_at = (
        (_alertmanager_timestamp(alert.endsAt) or observed_at or datetime.now(UTC))
        if status == "resolved"
        else None
    )
    summary = (
        annotations.get("summary")
        or annotations.get("description")
        or f"{alert_name} reported by Alertmanager"
    )[:1000]
    subject_key = hashlib.sha256(
        json.dumps(subject, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    measurements = standard_sli_measurements(alert)
    observed_value = measurements[0] if measurements is not None else None
    threshold = measurements[1] if measurements is not None else None
    series_identity = (
        standard_sli_series_identity(
            {
                "namespace": labels.get("kyro_namespace"),
                "resource_kind": labels.get("kyro_resource_kind"),
                "resource_name": labels.get("kyro_resource_name"),
                "service": labels.get("kyro_service"),
                "sli": labels.get("kyro_sli"),
                "symptom": labels.get("kyro_symptom"),
            }
        )
        if alert_name == STANDARD_SLI_ALERT_NAME
        else None
    )
    return {
        "event_id": alertmanager_alert_event_id(workspace_id, cluster_id, alert),
        "workspace_id": workspace_id,
        "rule_id": None,
        "rule_name": alert_name,
        "source": "alertmanager",
        "severity": severity,
        "subject_key": subject_key,
        "subject": subject,
        "fired_at": fired_at,
        "resolved_at": resolved_at,
        "status": status,
        "observed_value": observed_value,
        "threshold": threshold,
        "series_identity": series_identity,
        "evidence": [
            {
                "type": "alertmanager",
                "metric": alert_name,
                "observed_at": fired_at.isoformat(),
                "subject": subject,
                "value": observed_value,
                "summary": summary,
                "link": None,
            }
        ],
        "incident_id": incident_id,
    }


def _alertmanager_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


async def persist_alertmanager_alert_events(
    db: Any,
    workspace_id: str,
    cluster_id: str,
    payload: AlertmanagerWebhookRequest,
    *,
    incident_id: str | None,
) -> None:
    upsert = getattr(db, "upsert_external_alert_event", None)
    if not callable(upsert):
        raise RuntimeError("external alert event repository is unavailable")
    observed_at = datetime.now(UTC)
    for alert in payload.alerts:
        await db_call(
            upsert,
            build_alertmanager_alert_event(
                workspace_id,
                cluster_id,
                alert,
                incident_id=incident_id,
                observed_at=observed_at,
            ),
        )


@router.post(gateway_routes.ALERTMANAGER_WEBHOOK_PATH, response_model=AcceptedResponse)
@router.post("/rca/alertmanager", response_model=AcceptedResponse, include_in_schema=False)
@router.post("/api/rca/alertmanager", response_model=AcceptedResponse, include_in_schema=False)
async def alertmanager_webhook(
    payload: AlertmanagerWebhookRequest,
    request: Request,
    cluster_id: str,
    workspace_id: str = DEFAULT_WORKSPACE_ID,
    events: Any = Depends(get_events),
    db: Any = Depends(get_db),
) -> AcceptedResponse:
    await require_alertmanager_token(
        request,
        db,
        workspace_id=workspace_id,
        cluster_id=cluster_id,
    )
    registration = await db_call(db.get_cluster_registration, workspace_id, cluster_id)
    if registration is None:
        raise HTTPException(status_code=HTTP_NOT_FOUND, detail=CLUSTER_NOT_REGISTERED)
    validate_alertmanager_sli_labels(payload)

    if not any(alert.status.strip().lower() == "firing" for alert in payload.alerts):
        await persist_alertmanager_alert_events(
            db,
            workspace_id,
            cluster_id,
            payload,
            incident_id=None,
        )
        # resolved 만 담긴 통지는 수락만 하고 인시던트를 열지 않는다.
        return AcceptedResponse(accepted=True, event_id="", correlation_id="")

    evidence_key = alertmanager_evidence_key(workspace_id, cluster_id, payload)
    evidence_body = build_alertmanager_evidence_body(
        workspace_id, cluster_id, payload, evidence_key
    )
    # Each Alertmanager start is an immutable processing attempt.  The dashboard
    # groups those correlations into one operator PIN through
    # ``incident_occurrence_id``.  Reusing the PIN's first correlation here would
    # overwrite its timeline row and erase both the recurrence count and the
    # latest recovery authority snapshot.
    event_envelope = event(
        evidence_body.__subject__,
        getattr(events, "source", "api-gateway"),
        compact_cluster_evidence_payload(evidence_body),
        None,
    )
    existing = await db_call(db.get_evidence_window, evidence_key)
    if existing:
        disposition = await db_call(
            db.get_alertmanager_evidence_disposition,
            workspace_id,
            str(existing["correlation_id"]),
            str(existing["event_id"]),
        )
        if disposition not in ALERTMANAGER_REOPEN_DISPOSITIONS:
            await persist_alertmanager_alert_events(
                db,
                workspace_id,
                cluster_id,
                payload,
                incident_id=str(existing["correlation_id"]),
            )
            return AcceptedResponse(
                accepted=True,
                event_id=existing["event_id"],
                correlation_id=existing["correlation_id"],
            )
        recorded = await db_call(
            db.rotate_alertmanager_evidence_window,
            evidence_key=evidence_key,
            expected_event_id=str(existing["event_id"]),
            expected_correlation_id=str(existing["correlation_id"]),
            workspace_id=workspace_id,
            cluster_id=cluster_id,
            source_id=ALERTMANAGER_SOURCE_ID,
            window_start=evidence_body.window_start or evidence_key,
            agent_id=None,
            event_envelope=event_envelope,
            payload=evidence_body.to_body(),
        )
    else:
        recorded = await db_call(
            db.record_evidence_event_once,
            evidence_key=evidence_key,
            workspace_id=workspace_id,
            cluster_id=cluster_id,
            source_id=ALERTMANAGER_SOURCE_ID,
            window_start=evidence_body.window_start or evidence_key,
            agent_id=None,
            event_envelope=event_envelope,
            payload=evidence_body.to_body(),
        )
    await persist_alertmanager_alert_events(
        db,
        workspace_id,
        cluster_id,
        payload,
        incident_id=str(recorded["correlation_id"]),
    )
    return AcceptedResponse(
        accepted=True,
        event_id=recorded["event_id"],
        correlation_id=recorded["correlation_id"],
    )


def recovery_approval_id(plan_id: str, action_id: str) -> str:
    raw = f"{plan_id}|{action_id}|recovery-action"
    return f"approval-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def recovery_policy_decision_ref(approval_ref: str) -> str:
    return f"recovery:{approval_ref}:selected"


def candidate_by_action_id(
    plan: RecoveryPlan,
    action_id: str,
) -> RecoveryActionCandidate:
    for candidate in plan.candidates:
        if candidate.action_id == action_id:
            return candidate
    raise HTTPException(status_code=HTTP_NOT_FOUND, detail=RECOVERY_ACTION_NOT_FOUND)


def candidate_with_approval(
    candidate: RecoveryActionCandidate,
    *,
    approval_ref: str,
    policy_decision_ref: str,
) -> RecoveryActionCandidate:
    draft = candidate.draft
    return replace(
        candidate,
        draft=replace(
            draft,
            params={
                **draft.params,
                "approval_ref": approval_ref,
                "policy_decision_ref": policy_decision_ref,
            },
        ),
    )


def candidate_with_approval_identity(
    candidate: RecoveryActionCandidate,
    approval_record: object,
) -> RecoveryActionCandidate:
    """Persisted approval identity must be identical to the dispatched command identity."""
    if not isinstance(approval_record, dict):
        return candidate
    workflow_run_id = str(approval_record.get("workflow_run_id") or "")
    if not workflow_run_id:
        return candidate
    draft = candidate.draft
    return replace(
        candidate,
        draft=replace(
            draft,
            params={
                **draft.params,
                "workflow_run_id": workflow_run_id,
            },
        ),
    )


def recovery_approval_payload(
    plan: RecoveryPlan,
    selected: RecoveryActionCandidate,
    *,
    workspace_id: str,
    approval_ref: str,
    policy_decision_ref: str,
    selected_by: str,
    reason: str,
) -> dict[str, Any]:
    params = selected.draft.params
    return {
        "approval_id": approval_ref,
        "workflow_run_id": str(params.get("workflow_run_id", DEFAULT_WORKFLOW_RUN_ID)),
        "workspace_id": str(
            plan.target.get("workspace_id") or params.get("workspace_id") or workspace_id
        ),
        "application_id": str(params.get("application_id", DEFAULT_APPLICATION_ID)),
        "binding_id": str(params.get("binding_id", DEFAULT_DEPLOYMENT_BINDING_ID)),
        "environment": str(params.get("environment", DEFAULT_ENVIRONMENT)),
        "status": ApprovalStatus.GRANTED.value,
        "reason": reason,
        "requested_role": ResourceRole.RELEASE_OPERATOR.value,
        "requested_by": selected_by,
        "decided_by": selected_by,
        "decision": "selected",
        "details": {
            "approval_ref": approval_ref,
            "policy_decision_ref": policy_decision_ref,
            "recovery_plan_id": plan.plan_id,
            "recovery_action_id": selected.action_id,
            "selected_candidate": selected.to_body(),
        },
    }


async def _select_recovery_action_from_record(
    record: dict[str, Any],
    action_id: str | None,
    reason: str | None,
    *,
    expected_plan_id: str | None,
    current: Any,
    db: Any,
    events: Any,
    preflight: RecoveryActionPreflightPort | None,
) -> AcceptedResponse:
    workspace_id = current.workspace_id
    plan = RecoveryPlan.from_body(record["payload"])
    cluster_id = str(plan.target.get("cluster_id", ""))
    require_cluster_access(
        db,
        current,
        workspace_id,
        cluster_id,
        Permission.DEPLOY_RUN.value,
        detail=RECOVERY_SELECTION_ACCESS_DENIED,
    )
    if expected_plan_id is not None and plan.plan_id != expected_plan_id:
        raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_PLAN_CHANGED)
    selected = candidate_by_action_id(plan, action_id or plan.recommended_action_id)
    reason = reason or f"operator selected recovery action: {selected.title}"
    correlation_id = str(record["correlation_id"])
    if selected.route in PREFLIGHT_REQUIRED_ROUTES:
        proposed = RecoveryActionSelectedBody(
            plan=plan,
            selected=selected,
            selected_by=current.user_id,
            auto_selected=False,
            reason=reason,
            workspace_id=workspace_id,
        )
        prepared: RecoveryActionCandidate | RcaActionRequiredBody
        if preflight is None:
            is_safe_pr = selected.route in SAFE_PR_ROUTES
            prepared = RcaActionRequiredBody(
                reason=(
                    "Safe PR 사전 검증 서비스가 준비되지 않았습니다."
                    if is_safe_pr
                    else "복구 명령 사전 정책 검증 서비스가 준비되지 않았습니다."
                ),
                evidence_ref=plan.evidence_ref,
                workspace_id=workspace_id,
                reason_code=(
                    "safe_pr_preflight_unavailable"
                    if is_safe_pr
                    else "recovery_action_preflight_unavailable"
                ),
                missing_evidence=(
                    ["gitops_authority_context"]
                    if is_safe_pr
                    else ["command_control_policy"]
                ),
                diagnostics={
                    "plan_id": plan.plan_id,
                    "action_id": selected.action_id,
                    "route": selected.route,
                },
            )
        else:
            prepared = await preflight.prepare(proposed, correlation_id)
        if isinstance(prepared, RcaActionRequiredBody):
            await events.accept_body(
                prepared,
                correlation_id=correlation_id,
                actor=Actor(current.user_id, tuple(current.roles)),
            )
            raise HTTPException(
                status_code=HTTP_CONFLICT,
                detail={
                    "code": prepared.reason_code,
                    "detail": prepared.reason,
                    "missing_evidence": prepared.missing_evidence,
                    "next_actions": prepared.next_actions,
                    "retryable": True,
                },
            )
        selected = prepared
    approval_ref = recovery_approval_id(plan.plan_id, selected.action_id)
    policy_decision_ref = recovery_policy_decision_ref(approval_ref)
    selected = candidate_with_approval(
        selected,
        approval_ref=approval_ref,
        policy_decision_ref=policy_decision_ref,
    )
    with unit_of_work_or_null(db):
        selected_record = db.select_recovery_plan_action_if_open(
            plan.plan_id,
            workspace_id,
            selected.action_id,
            current.user_id,
        )
        if selected_record is None:
            raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_PLAN_ALREADY_RESOLVED)
        approval_record = db.request_workflow_approval(
            recovery_approval_payload(
                plan,
                selected,
                workspace_id=workspace_id,
                approval_ref=approval_ref,
                policy_decision_ref=policy_decision_ref,
                selected_by=current.user_id,
                reason=reason,
            )
        )
        selected = candidate_with_approval_identity(selected, approval_record)
        accepted = await events.accept_body(
            RecoveryActionSelectedBody(
                plan=plan,
                selected=selected,
                selected_by=current.user_id,
                auto_selected=False,
                reason=reason,
                workspace_id=workspace_id,
            ),
            correlation_id=correlation_id,
            actor=Actor(current.user_id, tuple(current.roles)),
        )
    return AcceptedResponse(
        accepted=True,
        event_id=accepted.event.event_id,
        correlation_id=accepted.event.correlation_id,
    )


@router.post(gateway_routes.RCA_RECOVERY_ACTION_SELECT_PATH, response_model=AcceptedResponse)
async def select_recovery_action(
    plan_id: str,
    action_id: str,
    payload: RecoveryActionSelectRequest,
    current: Any = Depends(require_session),
    db: Any = Depends(get_db),
    events: Any = Depends(get_events),
    preflight: RecoveryActionPreflightPort | None = Depends(get_recovery_action_preflight),
) -> AcceptedResponse:
    record = await db_call(db.get_recovery_plan, plan_id, current.workspace_id)
    if record is None:
        raise HTTPException(status_code=HTTP_NOT_FOUND, detail=RECOVERY_PLAN_NOT_FOUND)
    return await _select_recovery_action_from_record(
        record,
        action_id,
        payload.reason,
        expected_plan_id=None,
        current=current,
        db=db,
        events=events,
        preflight=preflight,
    )


@router.post(
    gateway_routes.RCA_RECOVERY_ACTION_SELECT_BY_CORRELATION_PATH,
    response_model=AcceptedResponse,
)
async def select_recovery_action_by_correlation(
    correlation_id: str,
    payload: RecoveryActionSelectByCorrelationRequest,
    current: Any = Depends(require_session),
    db: Any = Depends(get_db),
    events: Any = Depends(get_events),
    preflight: RecoveryActionPreflightPort | None = Depends(get_recovery_action_preflight),
) -> AcceptedResponse:
    record = await db_call(
        db.get_recovery_plan_by_correlation,
        correlation_id,
        current.workspace_id,
    )
    if record is None:
        raise HTTPException(status_code=HTTP_NOT_FOUND, detail=RECOVERY_PLAN_NOT_FOUND)
    return await _select_recovery_action_from_record(
        record,
        payload.action_id,
        payload.reason,
        expected_plan_id=payload.expected_plan_id,
        current=current,
        db=db,
        events=events,
        preflight=preflight,
    )


def recovery_object(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def recovery_text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def recovery_retry_number(container: dict[str, Any]) -> int:
    value = container.get("retry_attempt")
    return value + 1 if type(value) is int and value >= 0 else 1


def recovery_selection_attempt_number(lifecycle: dict[str, Any]) -> int:
    attempt = recovery_object(lifecycle.get("attempt"))
    value = attempt.get("number")
    return value + 1 if type(value) is int and value >= 0 else 1


def recovery_retry_approval_id(plan_id: str, action_id: str, attempt: int) -> str:
    raw = f"{plan_id}|{action_id}|safe-pr-retry|{attempt}"
    return f"approval-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def recovery_workflow_matches(
    workflow: object,
    *,
    workspace_id: str,
    workflow_run_id: str,
    binding_id: str,
    application_id: str,
    cluster_id: str,
    commit_sha: str,
    status: str,
) -> bool:
    row = recovery_object(workflow)
    return bool(
        workflow_run_id
        and binding_id
        and application_id
        and cluster_id
        and commit_sha
        and recovery_text(row.get("workflow_run_id")) == workflow_run_id
        and recovery_text(row.get("workspace_id")) == workspace_id
        and recovery_text(row.get("binding_id")) == binding_id
        and recovery_text(row.get("application_id")) == application_id
        and recovery_text(row.get("cluster_id")) == cluster_id
        and recovery_text(row.get("commit_sha")) == commit_sha
        and recovery_text(row.get("status")) == status
    )


def deploy_retry_body(
    *,
    record: dict[str, Any],
    lifecycle: dict[str, Any],
    workflow: object,
) -> tuple[GitWebhookReceivedBody, int] | None:
    merge = recovery_object(lifecycle.get("merge"))
    pr = recovery_object(lifecycle.get("pr"))
    request = recovery_object(merge.get("deployment_request"))
    old_workflow_run_id = recovery_text(merge.get("workflow_run_id"))
    workspace_id = recovery_text(record.get("workspace_id"))
    correlation_id = recovery_text(record.get("correlation_id"))
    binding_id = recovery_text(merge.get("binding_id"))
    application_id = recovery_text(merge.get("application_id"))
    cluster_id = recovery_text(merge.get("cluster_id"))
    commit_sha = recovery_text(merge.get("merge_commit_sha"))
    if not recovery_workflow_matches(
        workflow,
        workspace_id=workspace_id,
        workflow_run_id=old_workflow_run_id,
        binding_id=binding_id,
        application_id=application_id,
        cluster_id=cluster_id,
        commit_sha=commit_sha,
        status="failed",
    ):
        return None
    try:
        original = cast(
            GitWebhookReceivedBody,
            GitWebhookReceivedBody.from_body(request),
        )
    except (TypeError, ValueError):
        return None
    if (
        original.workspace_id != workspace_id
        or original.correlation_id != correlation_id
        or original.workflow_run_id != old_workflow_run_id
        or original.commit_sha != commit_sha
        or original.repository_id != recovery_text(pr.get("repository_id"))
        or original.repo_ref != recovery_text(pr.get("repo_ref"))
        or original.branch != recovery_text(pr.get("base_branch"))
        or original.binding_id != binding_id
        or original.application_id != application_id
        or original.cluster_id != cluster_id
        or original.manifest_path != recovery_text(pr.get("manifest_path"))
        or not original.image.strip()
        or type(original.replicas) is not int
        or original.replicas <= 0
    ):
        return None
    attempt = recovery_retry_number(merge)
    raw = f"{record['plan_id']}|{commit_sha}|deploy-retry|{attempt}"
    workflow_run_id = f"workflow-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"
    return replace(
        original,
        workflow_run_id=workflow_run_id,
        force=True,
    ), attempt


def verification_retry_lifecycle(
    *,
    record: dict[str, Any],
    lifecycle: dict[str, Any],
    workflow: object,
    now: datetime,
    requested_by: str,
    reason: str,
) -> tuple[dict[str, Any], int] | None:
    merge = recovery_object(lifecycle.get("merge"))
    verification = recovery_object(lifecycle.get("verification"))
    workflow_run_id = recovery_text(merge.get("workflow_run_id"))
    if not recovery_workflow_matches(
        workflow,
        workspace_id=recovery_text(record.get("workspace_id")),
        workflow_run_id=workflow_run_id,
        binding_id=recovery_text(merge.get("binding_id")),
        application_id=recovery_text(merge.get("application_id")),
        cluster_id=recovery_text(merge.get("cluster_id")),
        commit_sha=recovery_text(merge.get("merge_commit_sha")),
        status="succeeded",
    ):
        return None
    maximum = verification.get("maximum_seconds")
    maximum_seconds = (
        maximum
        if type(maximum) is int and 0 < maximum <= DEFAULT_MAXIMUM_SECONDS
        else DEFAULT_MAXIMUM_SECONDS
    )
    attempt = recovery_retry_number(verification)
    verification.update(
        {
            "status": RECOVERY_STATUS_VERIFICATION_PENDING,
            "started_at": now.isoformat(),
            "deadline_at": verification_deadline(now, maximum_seconds).isoformat(),
            "healthy_since": None,
            "last_healthy_observed_at": None,
            "distinct_evidence_count": 0,
            "last_evidence_key": None,
            "last_session_samples": verification.get("protected_session_baseline"),
            "after": {},
            "last_reason_code": "waiting_for_post_deploy_evidence",
            "last_reason": "동일 배포 identity로 안정화 검증을 다시 시작했습니다.",
            "retry_attempt": attempt,
        }
    )
    retried = dict(lifecycle)
    retried.pop("failure", None)
    retried.update(
        {
            "phase": RECOVERY_STATUS_VERIFICATION_PENDING,
            "verification": verification,
            "retry": {
                "stage": "verification",
                "attempt": attempt,
                "requested_by": requested_by,
                "requested_at": now.isoformat(),
                "reason": reason,
                "workflow_run_id": workflow_run_id,
            },
        }
    )
    return retried, attempt


@router.post(
    gateway_routes.RCA_RECOVERY_RETRY_BY_CORRELATION_PATH,
    response_model=AcceptedResponse,
    status_code=202,
)
async def retry_recovery_by_correlation(
    correlation_id: str,
    payload: RecoveryRetryRequest,
    current: Any = Depends(require_session),
    db: Any = Depends(get_db),
    events: Any = Depends(get_events),
    preflight: RecoveryActionPreflightPort | None = Depends(get_recovery_action_preflight),
) -> AcceptedResponse:
    workspace_id = current.workspace_id
    record = await db_call(
        db.get_recovery_plan_by_correlation,
        correlation_id,
        workspace_id,
    )
    if record is None:
        raise HTTPException(status_code=HTTP_NOT_FOUND, detail=RECOVERY_PLAN_NOT_FOUND)
    if recovery_text(record.get("plan_id")) != payload.expected_plan_id:
        raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_PLAN_CHANGED)
    record_payload = recovery_object(record.get("payload"))
    plan = RecoveryPlan.from_body(record_payload)
    cluster_id = recovery_text(plan.target.get("cluster_id"))
    require_cluster_access(
        db,
        current,
        workspace_id,
        cluster_id,
        Permission.DEPLOY_RUN.value,
        detail=RECOVERY_SELECTION_ACCESS_DENIED,
    )
    if recovery_text(record.get("status")) != RECOVERY_STATUS_FAILED:
        raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_RETRY_UNAVAILABLE)
    lifecycle = recovery_object(record_payload.get("lifecycle"))
    failure = recovery_object(lifecycle.get("failure"))
    reason_code = recovery_text(failure.get("reason_code"))
    if not reason_code:
        verification = recovery_object(lifecycle.get("verification"))
        reason_code = recovery_text(verification.get("last_reason_code"))
    merge = recovery_object(lifecycle.get("merge"))
    now = normalized_utc(await db_call(db.current_database_time))
    retry_reason = payload.reason or f"operator retried {reason_code}"
    action_id = recovery_text(record.get("selected_action_id"))
    if not action_id:
        raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_RETRY_IDENTITY_INVALID)

    if recovery_text(failure.get("stage")) == RECOVERY_SAFE_PR_FAILED_STAGE:
        if preflight is None:
            raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_RETRY_UNAVAILABLE)
        selected = candidate_by_action_id(plan, action_id)
        if selected.route not in SAFE_PR_ROUTES:
            raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_RETRY_IDENTITY_INVALID)
        proposed = RecoveryActionSelectedBody(
            plan=plan,
            selected=selected,
            selected_by=current.user_id,
            auto_selected=False,
            reason=retry_reason,
            workspace_id=workspace_id,
        )
        prepared = await preflight.prepare(proposed, correlation_id)
        if isinstance(prepared, RcaActionRequiredBody):
            await events.accept_body(
                prepared,
                correlation_id=correlation_id,
                actor=Actor(current.user_id, tuple(current.roles)),
            )
            raise HTTPException(
                status_code=HTTP_CONFLICT,
                detail={
                    "code": prepared.reason_code,
                    "detail": prepared.reason,
                    "missing_evidence": prepared.missing_evidence,
                    "next_actions": prepared.next_actions,
                    "retryable": True,
                },
            )
        attempt = recovery_selection_attempt_number(lifecycle)
        approval_ref = recovery_retry_approval_id(plan.plan_id, action_id, attempt)
        policy_decision_ref = recovery_policy_decision_ref(approval_ref)
        selected = candidate_with_approval(
            prepared,
            approval_ref=approval_ref,
            policy_decision_ref=policy_decision_ref,
        )
        approval_record = db.request_workflow_approval(
            recovery_approval_payload(
                plan,
                selected,
                workspace_id=workspace_id,
                approval_ref=approval_ref,
                policy_decision_ref=policy_decision_ref,
                selected_by=current.user_id,
                reason=retry_reason,
            )
        )
        selected = candidate_with_approval_identity(selected, approval_record)
        next_lifecycle = {
            "phase": "selected",
            "attempt": {
                "id": f"recovery-attempt-{uuid.uuid4()}",
                "number": attempt,
                "action_id": action_id,
                "selected_by": current.user_id,
                "selected_at": now.isoformat(),
            },
            "retry": {
                "stage": RECOVERY_SAFE_PR_FAILED_STAGE,
                "attempt": attempt,
                "requested_by": current.user_id,
                "requested_at": now.isoformat(),
                "reason": retry_reason,
                "previous_failure": dict(failure),
            },
        }
        retry_body = RecoveryRetryRequestedBody(
            plan_id=payload.expected_plan_id,
            incident_id=recovery_text(record.get("incident_id")),
            action_id=action_id,
            retry_stage=RECOVERY_SAFE_PR_FAILED_STAGE,
            attempt=attempt,
            requested_by=current.user_id,
            reason=retry_reason,
            workflow_run_id=recovery_text(selected.draft.params.get("workflow_run_id")) or None,
            workspace_id=workspace_id,
        )
        selected_body = RecoveryActionSelectedBody(
            plan=plan,
            selected=selected,
            selected_by=current.user_id,
            auto_selected=False,
            reason=retry_reason,
            workspace_id=workspace_id,
        )
        with unit_of_work_or_null(db):
            saved = db.update_recovery_plan_lifecycle_if_status(
                payload.expected_plan_id,
                workspace_id,
                expected_statuses=(RECOVERY_STATUS_FAILED,),
                status="selected",
                lifecycle=next_lifecycle,
            )
            if saved is None:
                raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_RETRY_UNAVAILABLE)
            accepted = await events.accept_body(
                retry_body,
                correlation_id=correlation_id,
                actor=Actor(current.user_id, tuple(current.roles)),
            )
            await events.accept_body(
                selected_body,
                correlation_id=correlation_id,
                causation_id=accepted.event.event_id,
                actor=Actor(current.user_id, tuple(current.roles)),
            )
        return AcceptedResponse(
            accepted=True,
            event_id=accepted.event.event_id,
            correlation_id=accepted.event.correlation_id,
        )

    deploy_body: GitWebhookReceivedBody | None = None
    old_workflow_run_id = recovery_text(merge.get("workflow_run_id"))
    if not old_workflow_run_id:
        raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_RETRY_IDENTITY_INVALID)
    workflow = await db_call(db.get_workflow_run, old_workflow_run_id)
    if reason_code == RECOVERY_DEPLOY_FAILED_REASON:
        prepared = deploy_retry_body(
            record=record,
            lifecycle=lifecycle,
            workflow=workflow,
        )
        if prepared is None:
            raise HTTPException(
                status_code=HTTP_CONFLICT,
                detail=RECOVERY_RETRY_IDENTITY_INVALID,
            )
        deploy_body, attempt = prepared
        merge.update(
            {
                "previous_workflow_run_id": old_workflow_run_id,
                "workflow_run_id": deploy_body.workflow_run_id,
                "deployment_request": deploy_body.to_body(),
                "retry_attempt": attempt,
            }
        )
        verification = recovery_object(lifecycle.get("verification"))
        verification.update(
            {
                "status": "waiting_for_deploy",
                "started_at": None,
                "deadline_at": None,
                "healthy_since": None,
                "last_healthy_observed_at": None,
                "distinct_evidence_count": 0,
                "last_evidence_key": None,
                "after": {},
            }
        )
        next_lifecycle = dict(lifecycle)
        next_lifecycle.pop("failure", None)
        next_lifecycle.update(
            {
                "phase": RECOVERY_STATUS_DEPLOY_PENDING,
                "merge": merge,
                "verification": verification,
                "retry": {
                    "stage": "deploy",
                    "attempt": attempt,
                    "requested_by": current.user_id,
                    "requested_at": now.isoformat(),
                    "reason": retry_reason,
                    "workflow_run_id": deploy_body.workflow_run_id,
                },
            }
        )
        next_status = RECOVERY_STATUS_DEPLOY_PENDING
        retry_stage = "deploy"
        retry_workflow_run_id = deploy_body.workflow_run_id
    elif reason_code == RECOVERY_VERIFICATION_EXPIRED_REASON:
        prepared_verification = verification_retry_lifecycle(
            record=record,
            lifecycle=lifecycle,
            workflow=workflow,
            now=now,
            requested_by=current.user_id,
            reason=retry_reason,
        )
        if prepared_verification is None:
            raise HTTPException(
                status_code=HTTP_CONFLICT,
                detail=RECOVERY_RETRY_IDENTITY_INVALID,
            )
        next_lifecycle, attempt = prepared_verification
        next_status = RECOVERY_STATUS_VERIFICATION_PENDING
        retry_stage = "verification"
        retry_workflow_run_id = old_workflow_run_id
    else:
        raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_RETRY_UNAVAILABLE)

    retry_body = RecoveryRetryRequestedBody(
        plan_id=payload.expected_plan_id,
        incident_id=recovery_text(record.get("incident_id")),
        action_id=action_id,
        retry_stage=retry_stage,
        attempt=attempt,
        requested_by=current.user_id,
        reason=retry_reason,
        workflow_run_id=retry_workflow_run_id,
        workspace_id=workspace_id,
    )
    with unit_of_work_or_null(db):
        saved = db.update_recovery_plan_lifecycle_if_status(
            payload.expected_plan_id,
            workspace_id,
            expected_statuses=(RECOVERY_STATUS_FAILED,),
            status=next_status,
            lifecycle=next_lifecycle,
        )
        if saved is None:
            raise HTTPException(status_code=HTTP_CONFLICT, detail=RECOVERY_RETRY_UNAVAILABLE)
        accepted = await events.accept_body(
            retry_body,
            correlation_id=correlation_id,
            actor=Actor(current.user_id, tuple(current.roles)),
        )
        if deploy_body is not None:
            await events.accept_body(
                deploy_body,
                correlation_id=correlation_id,
                causation_id=accepted.event.event_id,
            )
    return AcceptedResponse(
        accepted=True,
        event_id=accepted.event.event_id,
        correlation_id=accepted.event.correlation_id,
    )


@router.get(
    gateway_routes.RCA_RECOVERY_PLAN_BY_CORRELATION_PATH,
    response_model=RecoveryPlanStatusResponse,
)
async def recovery_plan_by_correlation(
    correlation_id: str,
    include_lifecycle: bool = Query(False),
    current: Any = Depends(require_session),
    db: Any = Depends(get_db),
) -> RecoveryPlanStatusResponse:
    workspace_id = current.workspace_id
    record = await db_call(db.get_recovery_plan_by_correlation, correlation_id, workspace_id)
    if record is None:
        raise HTTPException(status_code=HTTP_NOT_FOUND, detail=RECOVERY_PLAN_NOT_FOUND)
    plan = RecoveryPlan.from_body(record["payload"])
    cluster_id = str(plan.target.get("cluster_id", ""))
    if cluster_id:
        require_cluster_access(
            db,
            current,
            workspace_id,
            cluster_id,
            Permission.RCA_READ.value,
            detail=RECOVERY_SELECTION_ACCESS_DENIED,
        )
    return recovery_plan_status_response(
        record,
        plan,
        include_lifecycle=include_lifecycle,
    )


def recovery_action_candidate_item(
    candidate: RecoveryActionCandidate,
) -> RecoveryActionCandidateItem:
    return RecoveryActionCandidateItem(
        action_id=candidate.action_id,
        title=candidate.title,
        description=candidate.description,
        route=candidate.route,
        rank=candidate.rank,
        score=candidate.score,
        risk_level=candidate.risk_level,
        blast_radius=candidate.blast_radius,
        approval_required=candidate.approval_required,
        prerequisites=candidate.prerequisites,
        validation_checks=candidate.validation_checks,
        rollback_plan=candidate.rollback_plan,
        evidence_refs=candidate.evidence_refs,
        recommendation_reason=candidate.recommendation_reason or None,
        expected_outcome=candidate.expected_outcome or None,
        risk_explanation=candidate.risk_explanation or None,
        rollback_reason=candidate.rollback_reason or None,
    )


def recovery_plan_status_response(
    record: dict[str, Any],
    plan: RecoveryPlan,
    *,
    include_lifecycle: bool = False,
) -> RecoveryPlanStatusResponse:
    candidates = [recovery_action_candidate_item(candidate) for candidate in plan.candidates]
    selected_action_id = record.get("selected_action_id")
    selected_action = next(
        (candidate for candidate in candidates if candidate.action_id == selected_action_id),
        None,
    )
    return RecoveryPlanStatusResponse(
        plan_id=plan.plan_id,
        correlation_id=str(record["correlation_id"]),
        incident_id=plan.incident_id,
        evidence_ref=plan.evidence_ref,
        status=str(record["status"]),
        summary=plan.summary,
        target=plan.target,
        recommended_action_id=plan.recommended_action_id,
        execution_route=plan.execution_route,
        selection_required=plan.selection_required,
        selected_action_id=str(selected_action_id) if selected_action_id else None,
        selected_by=str(record["selected_by"]) if record.get("selected_by") else None,
        selected_action=selected_action,
        candidates=candidates,
        lifecycle=(
            dict(record.get("payload", {}).get("lifecycle", {}))
            if include_lifecycle
            and isinstance(record.get("payload"), dict)
            and isinstance(record["payload"].get("lifecycle"), dict)
            else None
        ),
    )


async def db_call(func: Any, *args: Any, **kwargs: Any) -> Any:
    return await to_thread_db_retry(func, *args, **kwargs)

"""Read and reduce one Argo CD Application without trusting it as cluster truth."""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from domains.gitops.events import GitChangedBody
from domains.gitops.repository_discovery import (
    RepositoryDiscoveryError,
    normalize_github_repo_ref,
)
from packages.config.settings import env
from packages.runtime.app import EventContext
from packages.security.credentials import CredentialEncryptionError, decrypt_credential

MAX_APPLICATION_RESPONSE_BYTES = 1_048_576
GIT_SHA = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
ARGOCD_PROVIDER = "argocd"


class ArgoObservationError(Exception):
    def __init__(
        self,
        reason: str,
        *,
        retryable: bool = False,
        request_count: int = 0,
        response_bytes: int = 0,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.retryable = retryable
        self.request_count = request_count
        self.response_bytes = response_bytes


@dataclass(frozen=True)
class ArgoApplicationTarget:
    server_url: str
    project: str
    application: str
    repo_ref: str
    manifest_path: str


@dataclass(frozen=True)
class ArgoApplicationObservation:
    project: str
    application: str
    repo_ref: str
    sync_revision: str
    operation_revision: str
    sync_status: str
    operation_phase: str
    health_status: str
    condition_types: str
    reconciled_at: datetime | None
    checked_at: datetime
    response_bytes: int
    request_count: int
    elapsed_ms: int


def classify_argo_application(
    observation: ArgoApplicationObservation,
    expected_sha: str,
    *,
    now: datetime,
    maximum_age_seconds: int,
) -> str:
    if not GIT_SHA.fullmatch(expected_sha):
        return "unknown"
    age_seconds = (now - observation.checked_at).total_seconds()
    if maximum_age_seconds <= 0 or not 0 <= age_seconds <= maximum_age_seconds:
        return "stale"
    if observation.sync_revision != expected_sha:
        return "pending"
    if observation.sync_status != "Synced":
        return "pending"
    if observation.operation_revision != expected_sha or observation.operation_phase != "Succeeded":
        return "unconfirmed"
    if observation.condition_types:
        return "unconfirmed"
    if observation.reconciled_at is None:
        return "unconfirmed"
    return "reported_applied"


class ArgoApplicationClient:
    def __init__(
        self,
        token: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.token = token
        self.transport = transport

    # cost: time O(b), heap O(b), stack O(1), io 1
    # vars: b = 제한 내 Application 응답 바이트 수
    # basis: estimate
    async def observe(self, target: ArgoApplicationTarget) -> ArgoApplicationObservation:
        if not self.token:
            raise ArgoObservationError("credential_unavailable")
        url = _application_url(target)
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(
                timeout=5.0,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                async with client.stream(
                    "GET",
                    url,
                    params={"project": target.project},
                    headers={"Authorization": f"Bearer {self.token}", "Accept": "application/json"},
                ) as response:
                    if response.status_code in (401, 403):
                        raise ArgoObservationError("access_denied", request_count=1)
                    if response.status_code == 404:
                        raise ArgoObservationError("application_missing", request_count=1)
                    if response.status_code != 200:
                        raise ArgoObservationError(
                            "upstream_unavailable",
                            retryable=response.status_code >= 500,
                            request_count=1,
                        )
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        content.extend(chunk)
                        if len(content) > MAX_APPLICATION_RESPONSE_BYTES:
                            raise ArgoObservationError(
                                "response_too_large",
                                request_count=1,
                                response_bytes=len(content),
                            )
        except httpx.RequestError as exc:
            raise ArgoObservationError(
                "upstream_unavailable", retryable=True, request_count=1
            ) from exc
        elapsed_ms = round((time.monotonic() - started) * 1000)
        try:
            document = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
            raise ArgoObservationError(
                "invalid_response", request_count=1, response_bytes=len(content)
            ) from exc
        try:
            return _reduce_application(document, target, len(content), elapsed_ms)
        except ArgoObservationError as exc:
            raise ArgoObservationError(
                exc.reason, request_count=1, response_bytes=len(content)
            ) from exc


def argo_credential_scope(binding_id: str) -> str:
    return f"binding:{binding_id}"


# cost: time O(n + b), heap O(n + b), stack O(1), io 3-5
# vars: n = 바인딩 조회 행 수, b = Argo Application 응답 바이트 수
# basis: estimate
async def observe_argocd_for_git_change(
    event: GitChangedBody,
    context: EventContext[Any],
    *,
    correlation_id: str | None = None,
) -> None:
    incident_id = correlation_id or context.correlation_id
    if not GIT_SHA.fullmatch(event.commit_sha) or not incident_id:
        raise ArgoObservationError("invalid_git_event")
    targets = await context.db.list_active_github_poll_targets(
        event.workspace_id, binding_id=event.binding_id
    )
    if len(targets) != 1:
        raise ArgoObservationError("binding_unavailable")
    target = targets[0]
    for field in ("repository_id", "repo_ref", "application_id", "cluster_id", "manifest_path"):
        if str(target.get(field) or "") != str(getattr(event, field)):
            raise ArgoObservationError("binding_mismatch")
    policy = _mapping(target.get("deploy_policy"))
    config = _mapping(policy.get("argocd"))
    project = str(config.get("project") or "")
    application = str(config.get("application") or "")
    payload: dict[str, Any] = {
        "workspace_id": event.workspace_id,
        "binding_id": event.binding_id,
        "correlation_id": incident_id,
        "expected_sha": event.commit_sha,
        "project": project,
        "application": application,
        "status": "unavailable",
        "reason": "not_configured",
        "request_count": 0,
        "response_bytes": 0,
        "checked_at": datetime.now(UTC),
    }
    server_url = env("ARGOCD_SERVER_URL", "").strip()
    if project and application and server_url:
        try:
            token = await _binding_token(event, context)
            observed = await ArgoApplicationClient(token).observe(
                ArgoApplicationTarget(
                    server_url=server_url,
                    project=project,
                    application=application,
                    repo_ref=event.repo_ref,
                    manifest_path=event.manifest_path,
                )
            )
            payload.update(
                status=classify_argo_application(
                    observed,
                    event.commit_sha,
                    now=observed.checked_at,
                    maximum_age_seconds=1,
                ),
                reason=None,
                sync_revision=observed.sync_revision,
                operation_revision=observed.operation_revision,
                sync_status=observed.sync_status,
                operation_phase=observed.operation_phase,
                health_status=observed.health_status,
                condition_types=observed.condition_types,
                reconciled_at=observed.reconciled_at,
                request_count=observed.request_count,
                response_bytes=observed.response_bytes,
                checked_at=observed.checked_at,
            )
        except (ArgoObservationError, CredentialEncryptionError) as exc:
            payload.update(
                reason=getattr(exc, "reason", "credential_unavailable"),
                request_count=getattr(exc, "request_count", 0),
                response_bytes=getattr(exc, "response_bytes", 0),
            )
    payload["fingerprint"] = hashlib.sha256(
        json.dumps(
            {
                key: payload.get(key)
                for key in (
                    "project",
                    "application",
                    "status",
                    "reason",
                    "sync_revision",
                    "operation_revision",
                    "sync_status",
                    "operation_phase",
                    "health_status",
                    "condition_types",
                )
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    await context.db.record_argo_application_observation(payload)


async def _binding_token(event: GitChangedBody, context: EventContext[Any]) -> str:
    scope = argo_credential_scope(event.binding_id)
    stored = await context.db.get_workspace_credential(event.workspace_id, ARGOCD_PROVIDER, scope)
    if stored is None or any(
        stored.get(field) != value
        for field, value in (
            ("workspace_id", event.workspace_id),
            ("provider", ARGOCD_PROVIDER),
            ("scope", scope),
        )
    ):
        raise ArgoObservationError("credential_unavailable")
    return decrypt_credential(str(stored["encrypted_value"]))


def _application_url(target: ArgoApplicationTarget) -> str:
    parsed = urlsplit(target.server_url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or not target.project
        or not target.application
        or "/" in target.application
    ):
        raise ArgoObservationError("invalid_target")
    return (
        f"{target.server_url.rstrip('/')}/api/v1/applications/{quote(target.application, safe='')}"
    )


def _reduce_application(
    document: Any,
    target: ArgoApplicationTarget,
    response_bytes: int,
    elapsed_ms: int,
) -> ArgoApplicationObservation:
    if not isinstance(document, dict):
        raise ArgoObservationError("invalid_response")
    metadata = _mapping(document.get("metadata"))
    spec = _mapping(document.get("spec"))
    status = _mapping(document.get("status"))
    if metadata.get("name") != target.application or spec.get("project") != target.project:
        raise ArgoObservationError("application_mismatch")
    if metadata.get("deletionTimestamp"):
        raise ArgoObservationError("application_deleting")
    if "sources" in spec:
        raise ArgoObservationError("multiple_sources_unsupported")
    source = _mapping(spec.get("source"))
    repo_ref = _github_repo_ref(source.get("repoURL"))
    try:
        expected_repo_ref = normalize_github_repo_ref(target.repo_ref)
    except (RepositoryDiscoveryError, ValueError) as exc:
        raise ArgoObservationError("repository_mismatch") from exc
    if repo_ref != expected_repo_ref:
        raise ArgoObservationError("repository_mismatch")
    source_path = str(source.get("path") or "").strip("/")
    if source_path and not target.manifest_path.startswith(f"{source_path}/"):
        raise ArgoObservationError("source_path_mismatch")
    sync = _mapping(status.get("sync"))
    operation = _mapping(status.get("operationState"))
    sync_result = _mapping(operation.get("syncResult"))
    health = _mapping(status.get("health"))
    conditions = status.get("conditions")
    if conditions is not None and not isinstance(conditions, list):
        raise ArgoObservationError("invalid_response")
    condition_types = ",".join(
        sorted(
            {
                str(condition["type"])[:80]
                for condition in (conditions or [])[:20]
                if isinstance(condition, dict) and condition.get("type")
            }
        )
    )
    if conditions and not condition_types:
        condition_types = "unknown_condition"
    reconciled_at = _parse_time(status.get("reconciledAt"))
    return ArgoApplicationObservation(
        project=target.project,
        application=target.application,
        repo_ref=repo_ref,
        sync_revision=str(sync.get("revision") or ""),
        operation_revision=str(sync_result.get("revision") or ""),
        sync_status=str(sync.get("status") or ""),
        operation_phase=str(operation.get("phase") or ""),
        health_status=str(health.get("status") or ""),
        condition_types=condition_types,
        reconciled_at=reconciled_at,
        checked_at=datetime.now(UTC),
        response_bytes=response_bytes,
        request_count=1,
        elapsed_ms=elapsed_ms,
    )


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _github_repo_ref(value: Any) -> str:
    if not isinstance(value, str):
        raise ArgoObservationError("repository_mismatch")
    if value.startswith("git@github.com:"):
        suffix = value.removeprefix("git@github.com:")
    else:
        parsed = urlsplit(value)
        if parsed.hostname != "github.com" or parsed.scheme not in {"https", "ssh"}:
            raise ArgoObservationError("repository_mismatch")
        suffix = parsed.path.lstrip("/")
    try:
        return normalize_github_repo_ref(suffix.removesuffix(".git"))
    except (RepositoryDiscoveryError, ValueError) as exc:
        raise ArgoObservationError("repository_mismatch") from exc


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else None

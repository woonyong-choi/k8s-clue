"""gitops 도메인 HTTP 라우터 — GitHub webhook 입구(라우터 단위 HMAC 서명 검증)."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from domains.gitops.dependencies import verify_github_signature
from domains.gitops.events import (
    GitWebhookReceivedBody,
)
from domains.gitops.recovery_merge import (
    approved_change_contract,
    approved_replica_count,
)
from domains.rca.events import (
    RecoveryPrMergedBody,
    RecoveryVerificationFailedBody,
)
from packages.config.constants import Target
from packages.contracts.gateway import routes as gateway_routes
from packages.contracts.gateway.requests import (
    DEFAULT_WEBHOOK_REPLICAS,
    GitHubWebhookRequest,
)
from packages.contracts.gateway.responses import AcceptedEventResponse
from packages.contracts.gitops import (
    DEFAULT_APPLICATION_ID,
    DEFAULT_DEPLOYMENT_BINDING_ID,
    DEFAULT_ENVIRONMENT,
    DEFAULT_MANIFEST_PATH,
    DEFAULT_REPO_BRANCH,
    DEFAULT_REPO_REF,
    DEFAULT_REPOSITORY_ID,
    DEFAULT_WATCH_TARGET_ID,
    RepositoryStatus,
)
from packages.contracts.identity import DEFAULT_WORKSPACE_ID
from packages.events.context import event_workspace
from packages.runtime.dependencies import get_db, get_events
from packages.storage.engine import unit_of_work_or_null

router = APIRouter(dependencies=[Depends(verify_github_signature)])
APPROVAL_NOT_FOUND = "approval not found"
APPROVAL_DIFF_MISSING = "approval diff is missing"
APPROVAL_ACCESS_DENIED = "approval access denied"
APPROVAL_CONFLICT = "approval already resolved"
HTTP_NOT_FOUND = 404
HTTP_CONFLICT = 409
GITHUB_PUSH_EVENT = "push"
GITHUB_PULL_REQUEST_EVENT = "pull_request"
# App 이 자동 수신하는 수명주기 이벤트 — 외부(GitHub) 변경을 내부 상태로 반영해
# 고아(연결은 남아있는데 실제로는 죽은) 상태를 없앤다.
GITHUB_INSTALLATION_EVENT = "installation"
GITHUB_INSTALLATION_REPOSITORIES_EVENT = "installation_repositories"
GITHUB_REPOSITORY_EVENT = "repository"
GITHUB_LIFECYCLE_EVENTS = frozenset(
    {
        GITHUB_INSTALLATION_EVENT,
        GITHUB_INSTALLATION_REPOSITORIES_EVENT,
        GITHUB_REPOSITORY_EVENT,
    }
)
RECOVERY_STATUS_PR_OPEN = "pr_open"
RECOVERY_STATUS_DEPLOY_PENDING = "deploy_pending"
RECOVERY_STATUS_FAILED = "failed"
RECOVERY_STATUS_SELECTION_REQUESTED = "selection_requested"


def recovery_merge_workflow_run_id(plan_id: str, merge_commit_sha: str) -> str:
    raw = f"{plan_id}|{merge_commit_sha}|recovery-merge"
    return f"workflow-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def build_git_webhook_body(payload: GitHubWebhookRequest) -> GitWebhookReceivedBody:
    return GitWebhookReceivedBody(**payload.model_dump())


def github_repo_ref(payload: Mapping[str, Any]) -> str:
    repository = payload.get("repository")
    if not isinstance(repository, Mapping):
        return ""
    return str(repository.get("full_name") or "").strip()


def github_branch_from_ref(ref: str) -> str:
    prefix = "refs/heads/"
    return ref.removeprefix(prefix) if ref.startswith(prefix) else ref


def github_push_commit(payload: Mapping[str, Any]) -> tuple[str, str] | None:
    ref = str(payload.get("ref") or "")
    commit_sha = str(payload.get("after") or "")
    if not ref.startswith("refs/heads/") or not commit_sha or set(commit_sha) == {"0"}:
        return None
    return github_branch_from_ref(ref), commit_sha


def github_merged_pr_commit(payload: Mapping[str, Any]) -> tuple[str, str] | None:
    if str(payload.get("action") or "") != "closed":
        return None
    pull_request = payload.get("pull_request")
    if not isinstance(pull_request, Mapping) or pull_request.get("merged") is not True:
        return None
    base = pull_request.get("base")
    branch = str(base.get("ref") or "") if isinstance(base, Mapping) else ""
    commit_sha = str(pull_request.get("merge_commit_sha") or "")
    if not branch or not commit_sha:
        return None
    return branch, commit_sha


def github_raw_change(payload: Mapping[str, Any], event_name: str) -> tuple[str, str, str] | None:
    repo_ref = github_repo_ref(payload)
    if not repo_ref:
        return None
    if event_name == GITHUB_PULL_REQUEST_EVENT:
        changed = github_merged_pr_commit(payload)
    else:
        changed = github_push_commit(payload)
    if changed is None:
        return None
    branch, commit_sha = changed
    return repo_ref, branch, commit_sha


def body_for_poll_target(
    target: Mapping[str, Any],
    *,
    commit_sha: str,
    image: str,
    correlation_id: str | None = None,
    replicas: int = DEFAULT_WEBHOOK_REPLICAS,
    force: bool = False,
) -> GitWebhookReceivedBody:
    return GitWebhookReceivedBody(
        correlation_id=correlation_id,
        commit_sha=commit_sha,
        image=image,
        replicas=replicas,
        workspace_id=str(target.get("workspace_id") or DEFAULT_WORKSPACE_ID),
        repository_id=str(target.get("repository_id") or DEFAULT_REPOSITORY_ID),
        repo_ref=str(target.get("repo_ref") or DEFAULT_REPO_REF),
        branch=str(target.get("branch") or DEFAULT_REPO_BRANCH),
        watch_target_id=str(target.get("watch_target_id") or DEFAULT_WATCH_TARGET_ID),
        binding_id=str(target.get("binding_id") or DEFAULT_DEPLOYMENT_BINDING_ID),
        application_id=str(target.get("application_id") or DEFAULT_APPLICATION_ID),
        environment=str(target.get("environment") or DEFAULT_ENVIRONMENT),
        cluster_id=str(target.get("cluster_id") or Target.DEFAULT_CLUSTER_ID),
        manifest_path=str(target.get("manifest_path") or DEFAULT_MANIFEST_PATH),
        source_type=str(target.get("source_type") or ""),
        force=force,
    )


def active_github_poll_targets(db: Any | None) -> list[Mapping[str, Any]]:
    if db is None or not hasattr(db, "list_active_github_poll_targets"):
        return []
    return [
        target
        for target in db.list_active_github_poll_targets(limit=1000)
        if isinstance(target, Mapping)
        and str(target.get("workspace_id") or "").strip()
        and str(target.get("repo_ref") or "").strip()
        and str(target.get("branch") or "").strip()
    ]


def poll_target_matches(
    target: Mapping[str, Any],
    *,
    workspace_id: str,
    repository_id: str,
    repo_ref: str,
    branch: str,
    watch_target_id: str,
    binding_id: str,
    application_id: str,
    environment: str,
    cluster_id: str,
    manifest_path: str,
    source_type: str,
) -> bool:
    return (
        str(target.get("workspace_id") or "") == workspace_id
        and str(target.get("repository_id") or "") == repository_id
        and str(target.get("repo_ref") or "").lower() == repo_ref.lower()
        and str(target.get("branch") or "") == branch
        and str(target.get("watch_target_id") or "") == watch_target_id
        and str(target.get("binding_id") or "") == binding_id
        and str(target.get("application_id") or "") == application_id
        and str(target.get("environment") or "") == environment
        and str(target.get("cluster_id") or "") == cluster_id
        and str(target.get("manifest_path") or "") == manifest_path
        and str(target.get("source_type") or "") == source_type
    )


def build_git_webhook_bodies(
    payload: Mapping[str, Any],
    *,
    db: Any | None = None,
    event_name: str = "",
) -> list[GitWebhookReceivedBody]:
    targets = active_github_poll_targets(db)
    try:
        requested = build_git_webhook_body(GitHubWebhookRequest(**dict(payload)))
    except ValidationError:
        requested = None
    if requested is not None:
        matched = [
            target
            for target in targets
            if poll_target_matches(
                target,
                workspace_id=requested.workspace_id,
                repository_id=requested.repository_id,
                repo_ref=requested.repo_ref,
                branch=requested.branch,
                watch_target_id=requested.watch_target_id,
                binding_id=requested.binding_id,
                application_id=requested.application_id,
                environment=requested.environment,
                cluster_id=requested.cluster_id,
                manifest_path=requested.manifest_path,
                source_type=requested.source_type,
            )
        ]
        return [
            body_for_poll_target(
                target,
                commit_sha=requested.commit_sha,
                image=requested.image,
                correlation_id=requested.correlation_id,
                replicas=requested.replicas,
                force=requested.force,
            )
            for target in matched
        ]
    raw_change = github_raw_change(payload, event_name)
    if raw_change is None:
        return []
    repo_ref, branch, commit_sha = raw_change
    matched = [
        body_for_poll_target(target, commit_sha=commit_sha, image="")
        for target in targets
        if str(target.get("repo_ref") or "").lower() == repo_ref.lower()
        and str(target.get("branch") or "") == branch
    ]
    return matched


def _installation_id(payload: Mapping[str, Any]) -> str:
    installation = payload.get("installation")
    if isinstance(installation, Mapping):
        return str(installation.get("id") or "")
    return ""


def github_lifecycle_intents(
    payload: Mapping[str, Any],
    event_name: str,
) -> list[dict[str, str]]:
    """수명주기 이벤트 → 저장소 상태 전이 의도(순수 함수, DB 무접근).

    - installation deleted/suspend → 그 설치에 묶인 모든 저장소를 invalid_credential
      (권한 회수/앱 제거). 자격증명이 더는 유효하지 않음을 정직하게 표시.
    - installation_repositories.repositories_removed → 해당 저장소 접근 상실 →
      invalid_credential.
    - repository deleted/archived/renamed/transferred → 소스 소실 → source_unreachable.
    added/created/unsuspend 등 '복구/증가' 이벤트는 상태를 내리지 않는다(무동작).
    """
    if event_name == GITHUB_INSTALLATION_EVENT:
        action = str(payload.get("action") or "")
        installation_id = _installation_id(payload)
        if installation_id and action in {"deleted", "suspend"}:
            return [
                {
                    "kind": "installation",
                    "installation_id": installation_id,
                    "status": RepositoryStatus.INVALID_CREDENTIAL.value,
                }
            ]
        return []
    if event_name == GITHUB_INSTALLATION_REPOSITORIES_EVENT:
        removed = payload.get("repositories_removed")
        intents: list[dict[str, str]] = []
        if isinstance(removed, list):
            for repo in removed:
                full = str((repo or {}).get("full_name") or "") if isinstance(repo, Mapping) else ""
                if full:
                    intents.append(
                        {
                            "kind": "repo",
                            "repo_ref": full,
                            "status": RepositoryStatus.INVALID_CREDENTIAL.value,
                        }
                    )
        return intents
    if event_name == GITHUB_REPOSITORY_EVENT:
        action = str(payload.get("action") or "")
        repository = payload.get("repository")
        full = str(repository.get("full_name") or "") if isinstance(repository, Mapping) else ""
        refs: list[str] = []
        if action in {"deleted", "archived"} and full:
            refs.append(full)
        elif action in {"renamed", "transferred"}:
            # rename/transfer 후엔 새 full_name 이 오므로, 우리가 저장한 '이전' ref 를
            # changes.repository.name.from + 기존 owner 로 최선 복원한다.
            owner = full.split("/")[0] if "/" in full else ""
            changes = payload.get("changes")
            old_name = ""
            if isinstance(changes, Mapping):
                repo_change = changes.get("repository")
                if isinstance(repo_change, Mapping):
                    name_change = repo_change.get("name")
                    if isinstance(name_change, Mapping):
                        old_name = str(name_change.get("from") or "")
            if owner and old_name:
                refs.append(f"{owner}/{old_name}")
            if full:
                refs.append(full)
        return [
            {"kind": "repo", "repo_ref": ref, "status": RepositoryStatus.SOURCE_UNREACHABLE.value}
            for ref in refs
        ]
    return []


def apply_github_lifecycle(db: Any, intents: list[dict[str, str]]) -> int:
    """수명주기 의도를 실제 저장소 상태로 반영하고 영향받은 저장소 수를 돌려준다.

    설치 단위 의도는 그 설치 참조에 묶인 저장소들을 찾아 일괄 전이하고, 캐시된
    설치 토큰을 무효화한다. 저장소가 없으면 조용히 0(멱등).
    """
    setter = getattr(db, "set_repository_connection_status", None)
    if not callable(setter):
        return 0
    from domains.scm.github_app_credentials import (
        invalidate_installation_token,
        make_app_installation_ref,
    )

    affected = 0
    for intent in intents:
        status = intent["status"]
        if intent.get("kind") == "installation":
            installation_id = intent["installation_id"]
            lister = getattr(db, "list_repositories_by_credential_ref", None)
            repositories = (
                lister(DEFAULT_WORKSPACE_ID, make_app_installation_ref(installation_id))
                if callable(lister)
                else []
            )
            for repository in repositories:
                repo_ref = str(repository.get("repo_ref") or "")
                if repo_ref and setter(DEFAULT_WORKSPACE_ID, repo_ref, status):
                    affected += 1
            invalidate_installation_token(installation_id)
        else:
            repo_ref = intent.get("repo_ref", "")
            if repo_ref and setter(DEFAULT_WORKSPACE_ID, repo_ref, status):
                affected += 1
    return affected


def accepted_event_response(accepted: Any) -> AcceptedEventResponse:
    return AcceptedEventResponse(
        accepted=True,
        event_id=accepted.event.event_id,
        correlation_id=accepted.event.correlation_id,
        event=accepted.event.to_dict(),
    )


def github_pull_request_identity(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    pull = payload.get("pull_request")
    if not isinstance(pull, Mapping):
        return None
    repository = payload.get("repository")
    head = pull.get("head")
    base = pull.get("base")
    if (
        not isinstance(repository, Mapping)
        or not isinstance(head, Mapping)
        or not isinstance(base, Mapping)
    ):
        return None
    number = pull.get("number") or payload.get("number")
    if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
        return None
    identity = {
        "action": str(payload.get("action") or ""),
        "merged": pull.get("merged") is True,
        "url": str(pull.get("html_url") or ""),
        "number": number,
        "node_id": str(pull.get("node_id") or ""),
        "repo_ref": str(repository.get("full_name") or ""),
        "base_branch": str(base.get("ref") or ""),
        "head_ref": str(head.get("ref") or ""),
        "head_sha": str(head.get("sha") or ""),
        "merge_commit_sha": str(pull.get("merge_commit_sha") or ""),
    }
    return (
        identity
        if all(
            identity[key]
            for key in (
                "url",
                "node_id",
                "repo_ref",
                "base_branch",
                "head_ref",
                "head_sha",
            )
        )
        else None
    )


def exact_recovery_poll_target(
    db: Any,
    record: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return None
    lifecycle = payload.get("lifecycle")
    target = payload.get("target")
    if not isinstance(lifecycle, Mapping) or not isinstance(target, Mapping):
        return None
    pr = lifecycle.get("pr")
    if not isinstance(pr, Mapping):
        return None
    expected = {
        "workspace_id": str(record.get("workspace_id") or ""),
        "repository_id": str(pr.get("repository_id") or ""),
        "repo_ref": str(pr.get("repo_ref") or ""),
        "branch": str(pr.get("base_branch") or ""),
        "binding_id": str(pr.get("binding_id") or ""),
        "application_id": str(pr.get("application_id") or ""),
        "environment": str(pr.get("environment") or ""),
        "cluster_id": str(pr.get("cluster_id") or target.get("cluster_id") or ""),
        "manifest_path": str(pr.get("manifest_path") or ""),
    }
    if any(not value for value in expected.values()):
        return None
    matches = []
    for candidate in active_github_poll_targets(db):
        if all(
            (
                str(candidate.get("workspace_id") or "") == expected["workspace_id"],
                str(candidate.get("repository_id") or "") == expected["repository_id"],
                str(candidate.get("repo_ref") or "").casefold() == expected["repo_ref"].casefold(),
                str(candidate.get("branch") or "") == expected["branch"],
                str(candidate.get("binding_id") or "") == expected["binding_id"],
                str(candidate.get("application_id") or "") == expected["application_id"],
                str(candidate.get("environment") or "") == expected["environment"],
                str(candidate.get("cluster_id") or "") == expected["cluster_id"],
                str(candidate.get("manifest_path") or "") == expected["manifest_path"],
            )
        ):
            matches.append(candidate)
    return matches[0] if len(matches) == 1 else None


async def reject_tracked_recovery_pull_request(
    *,
    db: Any,
    events: Any,
    record: Mapping[str, Any],
    reason_code: str,
    reason: str,
) -> AcceptedEventResponse | JSONResponse:
    payload = record.get("payload")
    lifecycle = dict(payload.get("lifecycle") or {}) if isinstance(payload, Mapping) else {}
    lifecycle["phase"] = RECOVERY_STATUS_FAILED
    lifecycle["failure"] = {"reason_code": reason_code, "reason": reason}
    workspace_id = str(record["workspace_id"])
    with unit_of_work_or_null(db):
        saved = db.update_recovery_plan_lifecycle_if_status(
            str(record["plan_id"]),
            workspace_id,
            expected_statuses=(RECOVERY_STATUS_PR_OPEN,),
            # PR이 merge/deploy 단계에 도달하지 못한 실패는 운영자가 권위
            # context를 보정한 뒤 같은 후보를 다시 선택할 수 있게 연다.
            status=RECOVERY_STATUS_SELECTION_REQUESTED,
            lifecycle=lifecycle,
            clear_selection=True,
        )
        if saved is None:
            return JSONResponse(
                status_code=202,
                content={"accepted": True, "ignored": True, "reason": "stale recovery PR event"},
            )
        verification = lifecycle.get("verification")
        before = dict(verification.get("before") or {}) if isinstance(verification, Mapping) else {}
        with event_workspace(workspace_id):
            accepted = await events.accept_body(
                RecoveryVerificationFailedBody(
                    plan_id=str(record["plan_id"]),
                    incident_id=str(record["incident_id"]),
                    reason_code=reason_code,
                    reason=reason,
                    evidence_ref=str(record.get("evidence_ref") or "unknown"),
                    before=before,
                    workspace_id=workspace_id,
                ),
                correlation_id=str(record["correlation_id"]),
            )
    return accepted_event_response(accepted)


async def handle_tracked_recovery_pull_request(
    *,
    payload: Mapping[str, Any],
    db: Any,
    events: Any,
) -> AcceptedEventResponse | JSONResponse | None:
    identity = github_pull_request_identity(payload)
    if identity is None:
        return None
    base_record = await asyncio.to_thread(
        db.find_open_recovery_plan_for_pull_request_base_identity,
        pr_url=identity["url"],
        repo_ref=identity["repo_ref"],
        base_branch=identity["base_branch"],
        pr_number=identity["number"],
        pr_node_id=identity["node_id"],
        head_ref=identity["head_ref"],
    )
    if base_record is None:
        return None
    if identity["action"] == "synchronize":
        return await reject_tracked_recovery_pull_request(
            db=db,
            events=events,
            record=base_record,
            reason_code="safe_pr_head_changed",
            reason="PR 생성 후 head가 변경되어 승인된 patch identity를 더 이상 신뢰할 수 없습니다.",
        )
    if identity["action"] != "closed":
        return JSONResponse(
            status_code=202,
            content={"accepted": True, "ignored": True, "reason": "recovery PR is still open"},
        )
    if not identity["merged"]:
        return await reject_tracked_recovery_pull_request(
            db=db,
            events=events,
            record=base_record,
            reason_code="safe_pr_closed_without_merge",
            reason="복구 PR이 merge되지 않고 닫혔습니다.",
        )
    exact_record = await asyncio.to_thread(
        db.find_open_recovery_plan_for_pull_request,
        pr_url=identity["url"],
        repo_ref=identity["repo_ref"],
        base_branch=identity["base_branch"],
        pr_number=identity["number"],
        pr_node_id=identity["node_id"],
        head_ref=identity["head_ref"],
        head_sha=identity["head_sha"],
    )
    if exact_record is None:
        return await reject_tracked_recovery_pull_request(
            db=db,
            events=events,
            record=base_record,
            reason_code="safe_pr_head_mismatch",
            reason="merge webhook의 head SHA가 생성 시 저장한 Safe PR head SHA와 다릅니다.",
        )
    target = exact_recovery_poll_target(db, exact_record)
    if target is None:
        return await reject_tracked_recovery_pull_request(
            db=db,
            events=events,
            record=exact_record,
            reason_code="recovery_binding_unavailable",
            reason="merge된 PR과 정확히 일치하는 활성 deployment binding을 찾지 못했습니다.",
        )
    record_payload = exact_record["payload"]
    lifecycle = dict(record_payload.get("lifecycle") or {})
    verification = lifecycle.get("verification")
    verification_blockers = (
        [str(value) for value in verification.get("blockers", []) if str(value)]
        if isinstance(verification, Mapping) and isinstance(verification.get("blockers"), list)
        else []
    )
    if verification_blockers:
        return JSONResponse(
            status_code=409,
            content={
                "accepted": False,
                "reason": "recovery merge blocked by missing verification baseline",
                "missing_evidence": verification_blockers,
            },
        )
    pr = dict(lifecycle.get("pr") or {})
    approved_changes = approved_change_contract(record_payload)
    approved_replicas = approved_replica_count(record_payload)
    if not identity["merge_commit_sha"] or approved_changes is None:
        return await reject_tracked_recovery_pull_request(
            db=db,
            events=events,
            record=exact_record,
            reason_code="recovery_deploy_context_incomplete",
            reason="merge commit 또는 승인된 변경 정보가 없어 추적을 시작할 수 없습니다.",
        )
    body = body_for_poll_target(
        target,
        commit_sha=identity["merge_commit_sha"],
        image="",
        correlation_id=str(exact_record["correlation_id"]),
        replicas=approved_replicas or DEFAULT_WEBHOOK_REPLICAS,
        force=True,
    )
    # A normal base-branch push may finish before the PR-closed webhook. Give
    # recovery its own deterministic run so delivery order cannot strand the
    # plan behind a terminal commit-scoped workflow.
    workflow_run_id = recovery_merge_workflow_run_id(
        str(exact_record["plan_id"]),
        identity["merge_commit_sha"],
    )
    body = replace(body, workflow_run_id=workflow_run_id)
    now = db.current_database_time()
    lifecycle.update(
        {
            "phase": RECOVERY_STATUS_DEPLOY_PENDING,
            "merge": {
                "pr_url": identity["url"],
                "head_sha": identity["head_sha"],
                "merge_commit_sha": identity["merge_commit_sha"],
                "merged_at": now.isoformat(),
                "workflow_run_id": workflow_run_id,
                "repository_id": pr.get("repository_id"),
                "binding_id": pr.get("binding_id"),
                "application_id": pr.get("application_id"),
                "cluster_id": pr.get("cluster_id"),
                "attempt_id": (
                    lifecycle.get("attempt", {}).get("id")
                    if isinstance(lifecycle.get("attempt"), Mapping)
                    else None
                ),
                "retry_attempt": 0,
                # A later explicit deploy retry must replay this exact,
                # server-derived binding/commit request rather than reconstruct
                # authority from browser input or a mutable poll target.
                "deployment_request": body.to_body(),
            },
        }
    )
    workspace_id = str(exact_record["workspace_id"])
    with unit_of_work_or_null(db):
        saved = db.update_recovery_plan_lifecycle_if_status(
            str(exact_record["plan_id"]),
            workspace_id,
            expected_statuses=(RECOVERY_STATUS_PR_OPEN,),
            status=RECOVERY_STATUS_DEPLOY_PENDING,
            lifecycle=lifecycle,
        )
        if saved is None:
            return JSONResponse(
                status_code=202,
                content={"accepted": True, "ignored": True, "reason": "duplicate recovery merge"},
            )
        with event_workspace(workspace_id):
            merged = await events.accept_body(
                RecoveryPrMergedBody(
                    plan_id=str(exact_record["plan_id"]),
                    incident_id=str(exact_record["incident_id"]),
                    pr_url=identity["url"],
                    merge_commit_sha=identity["merge_commit_sha"],
                    repository_id=str(pr["repository_id"]),
                    repo_ref=str(pr["repo_ref"]),
                    binding_id=str(pr["binding_id"]),
                    application_id=str(pr["application_id"]),
                    workflow_run_id=workflow_run_id,
                    cluster_id=str(pr["cluster_id"]),
                    workspace_id=workspace_id,
                ),
                correlation_id=str(exact_record["correlation_id"]),
            )
            await events.accept_body(
                body,
                correlation_id=str(exact_record["correlation_id"]),
                causation_id=merged.event.event_id,
            )
    return accepted_event_response(merged)


@router.post(gateway_routes.GITHUB_WEBHOOK_PATH, response_model=AcceptedEventResponse)
async def github_webhook(
    request: Request,
    payload: dict[str, Any] = Body(...),
    events: Any = Depends(get_events),
    db: Any = Depends(get_db),
) -> AcceptedEventResponse | JSONResponse:
    event_name = request.headers.get("x-github-event", "")
    # 배포 이벤트 이전에 수명주기 이벤트를 상태 전이로 흡수한다(고아 방지).
    # 서명은 라우터 의존성에서 이미 검증됨.
    if event_name in GITHUB_LIFECYCLE_EVENTS:
        intents = github_lifecycle_intents(payload, event_name)
        affected = await asyncio.to_thread(apply_github_lifecycle, db, intents)
        return JSONResponse(
            status_code=202,
            content={
                "accepted": True,
                "lifecycle": event_name,
                "repositories_transitioned": affected,
            },
        )
    if event_name == GITHUB_PULL_REQUEST_EVENT:
        recovery_response = await handle_tracked_recovery_pull_request(
            payload=payload,
            db=db,
            events=events,
        )
        if recovery_response is not None:
            return recovery_response
    bodies = build_git_webhook_bodies(
        payload,
        db=db,
        event_name=event_name,
    )
    if not bodies:
        return JSONResponse(
            status_code=202,
            content={"accepted": True, "ignored": True, "reason": "no deployable git change"},
        )
    first = None
    for body in bodies:
        with event_workspace(body.workspace_id):
            accepted = await events.accept_body(body)
        if first is None:
            first = accepted
    return accepted_event_response(first)

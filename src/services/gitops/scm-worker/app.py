"""scm-worker - safe_pr.ready_for_creation -> GitHub PR -> safe_pr.created."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from github_provider import GithubScmProvider, request_base_branch, request_repo

from domains.gitops.events import GitChangedBody, GitWebhookReceivedBody
from domains.gitops.source_observation import observe_git_source
from domains.providers.catalog import ProviderCategory, require_available_provider
from domains.scm.events import (
    SafePrCreatedBody,
    SafePrFailedBody,
    SafePrReadyForCreationBody,
    SafePrRequestedBody,
)
from domains.scm.pipeline import normalize_safe_pr_request
from domains.scm.policy import (
    MESSAGE_PROVIDER_ERROR,
    MESSAGE_PROVIDER_MISMATCH,
    REASON_PROVIDER_ERROR,
    REASON_PROVIDER_MISMATCH,
    STAGE_SCM,
    DefaultSafePrPreflightPolicy,
    SafePrPolicyResult,
    safe_pr_failed_body,
)
from packages.config.settings import env
from packages.contracts.event_bus.bodies import EventBody
from packages.contracts.scm.provider import ScmProvider, ScmPullRequestResult
from packages.contracts.stores import PullRequestStore
from packages.runtime.app import App, EventContext
from packages.runtime.outbound import deliver

app = App("scm-worker")

SCM_PROVIDER_ENV = "SCM_PROVIDER"
DEFAULT_SCM_PROVIDER = "github"
SCM_CREATE_PR_DEADLINE_SECONDS_ENV = "SCM_CREATE_PR_DEADLINE_SECONDS"
DEFAULT_SCM_CREATE_PR_DEADLINE_SECONDS = "20"
PR_MODE = "github_rest"
UNSUPPORTED_SCM_PROVIDER_MESSAGE = f"{SCM_PROVIDER_ENV} 값이 provider registry 지원 목록에 없음"


def build_scm_provider(name: str | None = None) -> ScmProvider:
    """SCM_PROVIDER env 로 provider 를 선택함."""

    provider = (name or env(SCM_PROVIDER_ENV, DEFAULT_SCM_PROVIDER)).strip().lower()
    try:
        definition = require_available_provider(ProviderCategory.SOURCE, provider)
    except ValueError as exc:
        raise RuntimeError(f"{UNSUPPORTED_SCM_PROVIDER_MESSAGE} (got: {provider})") from exc
    if definition.key == DEFAULT_SCM_PROVIDER:
        return GithubScmProvider()
    raise RuntimeError(
        f"{SCM_PROVIDER_ENV} adapter binding is missing for provider: {definition.key}"
    )


ACTIVE_SCM_PROVIDER = (
    env(SCM_PROVIDER_ENV, DEFAULT_SCM_PROVIDER).strip().lower() or DEFAULT_SCM_PROVIDER
)
SCM_PROVIDER: ScmProvider = build_scm_provider()
PREFLIGHT_POLICY = DefaultSafePrPreflightPolicy()


async def create_safe_pr(
    evt: SafePrRequestedBody,
    ctx: EventContext[PullRequestStore],
) -> ScmPullRequestResult:
    if evt.provider != ACTIVE_SCM_PROVIDER:
        raise RuntimeError(
            f"safe_pr provider mismatch: event={evt.provider}, worker={ACTIVE_SCM_PROVIDER}"
        )
    return await SCM_PROVIDER.create_pull_request(evt, ctx)


def create_pr_deadline_seconds() -> float:
    """SCM 외부 호출 전체 데드라인 — worker handler timeout 보다 짧게 유지함."""
    return float(env(SCM_CREATE_PR_DEADLINE_SECONDS_ENV, DEFAULT_SCM_CREATE_PR_DEADLINE_SECONDS))


def preflight_failure_body(request: SafePrRequestedBody) -> SafePrFailedBody | None:
    if request.provider != ACTIVE_SCM_PROVIDER:
        return safe_pr_failed_body(
            request,
            SafePrPolicyResult.reject(
                reason_code=REASON_PROVIDER_MISMATCH,
                message=MESSAGE_PROVIDER_MISMATCH,
                details={
                    "event_provider": request.provider,
                    "worker_provider": ACTIVE_SCM_PROVIDER,
                },
            ),
            stage=STAGE_SCM,
        )
    preflight = PREFLIGHT_POLICY.evaluate(request)
    if not preflight.allowed:
        return safe_pr_failed_body(request, preflight, stage=STAGE_SCM)
    return None


def provider_failure_body(request: SafePrRequestedBody, exc: Exception) -> SafePrFailedBody:
    return safe_pr_failed_body(
        request,
        SafePrPolicyResult.reject(
            reason_code=REASON_PROVIDER_ERROR,
            message=MESSAGE_PROVIDER_ERROR,
            details=provider_error_details(exc),
        ),
        stage=STAGE_SCM,
    )


def provider_error_details(exc: Exception) -> dict[str, object]:
    details: dict[str, object] = {"exception_type": type(exc).__name__}
    if isinstance(exc, ValueError | RuntimeError):
        details["error"] = str(exc)
    return details


def created_repo_ref(request: SafePrRequestedBody) -> str:
    try:
        return request_repo(request)
    except (RuntimeError, ValueError):
        return request.repo_ref


def created_base_branch(request: SafePrRequestedBody) -> str:
    try:
        return request_base_branch(request)
    except (RuntimeError, ValueError):
        return request.base_branch


@app.on(GitWebhookReceivedBody)
async def on_git_webhook_received(
    evt: GitWebhookReceivedBody, ctx: EventContext[PullRequestStore]
) -> GitChangedBody | None:
    return await observe_git_source(evt, ctx)


@app.on(SafePrReadyForCreationBody)
async def on_safe_pr_ready_for_creation(
    evt: SafePrReadyForCreationBody, ctx: EventContext[PullRequestStore]
) -> AsyncIterator[EventBody]:
    request = normalize_safe_pr_request(evt.request)

    preflight_failed = preflight_failure_body(request)
    if preflight_failed is not None:
        yield preflight_failed
        return

    async def create_pr() -> ScmPullRequestResult:
        return await asyncio.wait_for(create_safe_pr(request, ctx), create_pr_deadline_seconds())

    def created_body(result: ScmPullRequestResult) -> SafePrCreatedBody:
        return SafePrCreatedBody(
            pr_url=result.url,
            provider=request.provider,
            mode=PR_MODE,
            workspace_id=request.workspace_id,
            repository_id=request.repository_id,
            binding_id=request.binding_id,
            application_id=request.application_id,
            workflow_run_id=request.workflow_run_id,
            environment=request.environment,
            manifest_path=request.manifest_path,
            repo_ref=created_repo_ref(request),
            base_branch=created_base_branch(request),
            commit_sha=request.commit_sha,
            patch_sha256=request.patch_sha256,
            pr_number=result.number,
            pr_node_id=result.node_id,
            head_ref=result.head_ref,
            head_sha=result.head_sha,
        )

    async for out in deliver(
        call=create_pr,
        ok=created_body,
        fail=lambda exc: provider_failure_body(request, exc),
    ):
        yield out


if __name__ == "__main__":
    app.run()

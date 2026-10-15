"""Read one bound Deployment from an immutable GitHub commit."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

from domains.gitops.events import GitChangedBody, GitWebhookReceivedBody
from domains.gitops.repository import repository_credential_scope
from domains.gitops.repository_discovery import (
    GitHubRepositoryClient,
    RepositoryDiscoveryError,
    github_token,
    normalize_github_repo_ref,
)
from domains.gitops.source_patch import (
    RAW_YAML,
    ManifestSourcePatchError,
    parse_single_manifest,
    safe_manifest_path,
)
from domains.scm.github_app import GithubAppNotConfigured
from domains.scm.github_app_credentials import (
    is_app_installation_ref,
    parse_app_installation_ref,
    resolve_installation_token,
)
from domains.scm.github_app_manifest import APP_CONFIG_PROVIDER, APP_CONFIG_SCOPE
from packages.runtime.app import EventContext
from packages.security.credentials import (
    CredentialEncryptionError,
    decrypt_credential,
)
from packages.security.credentials import (
    credential_ref as database_credential_ref,
)

SOURCE_OBSERVED = "observed"
SOURCE_INVALID = "invalid"
SOURCE_UNAVAILABLE = "unavailable"
COMMIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")


class _CredentialView:
    def __init__(self, stored: Mapping[str, Any] | None) -> None:
        self._stored = stored

    def get_workspace_credential(
        self, workspace_id: str, provider: str, scope: str
    ) -> Mapping[str, Any] | None:
        if provider == APP_CONFIG_PROVIDER and scope == APP_CONFIG_SCOPE:
            return self._stored
        return None


# cost: time O(n + b), heap O(n + b), stack O(1), io 3-5
# vars: n = binding 조회 결과 수, b = GitHub 파일 바이트 수
# basis: estimate
async def observe_git_source(
    event: GitWebhookReceivedBody,
    context: EventContext[Any],
) -> GitChangedBody | None:
    target = await _find_target(event, context)
    base = {
        "workspace_id": event.workspace_id,
        "binding_id": event.binding_id,
        "repository_id": event.repository_id,
        "application_id": event.application_id,
        "cluster_id": event.cluster_id,
        "namespace": str(target["namespace"]),
        "deployment_name": str(target["app_name"]),
        "repo_ref": event.repo_ref,
        "commit_sha": event.commit_sha,
        "manifest_path": event.manifest_path,
        "source_type": str(target.get("source_type") or ""),
    }
    existing = await context.db.get_git_source_observation(
        event.workspace_id, event.binding_id, event.commit_sha, event.manifest_path
    )
    if existing is not None and existing.get("status") == SOURCE_OBSERVED:
        if any(existing.get(field) != base[field] for field in base):
            raise ValueError("observed Git source binding changed")
        return _changed_body(event, existing)

    if not COMMIT_SHA_PATTERN.fullmatch(event.commit_sha):
        await _record(context, base, SOURCE_INVALID, "invalid_commit_sha")
        return None
    if base["source_type"] != RAW_YAML or not safe_manifest_path(event.manifest_path):
        await _record(context, base, SOURCE_INVALID, "unsupported_source")
        return None

    try:
        token = await _repository_token(target, context)
        client = GitHubRepositoryClient(token=token)
        content = await client.content(
            normalize_github_repo_ref(event.repo_ref),
            event.commit_sha,
            event.manifest_path,
        )
        image, replicas = _deployment_values(content, target)
    except (GithubAppNotConfigured, CredentialEncryptionError) as exc:
        await _record(context, base, SOURCE_UNAVAILABLE, type(exc).__name__)
        return None
    except RepositoryDiscoveryError as exc:
        if exc.status_code >= 500 or exc.status_code == 429:
            raise
        await _record(context, base, SOURCE_UNAVAILABLE, f"github_{exc.status_code}")
        return None
    except (ManifestSourcePatchError, UnicodeDecodeError, ValueError) as exc:
        await _record(context, base, SOURCE_INVALID, type(exc).__name__)
        return None

    saved = await context.db.record_git_source_observation(
        {
            **base,
            "status": SOURCE_OBSERVED,
            "image": image,
            "replicas": replicas,
            "source_sha256": hashlib.sha256(content).hexdigest(),
        }
    )
    return _changed_body(event, saved)


# cost: time O(n), heap O(n), stack O(1), io 1
# vars: n = binding 조회 결과 수
# basis: estimate
async def _find_target(
    event: GitWebhookReceivedBody,
    context: EventContext[Any],
) -> Mapping[str, Any]:
    targets = await context.db.list_active_github_poll_targets(
        event.workspace_id, binding_id=event.binding_id
    )
    fields = (
        "workspace_id",
        "repository_id",
        "branch",
        "watch_target_id",
        "binding_id",
        "application_id",
        "environment",
        "cluster_id",
        "manifest_path",
    )
    matches = [
        target
        for target in targets
        if str(target.get("repo_ref") or "").casefold() == event.repo_ref.casefold()
        and all(str(target.get(field) or "") == str(getattr(event, field)) for field in fields)
    ]
    if len(matches) != 1:
        raise ValueError("git source binding is unavailable or ambiguous")
    return matches[0]


# cost: time O(1), heap O(1), stack O(1), io 1-2
# basis: estimate
async def _repository_token(
    target: Mapping[str, Any],
    context: EventContext[Any],
) -> str:
    workspace_id = str(target["workspace_id"])
    credential_ref = str(target.get("credential_ref") or "")
    if is_app_installation_ref(credential_ref):
        stored = await context.db.get_workspace_credential(
            workspace_id, APP_CONFIG_PROVIDER, APP_CONFIG_SCOPE
        )
        token = await resolve_installation_token(
            _CredentialView(stored),
            workspace_id,
            parse_app_installation_ref(credential_ref),
        )
        if not token:
            raise GithubAppNotConfigured("GitHub App installation token is unavailable")
        return token
    scope = repository_credential_scope(str(target["repository_id"]))
    if credential_ref and credential_ref != database_credential_ref("github", scope):
        raise CredentialEncryptionError("repository credential reference does not match")
    stored = await context.db.get_workspace_credential(workspace_id, "github", scope)
    if stored is not None:
        if any(
            stored.get(field) != value
            for field, value in (
                ("workspace_id", workspace_id),
                ("provider", "github"),
                ("scope", scope),
            )
        ):
            raise CredentialEncryptionError("repository credential scope does not match")
        return decrypt_credential(str(stored["encrypted_value"]))
    if credential_ref:
        raise CredentialEncryptionError("repository credential is unavailable")
    return github_token()


def _deployment_values(content: bytes, target: Mapping[str, Any]) -> tuple[str, int]:
    document = parse_single_manifest(content.decode("utf-8"), RAW_YAML)
    metadata = document.get("metadata")
    spec = document.get("spec")
    if document.get("apiVersion") != "apps/v1" or document.get("kind") != "Deployment":
        raise ManifestSourcePatchError("source is not an apps/v1 Deployment")
    if not isinstance(metadata, Mapping) or not isinstance(spec, Mapping):
        raise ManifestSourcePatchError("Deployment metadata or spec is missing")
    namespace = metadata.get("namespace", "default")
    if metadata.get("name") != target.get("app_name") or namespace != target.get("namespace"):
        raise ManifestSourcePatchError("Deployment binding does not match source")
    template = spec.get("template")
    pod_spec = template.get("spec") if isinstance(template, Mapping) else None
    containers = pod_spec.get("containers") if isinstance(pod_spec, Mapping) else None
    init_containers = pod_spec.get("initContainers", []) if isinstance(pod_spec, Mapping) else []
    if not isinstance(containers, list) or len(containers) != 1 or init_containers:
        raise ManifestSourcePatchError("source must have one container and no init container")
    container = containers[0]
    image = container.get("image") if isinstance(container, Mapping) else None
    replicas = spec.get("replicas", 1)
    if not isinstance(image, str) or not image.strip():
        raise ManifestSourcePatchError("Deployment image is missing")
    if isinstance(replicas, bool) or not isinstance(replicas, int) or replicas < 0:
        raise ManifestSourcePatchError("Deployment replicas are invalid")
    return image, replicas


# cost: time O(1), heap O(1), stack O(1), io 1
# basis: estimate
async def _record(
    context: EventContext[Any], base: dict[str, Any], status: str, reason: str
) -> None:
    await context.db.record_git_source_observation({**base, "status": status, "reason": reason})


def _changed_body(
    event: GitWebhookReceivedBody,
    observation: Mapping[str, Any],
) -> GitChangedBody:
    image = observation.get("image")
    replicas = observation.get("replicas")
    if not isinstance(image, str) or not image or not isinstance(replicas, int):
        raise ValueError("observed Git source image or replicas are missing")
    return GitChangedBody(
        commit_sha=event.commit_sha,
        image=image,
        replicas=replicas,
        workspace_id=event.workspace_id,
        repository_id=event.repository_id,
        repo_ref=event.repo_ref,
        branch=event.branch,
        watch_target_id=event.watch_target_id,
        binding_id=event.binding_id,
        application_id=event.application_id,
        workflow_run_id=event.workflow_run_id,
        environment=event.environment,
        cluster_id=event.cluster_id,
        manifest_path=event.manifest_path,
        source_type=event.source_type,
    )

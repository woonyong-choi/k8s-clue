"""Repository discovery helpers for the repo registration UX."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import posixpath
import re
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol
from urllib.parse import quote, urlparse

import httpx
import yaml

from packages.config.settings import env
from packages.contracts.gateway.requests import (
    RepositoryManifestValidationRequest,
    RepositoryProbeRequest,
)
from packages.contracts.gateway.responses import (
    RepoManifestFile,
    RepoManifestFileListResponse,
    RepositoryBranchItem,
    RepositoryBranchListResponse,
    RepositoryManifestCandidate,
    RepositoryManifestCandidateListResponse,
    RepositoryManifestResource,
    RepositoryManifestValidationResponse,
    RepositoryProbeResponse,
)
from packages.contracts.gitops import (
    DEFAULT_GITHUB_API_BASE,
    DEFAULT_REPO_BRANCH,
    GITHUB_API_BASE_ENV,
    GITHUB_TOKEN_ENV,
    GITHUB_TOKEN_REF_ENV,
)
from packages.contracts.security import SecretRef
from packages.security import SecretNotFound, build_token_vault

REPO_REF_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")
GIT_SSH_PATTERN = re.compile(r"^git@[^:]+:(?P<repo>[^/]+/[^/]+?)(?:\.git)?$")
GITHUB_HOST = "github.com"
KUSTOMIZATION_FILES = {"kustomization.yaml", "kustomization.yml", "Kustomization"}
KUSTOMIZATION_FILE_ORDER = ("kustomization.yaml", "kustomization.yml", "Kustomization")
HELM_CHART_FILE = "Chart.yaml"
MANIFEST_EXTENSIONS = {".yaml", ".yml", ".json"}
MAX_BRANCHES = 100
MAX_TREE_ITEMS = 5000
MAX_CANDIDATES = 150
MAX_MANIFEST_BYTES = 1_048_576
MANIFEST_SCAN_CONCURRENCY_ENV = "GITHUB_MANIFEST_SCAN_CONCURRENCY"
MANIFEST_SCAN_TIMEOUT_SECONDS_ENV = "GITHUB_MANIFEST_SCAN_TIMEOUT_SECONDS"
MANIFEST_SCAN_CONCURRENCY = max(
    1,
    min(32, int(env(MANIFEST_SCAN_CONCURRENCY_ENV, "8"))),
)
MANIFEST_SCAN_TIMEOUT_SECONDS = max(
    1.0,
    min(60.0, float(env(MANIFEST_SCAN_TIMEOUT_SECONDS_ENV, "20"))),
)

# 루트 후보 랭킹 신호 — 사용자가 고를 법한 진입점을 상위로 올려 자동 선택·추천한다.
# 우선순위: source_type(kustomize>helm>기타) → 표준 경로 → 파일명 신호 → 얕은 깊이.
_STANDARD_MANIFEST_DIR_SIGNALS = (
    "/overlays/",
    "/deploy/",
    "/deployment/",
    "/deployments/",
    "/k8s/",
    "/kubernetes/",
    "/manifests/",
    "/kustomize/",
    "/base/",
    "/chart/",
    "/charts/",
)
_RECOMMENDED_MANIFEST_FILENAMES = (
    "kustomization.yaml",
    "kustomization.yml",
    "chart.yaml",
    "deployment.yaml",
    "app.yaml",
    "release.yaml",
)


def _manifest_candidate_rank(
    candidate: RepositoryManifestCandidate,
) -> tuple[int, int, int, int, str]:
    """작을수록 좋은(상위) 후보. 진입점·표준 경로·파일명·얕은 깊이를 선호한다."""
    path = candidate.path.lower()
    filename = path.rsplit("/", 1)[-1]
    type_rank = {"kustomize": 0, "helm": 1}.get(candidate.source_type, 2)
    dir_rank = 0 if any(signal in f"/{path}" for signal in _STANDARD_MANIFEST_DIR_SIGNALS) else 1
    name_rank = 0 if filename in _RECOMMENDED_MANIFEST_FILENAMES else 1
    depth = path.count("/")
    return (type_rank, dir_rank, name_rank, depth, path)


def _manifest_candidate_reason(candidate: RepositoryManifestCandidate, *, recommended: bool) -> str:
    """이 후보가 어떤 성격인지(왜 선택할 만한지) 한 줄로 설명한다."""
    if candidate.source_type == "kustomize":
        hint = "Kustomize 루트 — 참조 리소스를 자동으로 함께 추적합니다"
    elif candidate.source_type == "helm":
        hint = "Helm 차트 루트 — 차트 전체가 렌더됩니다"
    else:
        hint = "단일 매니페스트 파일 — 이 파일만 추적합니다"
    existing = candidate.reason.strip()
    text = f"{existing} · {hint}" if existing and existing != hint else hint
    return f"추천 · {text}" if recommended else text


def rank_manifest_candidates(
    candidates: Sequence[RepositoryManifestCandidate],
) -> list[RepositoryManifestCandidate]:
    """후보를 추천 순으로 정렬하고, 각 후보에 선택 근거(reason)를 채워 돌려준다.

    최상위(index 0)가 프론트에서 자동 선택되므로, 진입점(kustomization/Chart)이
    있으면 그게 기본 제안이 된다.
    """
    ordered = sorted(candidates, key=_manifest_candidate_rank)
    return [
        candidate.model_copy(
            update={"reason": _manifest_candidate_reason(candidate, recommended=index == 0)}
        )
        for index, candidate in enumerate(ordered)
    ]


MAX_RENDER_SOURCE_FILES = 500
MAX_RENDER_SOURCE_BYTES = 5 * 1_048_576
MAX_RENDER_ERROR_LENGTH = 2000
DEFAULT_TIMEOUT_SECONDS = 5.0
DEFAULT_RENDER_TIMEOUT_SECONDS = 5.0
GIT_MANIFEST_COMMAND_TIMEOUT_SECONDS_ENV = "GIT_MANIFEST_COMMAND_TIMEOUT_SECONDS"
GITOPS_KUBECTL_BIN_ENV = "GITOPS_KUBECTL_BIN"
GITOPS_HELM_BIN_ENV = "GITOPS_HELM_BIN"
GITOPS_HELM_RELEASE_NAME_ENV = "GITOPS_HELM_RELEASE_NAME"
GITOPS_HELM_NAMESPACE_ENV = "GITOPS_HELM_NAMESPACE"
STATIC_PARSE_WARNING = "static manifest parse only; Kubernetes server dry-run is not executed"
RENDER_PARSE_WARNING = "rendered manifest parse only; Kubernetes server dry-run is not executed"

JsonMap = dict[str, Any]
_AMBIENT_GITHUB_TOKEN = object()

_LOGGER = logging.getLogger(__name__)


class RepositoryDiscoveryError(Exception):
    def __init__(
        self,
        status_code: int,
        detail: str,
        *,
        observability: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        # 서버 로그 전용, secret-free. 응답 스키마에는 노출하지 않는다.
        self.observability = dict(observability) if observability else None


class ManifestRenderValidationError(Exception):
    """The selected render source could not be exported or rendered for validation."""


class RepositoryManifestBatchError(RepositoryDiscoveryError):
    """One immutable validation batch failed without exposing partial results."""

    def __init__(self, source_errors: Mapping[str, str]) -> None:
        self.source_errors = dict(source_errors)
        detail = "; ".join(
            f"{source}: {error}" for source, error in sorted(self.source_errors.items())
        )
        super().__init__(422, detail or "repository manifest batch validation failed")


@dataclass(frozen=True)
class RepositoryManifestValidationBatch:
    repo_ref: str
    branch: str
    revision: str
    validations: tuple[RepositoryManifestValidationResponse, ...]


class GitHubClient(Protocol):
    async def repository(self, repo_ref: str) -> JsonMap: ...

    async def branches(self, repo_ref: str) -> list[JsonMap]: ...

    async def tree(self, repo_ref: str, branch: str) -> tuple[list[JsonMap], list[str]]: ...

    async def tree_at_revision(
        self, repo_ref: str, revision: str
    ) -> tuple[list[JsonMap], list[str]]: ...

    async def content(self, repo_ref: str, branch: str, path: str) -> bytes: ...

    async def branch_sha(self, repo_ref: str, branch: str) -> str: ...


class RenderCommandExecutor(Protocol):
    def __call__(
        self, command: Sequence[str], timeout_seconds: float
    ) -> subprocess.CompletedProcess[str]: ...


class GitHubRepositoryClient:
    def __init__(
        self,
        *,
        api_base: str | None = None,
        token: str | None | object = _AMBIENT_GITHUB_TOKEN,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.api_base = (api_base or env(GITHUB_API_BASE_ENV, DEFAULT_GITHUB_API_BASE)).rstrip("/")
        self.token = github_token() if token is _AMBIENT_GITHUB_TOKEN else token
        self.timeout = timeout
        self.transport = transport

    async def repository(self, repo_ref: str) -> JsonMap:
        data = await self._get(f"/repos/{quote(repo_ref, safe='/')}")
        if not isinstance(data, Mapping):
            raise _invalid_shape_error("repository", "github repository response was invalid")
        return dict(data)

    async def branches(self, repo_ref: str) -> list[JsonMap]:
        data = await self._get(
            f"/repos/{quote(repo_ref, safe='/')}/branches",
            params={"per_page": str(MAX_BRANCHES)},
        )
        if not isinstance(data, list):
            raise _invalid_shape_error("branches", "github branch response was invalid")
        return [dict(item) for item in data if isinstance(item, Mapping)]

    async def tree(self, repo_ref: str, branch: str) -> tuple[list[JsonMap], list[str]]:
        branch_data = await self._get(
            f"/repos/{quote(repo_ref, safe='/')}/branches/{quote(branch, safe='')}"
        )
        if not isinstance(branch_data, Mapping):
            raise _invalid_shape_error("branch", "github branch response was invalid")
        commit = branch_data.get("commit")
        commit_map = commit if isinstance(commit, Mapping) else {}
        nested_commit = commit_map.get("commit")
        nested_commit_map = nested_commit if isinstance(nested_commit, Mapping) else {}
        tree = nested_commit_map.get("tree")
        tree_map = tree if isinstance(tree, Mapping) else {}
        tree_sha = str(tree_map.get("sha") or "").strip()
        if not tree_sha:
            raise _invalid_shape_error("branch_tree", "github branch tree response was invalid")
        return await self._tree(repo_ref, tree_sha)

    async def tree_at_revision(
        self,
        repo_ref: str,
        revision: str,
    ) -> tuple[list[JsonMap], list[str]]:
        if not re.fullmatch(r"[0-9a-f]{40,64}", revision):
            raise RepositoryDiscoveryError(422, "repository revision is invalid")
        commit_data = await self._get(
            f"/repos/{quote(repo_ref, safe='/')}/git/commits/{quote(revision, safe='')}"
        )
        if not isinstance(commit_data, Mapping):
            raise _invalid_shape_error("commit", "github commit response was invalid")
        tree = commit_data.get("tree")
        tree_sha = str(tree.get("sha") or "") if isinstance(tree, Mapping) else ""
        if not tree_sha:
            raise _invalid_shape_error("commit_tree", "github commit tree response was invalid")
        return await self._tree(repo_ref, tree_sha)

    async def _tree(self, repo_ref: str, tree_sha: str) -> tuple[list[JsonMap], list[str]]:
        tree_data = await self._get(
            f"/repos/{quote(repo_ref, safe='/')}/git/trees/{quote(tree_sha, safe='')}",
            params={"recursive": "1"},
        )
        if not isinstance(tree_data, Mapping):
            raise _invalid_shape_error("tree", "github tree response was invalid")
        raw_tree = tree_data.get("tree")
        if not isinstance(raw_tree, list):
            raise _invalid_shape_error("tree", "github tree response was invalid")
        warnings = []
        if bool(tree_data.get("truncated")):
            warnings.append(
                "repository tree was truncated by GitHub; candidate list may be incomplete"
            )
        if len(raw_tree) > MAX_TREE_ITEMS:
            warnings.append("repository tree is large; scanned the first bounded set of paths")
        return [
            dict(item) for item in raw_tree[:MAX_TREE_ITEMS] if isinstance(item, Mapping)
        ], warnings

    async def content(self, repo_ref: str, branch: str, path: str) -> bytes:
        data = await self._get(
            f"/repos/{quote(repo_ref, safe='/')}/contents/{quote(path, safe='/')}",
            params={"ref": branch},
        )
        if not isinstance(data, Mapping):
            raise RepositoryDiscoveryError(422, "selected manifest path is not a file")
        if str(data.get("type", "")) != "file":
            raise RepositoryDiscoveryError(422, "selected manifest path is not a file")
        size = data.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise _invalid_shape_error("content", "github content size was invalid")
        if size > MAX_MANIFEST_BYTES:
            raise RepositoryDiscoveryError(422, "selected manifest exceeds the scan size limit")
        encoding = str(data.get("encoding") or "")
        raw_content = data.get("content")
        if encoding != "base64" or not isinstance(raw_content, str):
            raise _invalid_shape_error("content", "github content response was invalid")
        try:
            decoded = base64.b64decode("".join(raw_content.split()), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise _invalid_shape_error("content", "github content response was invalid") from exc
        if len(decoded) != size:
            raise _invalid_shape_error("content", "github content size did not match")
        return decoded

    async def branch_sha(self, repo_ref: str, branch: str) -> str:
        data = await self._get(
            f"/repos/{quote(repo_ref, safe='/')}/git/ref/heads/{quote(branch, safe='')}"
        )
        if not isinstance(data, Mapping):
            raise _invalid_shape_error("branch_ref", "github branch response was invalid")
        target = data.get("object")
        sha = str(target.get("sha") or "") if isinstance(target, Mapping) else ""
        if not re.fullmatch(r"[0-9a-f]{40,64}", sha):
            raise _invalid_shape_error("branch_ref", "github branch response was invalid")
        return sha

    async def _get(self, path: str, params: Mapping[str, str] | None = None) -> Any:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        started = time.monotonic()
        async with httpx.AsyncClient(
            base_url=self.api_base,
            timeout=self.timeout,
            transport=self.transport,
        ) as client:
            try:
                response = await client.get(path, params=params, headers=headers)
            except httpx.RequestError as exc:
                observability = {
                    "error_class": _request_error_class(exc),
                    "exception_type": type(exc).__name__,
                    "elapsed_ms": round((time.monotonic() - started) * 1000),
                }
                _log_origin_failure(path, observability)
                raise RepositoryDiscoveryError(
                    502, "github api request failed", observability=observability
                ) from exc
        elapsed_ms = round((time.monotonic() - started) * 1000)
        if response.status_code >= 400:
            error = github_http_error(response.status_code, response.headers)
            if error.status_code == 502:
                _log_origin_failure(path, {**(error.observability or {}), "elapsed_ms": elapsed_ms})
            raise error
        try:
            return response.json()
        except ValueError as exc:
            observability = {
                "error_class": "non_json",
                "elapsed_ms": elapsed_ms,
                "upstream_status": response.status_code,
                "github_request_id": _github_request_id(response.headers),
            }
            _log_origin_failure(path, observability)
            raise RepositoryDiscoveryError(
                502, "github api response was not json", observability=observability
            ) from exc


class ImmutableRepositorySnapshotClient:
    """Revision-pinned tree and coalesced content reads for one validation batch."""

    def __init__(
        self,
        client: GitHubClient,
        *,
        repo_ref: str,
        branch: str,
        revision: str,
        tree: Sequence[Mapping[str, Any]],
        warnings: Sequence[str],
    ) -> None:
        self._client = client
        self._repo_ref = repo_ref
        self._branch = branch
        self._revision = revision
        self._tree = [dict(item) for item in tree]
        self._warnings = list(warnings)
        self._content_tasks: dict[str, asyncio.Task[bytes]] = {}
        self._content_lock = asyncio.Lock()

    def _require_scope(self, repo_ref: str, revision: str) -> None:
        if repo_ref != self._repo_ref or revision not in {self._branch, self._revision}:
            raise RepositoryDiscoveryError(422, "repository snapshot scope mismatch")

    async def repository(self, repo_ref: str) -> JsonMap:
        self._require_scope(repo_ref, self._branch)
        return await self._client.repository(repo_ref)

    async def branches(self, repo_ref: str) -> list[JsonMap]:
        self._require_scope(repo_ref, self._branch)
        return await self._client.branches(repo_ref)

    async def tree(self, repo_ref: str, branch: str) -> tuple[list[JsonMap], list[str]]:
        self._require_scope(repo_ref, branch)
        return [dict(item) for item in self._tree], list(self._warnings)

    async def tree_at_revision(
        self,
        repo_ref: str,
        revision: str,
    ) -> tuple[list[JsonMap], list[str]]:
        self._require_scope(repo_ref, revision)
        return [dict(item) for item in self._tree], list(self._warnings)

    async def content(self, repo_ref: str, branch: str, path: str) -> bytes:
        self._require_scope(repo_ref, branch)
        normalized_path = normalize_manifest_path(path)
        async with self._content_lock:
            task = self._content_tasks.get(normalized_path)
            if task is None:
                task = asyncio.create_task(
                    self._client.content(self._repo_ref, self._revision, normalized_path)
                )
                self._content_tasks[normalized_path] = task
        return await asyncio.shield(task)

    async def branch_sha(self, repo_ref: str, branch: str) -> str:
        self._require_scope(repo_ref, branch)
        return self._revision


class RepositoryDiscoveryService:
    def __init__(
        self,
        client: GitHubClient | None = None,
        *,
        render_executor: RenderCommandExecutor | None = None,
    ) -> None:
        self.client = client or GitHubRepositoryClient()
        self.render_executor = render_executor or default_render_command_executor

    async def probe_repository(self, payload: RepositoryProbeRequest) -> RepositoryProbeResponse:
        try:
            repo_ref = normalize_repo_ref(payload.repo_ref)
        except ValueError as exc:
            return RepositoryProbeResponse(
                repo_ref=payload.repo_ref,
                normalized_repo_ref="",
                valid=False,
                reachable=False,
                errors=[str(exc)],
            )
        try:
            metadata = await self.client.repository(repo_ref)
        except RepositoryDiscoveryError as exc:
            return RepositoryProbeResponse(
                repo_ref=payload.repo_ref,
                normalized_repo_ref=repo_ref,
                valid=True,
                reachable=False,
                errors=[exc.detail],
            )
        return RepositoryProbeResponse(
            repo_ref=payload.repo_ref,
            normalized_repo_ref=repo_ref,
            valid=True,
            reachable=True,
            default_branch=str(metadata.get("default_branch") or DEFAULT_REPO_BRANCH),
            private=bool(metadata.get("private")) if "private" in metadata else None,
            html_url=str(metadata.get("html_url") or "") or None,
            warnings=repository_metadata_warnings(metadata),
        )

    async def list_branches(
        self, repo_ref: str, *, metadata: JsonMap | None = None
    ) -> RepositoryBranchListResponse:
        normalized = normalize_repo_ref(repo_ref)
        # 호출부가 직전 probe에서 이미 metadata를 얻었으면 재요청을 생략(cross-request rate 절감).
        if metadata is None:
            metadata = await self.client.repository(normalized)
        default_branch = str(metadata.get("default_branch") or DEFAULT_REPO_BRANCH)
        branches = [
            RepositoryBranchItem(
                name=str(item.get("name") or ""),
                protected=bool(item.get("protected")),
                default=str(item.get("name") or "") == default_branch,
            )
            for item in await self.client.branches(normalized)
            if str(item.get("name") or "")
        ]
        return RepositoryBranchListResponse(
            repo_ref=normalized,
            default_branch=default_branch,
            branches=branches,
            warnings=[] if len(branches) < MAX_BRANCHES else ["showing the first 100 branches"],
        )

    async def resolve_branch_revision(self, repo_ref: str, branch: str) -> str:
        """Resolve one normalized branch through the discovery client's bounded API."""

        return await self.client.branch_sha(
            normalize_repo_ref(repo_ref),
            normalize_branch(branch),
        )

    async def list_manifest_candidates(
        self, repo_ref: str, branch: str
    ) -> RepositoryManifestCandidateListResponse:
        normalized = normalize_repo_ref(repo_ref)
        normalized_branch = normalize_branch(branch)
        tree, warnings = await self.client.tree(normalized, normalized_branch)
        candidates, classification_warnings = await content_aware_manifest_candidates(
            self.client,
            normalized,
            normalized_branch,
            tree,
        )
        warnings.extend(classification_warnings)
        if len(candidates) >= MAX_CANDIDATES:
            warnings.append("candidate list was limited; narrow the repository layout if needed")
        if not candidates:
            warnings.append("no attachable yaml, json, kustomize, or helm candidates were found")
        # 추천 순으로 정렬 + 선택 근거 채움 → 최상위가 자동 선택되고 이유가 노출된다.
        candidates = rank_manifest_candidates(candidates)
        return RepositoryManifestCandidateListResponse(
            repo_ref=normalized,
            branch=normalized_branch,
            candidates=candidates,
            warnings=warnings,
        )

    async def list_attachable_manifest_files(
        self, repo_ref: str, branch: str
    ) -> RepoManifestFileListResponse:
        normalized = normalize_github_repo_ref(repo_ref)
        normalized_branch = normalize_branch(branch)
        tree, warnings = await self.client.tree(normalized, normalized_branch)
        paths = [
            path
            for item in tree
            if str(item.get("type") or "") == "blob"
            for path in [normalize_tree_path(str(item.get("path") or ""))]
            if manifest_extension(path) in {".yaml", ".yml"}
        ][:MAX_CANDIDATES]
        semaphore = asyncio.Semaphore(MANIFEST_SCAN_CONCURRENCY)

        async def inspect(index: int, path: str) -> tuple[int, RepoManifestFile | None]:
            async with semaphore:
                try:
                    content = await self.client.content(normalized, normalized_branch, path)
                    text = content.decode("utf-8")
                    kinds = manifest_kinds(text, "raw-yaml")
                except (RepositoryDiscoveryError, UnicodeDecodeError, ValueError, yaml.YAMLError):
                    return index, None
                manifest = RepoManifestFile(path=path, kinds=kinds) if kinds else None
                return index, manifest

        tasks = [asyncio.create_task(inspect(index, path)) for index, path in enumerate(paths)]
        done, pending = await asyncio.wait(tasks, timeout=MANIFEST_SCAN_TIMEOUT_SECONDS)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
            warnings.append("매니페스트 스캔 제한시간 내 확인된 파일만 표시합니다.")
        inspected = sorted((task.result() for task in done), key=lambda item: item[0])
        manifests = [manifest for _, manifest in inspected if manifest is not None]
        if len(paths) >= MAX_CANDIDATES:
            warnings.append("manifest scan was limited; narrow the repository layout if needed")
        if not manifests:
            warnings.append("첨부 가능한 Kubernetes YAML 파일을 찾지 못했습니다.")
        return RepoManifestFileListResponse(
            repo=normalized,
            branch=normalized_branch,
            manifests=manifests,
            warnings=dedupe(warnings),
        )

    async def validate_manifest(
        self, payload: RepositoryManifestValidationRequest
    ) -> RepositoryManifestValidationResponse:
        repo_ref = normalize_repo_ref(payload.repo_ref)
        branch = normalize_branch(payload.branch)
        manifest_path = normalize_manifest_path(payload.manifest_path)
        values_path = (
            normalize_manifest_path(payload.values_path)
            if payload.values_path is not None
            else None
        )
        source_type = normalize_source_type(payload.source_type) or source_type_from_path(
            manifest_path
        )
        if values_path is not None and source_type != "helm":
            raise ValueError("values_path is valid only for Helm manifest validation")
        if source_type in {"kustomize", "helm"}:
            return await validate_render_manifest(
                self.client,
                self.render_executor,
                repo_ref,
                branch,
                manifest_path,
                source_type,
                values_path=values_path,
            )
        content = await self.client.content(repo_ref, branch, manifest_path)
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RepositoryDiscoveryError(422, "selected manifest is not valid utf-8") from exc
        return validate_manifest_text(repo_ref, branch, manifest_path, text, source_type)

    async def render_desired_objects(
        self, payload: RepositoryManifestValidationRequest
    ) -> tuple[str, list[JsonMap], list[str]]:
        """연결 프리뷰용 — 선택 매니페스트를 실제 리비전에서 렌더/조회해 full desired
        Kubernetes 오브젝트 목록과 revision 을 반환한다(식별자 축약 전).

        validate_manifest 와 동일한 소스타입 디스패치(raw 조회 / kustomize·helm 렌더)를
        써서 프리뷰가 검증·연결과 같은 산출물을 본다.
        """
        repo_ref = normalize_repo_ref(payload.repo_ref)
        branch = normalize_branch(payload.branch)
        manifest_path = normalize_manifest_path(payload.manifest_path)
        values_path = (
            normalize_manifest_path(payload.values_path)
            if payload.values_path is not None
            else None
        )
        source_type = normalize_source_type(payload.source_type) or source_type_from_path(
            manifest_path
        )
        if values_path is not None and source_type != "helm":
            raise ValueError("values_path is valid only for Helm manifest validation")
        revision = await self.resolve_branch_revision(repo_ref, branch)
        warnings: list[str] = []
        if source_type in {"kustomize", "helm"}:
            with TemporaryDirectory(prefix="repo-preview-render-") as tmp:
                checkout_root = Path(tmp) / "repo"
                checkout_root.mkdir()
                render_path, export_warnings = await export_render_source(
                    self.client,
                    repo_ref,
                    branch,
                    manifest_path,
                    source_type,
                    checkout_root,
                    values_path=values_path,
                )
                warnings.extend(export_warnings)
                render_values_path = (
                    safe_checkout_file_path(checkout_root, values_path)
                    if values_path is not None
                    else None
                )
                text = await render_source(
                    source_type,
                    render_path,
                    self.render_executor,
                    values_path=render_values_path,
                )
            parse_source = "raw-yaml"
        else:
            content = await self.client.content(repo_ref, branch, manifest_path)
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise RepositoryDiscoveryError(422, "selected manifest is not valid utf-8") from exc
            parse_source = source_type
        objects = flatten_manifest_objects(parse_manifest_documents(text, parse_source))
        return revision, objects, warnings

    async def validate_manifests_at_revision(
        self,
        payloads: Sequence[RepositoryManifestValidationRequest],
        *,
        expected_revision: str,
    ) -> RepositoryManifestValidationBatch:
        """Validate one repository batch from a single immutable revision snapshot."""

        if not payloads:
            raise ValueError("repository manifest validation batch cannot be empty")
        if not re.fullmatch(r"[0-9a-f]{40,64}", expected_revision):
            raise ValueError("expected repository revision is invalid")

        normalized: list[tuple[RepositoryManifestValidationRequest, str, str, str, str]] = []
        for payload in payloads:
            repo_ref = normalize_repo_ref(payload.repo_ref)
            branch = normalize_branch(payload.branch)
            manifest_path = normalize_manifest_path(payload.manifest_path)
            source_type = normalize_source_type(payload.source_type) or source_type_from_path(
                manifest_path
            )
            normalized.append((payload, repo_ref, branch, manifest_path, source_type))

        repo_refs = {item[1] for item in normalized}
        branches = {item[2] for item in normalized}
        if len(repo_refs) != 1 or len(branches) != 1:
            raise ValueError("repository manifest batch must share one repository and branch")
        repo_ref = repo_refs.pop()
        branch = branches.pop()

        observed_revision = await self.client.branch_sha(repo_ref, branch)
        if observed_revision != expected_revision:
            raise RepositoryDiscoveryError(
                409,
                "repository revision does not match expected revision",
            )
        tree, warnings = await tree_at_revision(self.client, repo_ref, observed_revision)
        blob_paths = {
            path
            for item in tree
            if str(item.get("type") or "") == "blob"
            for path in [normalize_tree_path(str(item.get("path") or ""))]
            if path
        }
        candidate_identities = {
            (candidate.path, candidate.source_type)
            for candidate in manifest_candidates_from_tree(tree)
        }
        missing_errors = {
            manifest_request_identity(payload, manifest_path, source_type): (
                "selected manifest is not an attachable repository candidate"
            )
            for payload, _, _, manifest_path, source_type in normalized
            if not manifest_request_is_attachable(
                manifest_path,
                source_type,
                blob_paths=blob_paths,
                candidate_identities=candidate_identities,
            )
        }

        snapshot = ImmutableRepositorySnapshotClient(
            self.client,
            repo_ref=repo_ref,
            branch=branch,
            revision=observed_revision,
            tree=tree,
            warnings=warnings,
        )
        snapshot_service = RepositoryDiscoveryService(
            snapshot,
            render_executor=self.render_executor,
        )
        outcomes = await asyncio.gather(
            *(snapshot_service.validate_manifest(item[0]) for item in normalized),
            return_exceptions=True,
        )

        confirmed_revision = await self.client.branch_sha(repo_ref, branch)
        if confirmed_revision != observed_revision:
            raise RepositoryDiscoveryError(
                409,
                "repository changed during manifest batch validation",
            )

        validations: list[RepositoryManifestValidationResponse] = []
        source_errors = dict(missing_errors)
        unexpected_error: BaseException | None = None
        for item, outcome in zip(normalized, outcomes, strict=True):
            payload, _, _, manifest_path, source_type = item
            if isinstance(outcome, RepositoryManifestValidationResponse):
                validations.append(outcome)
                continue
            if isinstance(outcome, RepositoryDiscoveryError):
                source_errors[manifest_request_identity(payload, manifest_path, source_type)] = (
                    outcome.detail
                )
                continue
            if isinstance(outcome, (ValueError, UnicodeDecodeError)):
                source_errors[manifest_request_identity(payload, manifest_path, source_type)] = str(
                    outcome
                )
                continue
            if isinstance(outcome, BaseException) and unexpected_error is None:
                unexpected_error = outcome

        if unexpected_error is not None:
            raise unexpected_error
        if source_errors:
            raise RepositoryManifestBatchError(source_errors)
        return RepositoryManifestValidationBatch(
            repo_ref=repo_ref,
            branch=branch,
            revision=observed_revision,
            validations=tuple(validations),
        )


async def tree_at_revision(
    client: GitHubClient,
    repo_ref: str,
    revision: str,
) -> tuple[list[JsonMap], list[str]]:
    resolver = getattr(client, "tree_at_revision", None)
    if not callable(resolver):
        raise RepositoryDiscoveryError(
            500,
            "repository client cannot provide a revision-pinned tree",
        )
    return await resolver(repo_ref, revision)


def manifest_request_identity(
    payload: RepositoryManifestValidationRequest,
    manifest_path: str,
    source_type: str,
) -> str:
    values = normalize_manifest_path(payload.values_path) if payload.values_path is not None else ""
    suffix = f"?values={values}" if values else ""
    return f"{source_type}:{manifest_path}{suffix}"


def manifest_request_is_attachable(
    manifest_path: str,
    source_type: str,
    *,
    blob_paths: set[str],
    candidate_identities: set[tuple[str, str]],
) -> bool:
    """Validate an explicit source contract without relying on its file name alone.

    A Kubernetes custom resource may legitimately be named ``kustomization.yaml``.
    The repository candidate list can still present the containing directory as a
    conventional Kustomize root, while an explicit raw-file request is accepted
    only when the exact repository blob and extension agree. Normal manifest
    parsing then validates the file contents before the batch can succeed.
    """

    if source_type == "raw-yaml":
        return manifest_path in blob_paths and manifest_extension(manifest_path) in {
            ".yaml",
            ".yml",
        }
    if source_type == "raw-json":
        return manifest_path in blob_paths and manifest_extension(manifest_path) == ".json"
    return (manifest_path, source_type) in candidate_identities


async def validate_render_manifest(
    client: GitHubClient,
    render_executor: RenderCommandExecutor,
    repo_ref: str,
    branch: str,
    manifest_path: str,
    source_type: str,
    *,
    values_path: str | None = None,
) -> RepositoryManifestValidationResponse:
    validation_mode = f"{source_type}-render"
    warnings: list[str] = []
    try:
        with TemporaryDirectory(prefix="repo-discovery-render-") as tmp:
            checkout_root = Path(tmp) / "repo"
            checkout_root.mkdir()
            render_path, export_warnings = await export_render_source(
                client,
                repo_ref,
                branch,
                manifest_path,
                source_type,
                checkout_root,
                values_path=values_path,
            )
            warnings.extend(export_warnings)
            render_values_path = (
                safe_checkout_file_path(checkout_root, values_path)
                if values_path is not None
                else None
            )
            rendered_text = await render_source(
                source_type,
                render_path,
                render_executor,
                values_path=render_values_path,
            )
    except ManifestRenderValidationError as exc:
        return render_invalid_validation_response(
            repo_ref,
            branch,
            manifest_path,
            validation_mode,
            str(exc),
            warnings,
        )
    return validate_manifest_text(
        repo_ref,
        branch,
        manifest_path,
        rendered_text,
        "raw-yaml",
        validation_mode=validation_mode,
        parse_warning=RENDER_PARSE_WARNING,
        parse_error_prefix="rendered manifest parse failed",
        extra_warnings=warnings,
    )


async def export_render_source(
    client: GitHubClient,
    repo_ref: str,
    branch: str,
    manifest_path: str,
    source_type: str,
    checkout_root: Path,
    *,
    values_path: str | None = None,
) -> tuple[Path, list[str]]:
    source_dir = render_source_directory(manifest_path, source_type)
    tree, warnings = await client.tree(repo_ref, branch)
    if source_type == "kustomize":
        source_contents, dependency_warnings = await collect_kustomize_source_contents(
            client,
            repo_ref,
            branch,
            source_dir,
            tree,
        )
        warnings.extend(dependency_warnings)
        for path, content in sorted(source_contents.items()):
            write_render_source_file(checkout_root, path, content)
        return checkout_root if source_dir == "." else checkout_root / source_dir, warnings

    blob_paths = {
        path
        for item in tree
        if str(item.get("type") or "") == "blob"
        for path in [normalize_tree_path(str(item.get("path") or ""))]
        if path
    }
    source_paths = {path for path in blob_paths if path_is_under_directory(path, source_dir)}
    if values_path is not None:
        if source_type != "helm":
            raise ManifestRenderValidationError(
                "values_path is valid only for Helm manifest validation"
            )
        if values_path not in blob_paths:
            raise ManifestRenderValidationError(
                f"Helm values override does not exist in repository: {values_path}"
            )
        source_paths.add(values_path)
    source_paths = sorted(source_paths)
    if not source_paths:
        raise ManifestRenderValidationError(
            f"{source_type} render source contains no files under {source_dir}"
        )
    if len(source_paths) > MAX_RENDER_SOURCE_FILES:
        raise ManifestRenderValidationError(
            f"{source_type} render source exceeds file limit "
            f"({len(source_paths)} > {MAX_RENDER_SOURCE_FILES})"
        )

    total_bytes = 0
    for path in source_paths:
        try:
            content = await client.content(repo_ref, branch, path)
        except RepositoryDiscoveryError as exc:
            raise ManifestRenderValidationError(
                f"render source content fetch failed for {path}: {exc.detail}"
            ) from exc
        total_bytes += len(content)
        if total_bytes > MAX_RENDER_SOURCE_BYTES:
            raise ManifestRenderValidationError(
                f"{source_type} render source exceeds byte limit "
                f"({total_bytes} > {MAX_RENDER_SOURCE_BYTES})"
            )
        write_render_source_file(checkout_root, path, content)

    return checkout_root if source_dir == "." else checkout_root / source_dir, warnings


async def collect_kustomize_source_contents(
    client: GitHubClient,
    repo_ref: str,
    branch: str,
    source_dir: str,
    tree: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, bytes], list[str]]:
    blob_paths = {
        path
        for item in tree
        if str(item.get("type") or "") == "blob"
        for path in [normalize_tree_path(str(item.get("path") or ""))]
        if path
    }
    selected_source_paths = {
        path for path in blob_paths if path_is_under_directory(path, source_dir)
    }
    if len(selected_source_paths) > MAX_RENDER_SOURCE_FILES:
        raise ManifestRenderValidationError(
            "kustomize render source exceeds file limit "
            f"({len(selected_source_paths)} > {MAX_RENDER_SOURCE_FILES})"
        )
    directory_paths = repository_directories(blob_paths)
    source_contents: dict[str, bytes] = {}
    total_bytes = 0

    async def fetch(path: str) -> bytes:
        nonlocal total_bytes
        cached = source_contents.get(path)
        if cached is not None:
            return cached
        if path not in blob_paths:
            raise ManifestRenderValidationError(
                f"Kustomize local reference does not exist in repository: {path}"
            )
        if len(source_contents) >= MAX_RENDER_SOURCE_FILES:
            raise ManifestRenderValidationError(
                "kustomize render source exceeds file limit "
                f"({len(source_contents) + 1} > {MAX_RENDER_SOURCE_FILES})"
            )
        try:
            content = await client.content(repo_ref, branch, path)
        except RepositoryDiscoveryError as exc:
            raise ManifestRenderValidationError(
                f"render source content fetch failed for {path}: {exc.detail}"
            ) from exc
        total_bytes += len(content)
        if total_bytes > MAX_RENDER_SOURCE_BYTES:
            raise ManifestRenderValidationError(
                "kustomize render source exceeds byte limit "
                f"({total_bytes} > {MAX_RENDER_SOURCE_BYTES})"
            )
        source_contents[path] = content
        return content

    pending_directories = [source_dir]
    visited_directories: set[str] = set()
    used_external_local_reference = False
    while pending_directories:
        directory = pending_directories.pop()
        if directory in visited_directories:
            continue
        visited_directories.add(directory)
        kustomization_path = find_kustomization_path(directory, blob_paths)
        content = await fetch(kustomization_path)
        document = parse_kustomization_document(content, kustomization_path)
        for field, raw_reference, may_be_directory in kustomize_local_references(document):
            reference = normalize_kustomize_local_reference(
                raw_reference,
                field=field,
                current_directory=directory,
            )
            if reference != directory and not path_is_under_directory(reference, source_dir):
                used_external_local_reference = True
            if may_be_directory and reference in directory_paths:
                pending_directories.append(reference)
                continue
            await fetch(reference)

    warnings = []
    if used_external_local_reference:
        warnings.append(
            "Kustomize parent or sibling references were resolved within the bounded repository tree"
        )
    return source_contents, warnings


def repository_directories(blob_paths: set[str]) -> set[str]:
    directories = {"."}
    for path in blob_paths:
        parent = parent_path(path)
        while parent != ".":
            directories.add(parent)
            parent = parent_path(parent)
    return directories


def find_kustomization_path(directory: str, blob_paths: set[str]) -> str:
    candidates = [
        name if directory == "." else f"{directory}/{name}"
        for name in KUSTOMIZATION_FILE_ORDER
        if (name if directory == "." else f"{directory}/{name}") in blob_paths
    ]
    if not candidates:
        raise ManifestRenderValidationError(
            f"Kustomize source is missing kustomization.yaml under {directory}"
        )
    if len(candidates) > 1:
        raise ManifestRenderValidationError(
            f"Kustomize source has multiple kustomization files under {directory}"
        )
    return candidates[0]


def parse_kustomization_document(content: bytes, path: str) -> Mapping[str, Any]:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ManifestRenderValidationError(f"Kustomize source is not valid utf-8: {path}") from exc
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ManifestRenderValidationError(
            f"Kustomize source parse failed for {path}: {exc}"
        ) from exc
    if not isinstance(document, Mapping):
        raise ManifestRenderValidationError(f"Kustomize source must be an object: {path}")
    return document


def kustomize_local_references(document: Mapping[str, Any]) -> list[tuple[str, str, bool]]:
    references: list[tuple[str, str, bool]] = []
    for field in ("resources", "bases", "components"):
        references.extend(
            (field, value, True) for value in string_sequence(document.get(field), field)
        )
    for field in ("crds", "configurations", "generators", "transformers"):
        references.extend(
            (field, value, False) for value in string_sequence(document.get(field), field)
        )
    references.extend(kustomize_patch_references(document))
    references.extend(kustomize_generator_references(document))

    openapi = document.get("openapi")
    if isinstance(openapi, Mapping) and isinstance(openapi.get("path"), str):
        references.append(("openapi.path", str(openapi["path"]), False))

    helm_globals = document.get("helmGlobals")
    if isinstance(helm_globals, Mapping) and isinstance(helm_globals.get("chartHome"), str):
        references.append(("helmGlobals.chartHome", str(helm_globals["chartHome"]), True))
    for chart in mapping_sequence(document.get("helmCharts"), "helmCharts"):
        repository = chart.get("repo")
        if isinstance(repository, str) and repository.strip():
            raise ManifestRenderValidationError(
                "Kustomize remote reference is not allowed: helmCharts.repo"
            )
        chart_name = chart.get("name")
        if isinstance(chart_name, str) and (
            is_remote_kustomize_reference(chart_name.strip())
            or "/" in chart_name
            or "\\" in chart_name
            or chart_name.strip() in {"", ".", ".."}
        ):
            raise ManifestRenderValidationError(
                "Kustomize remote or path-based chart name is not allowed: helmCharts.name"
            )
        values_file = chart.get("valuesFile")
        if isinstance(values_file, str) and values_file.strip():
            references.append(("helmCharts.valuesFile", values_file, False))
        references.extend(
            ("helmCharts.additionalValuesFiles", value, False)
            for value in string_sequence(
                chart.get("additionalValuesFiles"),
                "helmCharts.additionalValuesFiles",
            )
        )
    return references


def kustomize_patch_references(document: Mapping[str, Any]) -> list[tuple[str, str, bool]]:
    references: list[tuple[str, str, bool]] = []
    for patch in sequence_value(document.get("patches"), "patches"):
        if isinstance(patch, Mapping) and isinstance(patch.get("path"), str):
            references.append(("patches.path", str(patch["path"]), False))
    for patch in sequence_value(document.get("patchesJson6902"), "patchesJson6902"):
        if isinstance(patch, Mapping) and isinstance(patch.get("path"), str):
            references.append(("patchesJson6902.path", str(patch["path"]), False))
    for patch in string_sequence(document.get("patchesStrategicMerge"), "patchesStrategicMerge"):
        if "\n" not in patch:
            references.append(("patchesStrategicMerge", patch, False))
    return references


def kustomize_generator_references(
    document: Mapping[str, Any],
) -> list[tuple[str, str, bool]]:
    references: list[tuple[str, str, bool]] = []
    for generator_field in ("configMapGenerator", "secretGenerator"):
        for generator in mapping_sequence(document.get(generator_field), generator_field):
            for file_field in ("files", "envs"):
                references.extend(
                    (f"{generator_field}.{file_field}", generator_file_path(value), False)
                    for value in string_sequence(
                        generator.get(file_field), f"{generator_field}.{file_field}"
                    )
                )
            env_file = generator.get("env")
            if isinstance(env_file, str) and env_file.strip():
                references.append((f"{generator_field}.env", env_file, False))
    return references


def generator_file_path(reference: str) -> str:
    if "=" not in reference:
        return reference
    _, path = reference.split("=", 1)
    return path


def sequence_value(value: Any, field: str) -> Sequence[Any]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ManifestRenderValidationError(f"Kustomize {field} must be a list")
    return value


def string_sequence(value: Any, field: str) -> list[str]:
    values = sequence_value(value, field)
    if any(not isinstance(item, str) or not item.strip() for item in values):
        raise ManifestRenderValidationError(f"Kustomize {field} entries must be paths")
    return [str(item) for item in values]


def mapping_sequence(value: Any, field: str) -> list[Mapping[str, Any]]:
    values = sequence_value(value, field)
    if any(not isinstance(item, Mapping) for item in values):
        raise ManifestRenderValidationError(f"Kustomize {field} entries must be objects")
    return [item for item in values if isinstance(item, Mapping)]


def normalize_kustomize_local_reference(
    raw_reference: str,
    *,
    field: str,
    current_directory: str,
) -> str:
    reference = raw_reference.strip()
    if is_remote_kustomize_reference(reference):
        raise ManifestRenderValidationError(
            f"Kustomize remote reference is not allowed in {field}: {reference}"
        )
    if not reference or "\\" in reference or reference.startswith("/"):
        raise ManifestRenderValidationError(
            f"Kustomize local reference is invalid in {field}: {reference or '<empty>'}"
        )
    base = "" if current_directory == "." else current_directory
    normalized = posixpath.normpath(posixpath.join(base, reference))
    if normalized in {"", ".", ".."} or normalized.startswith("../"):
        raise ManifestRenderValidationError(
            f"Kustomize local reference escapes repository in {field}: {reference}"
        )
    safe = normalize_tree_path(normalized)
    if not safe:
        raise ManifestRenderValidationError(
            f"Kustomize local reference is invalid in {field}: {reference}"
        )
    return safe


def is_remote_kustomize_reference(reference: str) -> bool:
    lowered = reference.casefold()
    return bool(
        re.match(r"^[a-z][a-z0-9+.-]*:", lowered)
        or lowered.startswith(("git@", "//"))
        or lowered.startswith(
            (
                "github.com/",
                "gitlab.com/",
                "bitbucket.org/",
                "dev.azure.com/",
            )
        )
        or ".git//" in lowered
        or "?ref=" in lowered
    )


async def render_source(
    source_type: str,
    source_path: Path,
    render_executor: RenderCommandExecutor,
    *,
    values_path: Path | None = None,
) -> str:
    command = render_command(source_type, source_path, values_path=values_path)
    return await asyncio.to_thread(
        run_render_command,
        command,
        f"{source_type} render failed",
        render_executor,
    )


def render_source_directory(manifest_path: str, source_type: str) -> str:
    basename = manifest_path.rsplit("/", 1)[-1]
    if source_type == "helm" and basename == HELM_CHART_FILE:
        return parent_path(manifest_path)
    if source_type == "kustomize" and basename in KUSTOMIZATION_FILES:
        return parent_path(manifest_path)
    return manifest_path


def path_is_under_directory(path: str, directory: str) -> bool:
    return directory == "." or path == directory or path.startswith(f"{directory}/")


def write_render_source_file(checkout_root: Path, repository_path: str, content: bytes) -> None:
    destination = safe_checkout_file_path(checkout_root, repository_path)
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    except OSError as exc:
        message = exc.strerror or str(exc)
        raise ManifestRenderValidationError(f"failed to write render source: {message}") from exc


def safe_checkout_file_path(checkout_root: Path, repository_path: str) -> Path:
    path = normalize_tree_path(repository_path)
    if not path:
        raise ManifestRenderValidationError("render source path is invalid")
    destination = checkout_root.joinpath(*path.split("/"))
    root = checkout_root.resolve()
    resolved = destination.resolve()
    if resolved != root and root not in resolved.parents:
        raise ManifestRenderValidationError("render source path escapes checkout directory")
    return destination


def render_command(
    source_type: str,
    source_path: Path,
    *,
    values_path: Path | None = None,
) -> list[str]:
    if source_type == "kustomize":
        if values_path is not None:
            raise ManifestRenderValidationError(
                "values_path is valid only for Helm manifest validation"
            )
        if not source_path.is_dir():
            raise ManifestRenderValidationError("Kustomize rendering requires a directory source")
        if not any((source_path / name).is_file() for name in KUSTOMIZATION_FILES):
            raise ManifestRenderValidationError("Kustomize source is missing kustomization.yaml")
        return [kubectl_bin(), "kustomize", str(source_path)]
    if source_type == "helm":
        if not source_path.is_dir():
            raise ManifestRenderValidationError("Helm rendering requires a chart directory source")
        if not (source_path / HELM_CHART_FILE).is_file():
            raise ManifestRenderValidationError("Helm source is missing Chart.yaml")
        command = [
            helm_bin(),
            "template",
            helm_release_name(source_path),
            str(source_path),
            "--namespace",
            render_namespace(),
        ]
        if values_path is not None:
            if not values_path.is_file():
                raise ManifestRenderValidationError("Helm values override is not a file")
            command.extend(["--values", str(values_path)])
        return command
    raise ManifestRenderValidationError(f"unsupported render source type: {source_type}")


def default_render_command_executor(
    command: Sequence[str], timeout_seconds: float
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


def run_render_command(
    command: Sequence[str],
    error_prefix: str,
    render_executor: RenderCommandExecutor,
) -> str:
    timeout_seconds = render_timeout_seconds()
    try:
        result = render_executor(command, timeout_seconds)
    except FileNotFoundError as exc:
        raise ManifestRenderValidationError(
            f"{error_prefix}: executable not found: {command[0]}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise ManifestRenderValidationError(
            f"{error_prefix}: timed out after {timeout_seconds}s"
        ) from exc
    if result.returncode != 0:
        message = str(result.stderr or "").strip() or str(result.stdout or "").strip()
        raise ManifestRenderValidationError(
            f"{error_prefix}: {compact_render_error(message or 'renderer exited non-zero')}"
        )
    stdout = str(result.stdout or "")
    if not stdout.strip():
        raise ManifestRenderValidationError(f"{error_prefix}: renderer produced empty output")
    return stdout


def render_timeout_seconds() -> float:
    raw = env(GIT_MANIFEST_COMMAND_TIMEOUT_SECONDS_ENV, str(DEFAULT_RENDER_TIMEOUT_SECONDS))
    try:
        return max(0.1, float(raw))
    except ValueError:
        return DEFAULT_RENDER_TIMEOUT_SECONDS


def kubectl_bin() -> str:
    return env(GITOPS_KUBECTL_BIN_ENV, "kubectl")


def helm_bin() -> str:
    return env(GITOPS_HELM_BIN_ENV, "helm")


def render_namespace() -> str:
    return env(GITOPS_HELM_NAMESPACE_ENV, "sandbox")


def helm_release_name(source_path: Path) -> str:
    configured = env(GITOPS_HELM_RELEASE_NAME_ENV, "").strip()
    if configured:
        return configured
    cleaned = "".join(char.lower() if char.isalnum() else "-" for char in source_path.name)
    return (cleaned.strip("-") or "release")[:53]


def compact_render_error(message: str) -> str:
    compact = " ".join(message.split())
    try:
        token = github_token()
    except SecretNotFound:
        token = ""
    if token:
        compact = compact.replace(token, "<redacted>")
    compact = re.sub(r"(?i)(authorization:\s*bearer\s+)[^\s]+", r"\1<redacted>", compact)
    compact = re.sub(
        r"(?i)\b(token|secret|password|api[_-]?key)(\s*[:=]\s*)([^\s,;]+)",
        r"\1\2<redacted>",
        compact,
    )
    compact = re.sub(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b", "<redacted>", compact)
    compact = re.sub(r"\bsk-[A-Za-z0-9_-]{10,}\b", "<redacted>", compact)
    return compact[:MAX_RENDER_ERROR_LENGTH]


def render_invalid_validation_response(
    repo_ref: str,
    branch: str,
    manifest_path: str,
    validation_mode: str,
    error: str,
    warnings: Sequence[str] = (),
) -> RepositoryManifestValidationResponse:
    return RepositoryManifestValidationResponse(
        repo_ref=repo_ref,
        branch=branch,
        manifest_path=manifest_path,
        valid=False,
        status="invalid",
        validation_mode=validation_mode,
        warnings=dedupe(warnings),
        errors=[compact_render_error(error)],
    )


def github_token() -> str:
    token_ref = env(GITHUB_TOKEN_REF_ENV, "").strip()
    if token_ref:
        try:
            return build_token_vault().read_token(SecretRef(token_ref))
        except SecretNotFound:
            return ""
    try:
        return build_token_vault("env").read_token(SecretRef(GITHUB_TOKEN_ENV))
    except SecretNotFound:
        return ""


def _github_rate_limited(headers: Mapping[str, str] | None) -> bool:
    """GitHub 미인증 primary rate limit은 403 + x-ratelimit-remaining:0 (429 아님)."""
    if not headers:
        return False
    if str(headers.get("x-ratelimit-remaining", "")).strip() == "0":
        return True
    return bool(str(headers.get("retry-after", "")).strip())


def _github_request_id(headers: Mapping[str, str] | None) -> str | None:
    if not headers:
        return None
    value = str(headers.get("x-github-request-id", "")).strip()
    return value or None


def _request_error_class(exc: httpx.RequestError) -> str:
    if isinstance(exc, httpx.ConnectTimeout):
        return "connect_timeout"
    if isinstance(exc, httpx.ReadTimeout):
        return "read_timeout"
    if isinstance(exc, (httpx.WriteTimeout, httpx.PoolTimeout)):
        return "io_timeout"
    if isinstance(exc, httpx.ConnectError):
        return "connect_error"  # DNS/TLS/refused
    return "request_error"


def _log_origin_failure(path: str, observability: Mapping[str, Any]) -> None:
    # path=/repos/{owner}/{repo}/... (공개 식별자), observability=secret-free 필드만.
    # Authorization 헤더·토큰·query 토큰은 절대 로그하지 않는다.
    _LOGGER.warning(
        "github_origin_request_failed",
        extra={"action": "github_origin_request_failed", "path": path, **dict(observability)},
    )


def _invalid_shape_error(shape: str, detail: str) -> RepositoryDiscoveryError:
    """200이지만 기대 shape가 아닌 응답의 502를 shape별 taxonomy로 분해한다.

    HTTP status(502)와 detail 문자열은 기존과 동일하게 유지해 UI 하위호환을 보장하고,
    분류(`error_class="invalid_shape"`, `shape=<repository|branches|branch|tree|...>`)는
    secret-free observability로 서버 로그와 에러 객체에만 남긴다.
    """
    observability = {"error_class": "invalid_shape", "shape": shape}
    _log_origin_failure(f"github:shape:{shape}", observability)
    return RepositoryDiscoveryError(502, detail, observability=observability)


def github_http_error(
    status_code: int,
    headers: Mapping[str, str] | None = None,
) -> RepositoryDiscoveryError:
    if status_code == 403 and _github_rate_limited(headers):
        return RepositoryDiscoveryError(
            429,
            "github rate limit reached",
            observability={
                "error_class": "rate_limited",
                "upstream_status": 403,
                "github_request_id": _github_request_id(headers),
            },
        )
    if status_code in {401, 403}:
        return RepositoryDiscoveryError(
            403, "github authentication failed or lacks repository access"
        )
    if status_code == 404:
        return RepositoryDiscoveryError(404, "repository not found or inaccessible")
    if status_code == 422:
        return RepositoryDiscoveryError(422, "github rejected the repository discovery request")
    if status_code == 429:
        return RepositoryDiscoveryError(429, "github rate limit reached")
    return RepositoryDiscoveryError(
        502,
        "github api request failed",
        observability={
            "error_class": "upstream_status",
            "upstream_status": status_code,
            "github_request_id": _github_request_id(headers),
        },
    )


def normalize_repo_ref(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ValueError("repo_ref is required")
    repo = raw
    if raw.startswith("git@"):
        match = GIT_SSH_PATTERN.match(raw)
        repo = match.group("repo") if match else raw
    elif "://" in raw:
        parsed = urlparse(raw)
        parts = [part for part in parsed.path.strip("/").split("/") if part]
        repo = "/".join(parts[:2]) if len(parts) >= 2 else raw
    repo = repo.strip().strip("/")
    if repo.endswith(".git"):
        repo = repo[:-4]
    if not REPO_REF_PATTERN.match(repo) or any(part in {".", ".."} for part in repo.split("/")):
        raise ValueError("repo_ref must be owner/name or a GitHub repository URL")
    return repo


def normalize_github_repo_ref(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ValueError("저장소 URL을 입력해 주세요.")
    host = ""
    repo = raw
    if raw.startswith("git@"):
        match = re.match(r"^git@(?P<host>[^:]+):(?P<repo>[^/]+/[^/]+?)(?:\.git)?/?$", raw)
        if not match:
            raise ValueError("GitHub SSH 저장소 주소 형식이 올바르지 않습니다.")
        host = match.group("host").lower()
        repo = match.group("repo")
    elif "://" in raw:
        parsed = urlparse(raw)
        host = parsed.netloc.lower()
        parts = [part for part in parsed.path.strip("/").split("/") if part]
        repo = "/".join(parts[:2]) if len(parts) >= 2 else ""
    elif raw.lower().startswith(f"{GITHUB_HOST}/"):
        host = GITHUB_HOST
        parts = [part for part in raw.split("/", 1)[1].strip("/").split("/") if part]
        repo = "/".join(parts[:2]) if len(parts) >= 2 else ""
    else:
        repo = raw
    if host and host != GITHUB_HOST:
        raise RepositoryDiscoveryError(422, "unsupported_host")
    try:
        return normalize_repo_ref(repo).casefold()
    except ValueError as exc:
        raise ValueError("GitHub 저장소는 owner/repo 형식이어야 합니다.") from exc


def normalize_branch(value: str) -> str:
    branch = value.strip()
    if (
        not branch
        or len(branch) > 200
        or branch.startswith("/")
        or branch.endswith("/")
        or "\\" in branch
        or ".." in branch
        or any(ord(ch) < 32 for ch in branch)
    ):
        raise ValueError("branch must be a valid repository branch name")
    return branch


def normalize_manifest_path(value: str) -> str:
    path = value.strip().strip("/")
    if path == ".":
        return path
    if not path or "\\" in path or len(path) > 500:
        raise ValueError("manifest_path must be a relative repository path")
    parts = [part for part in path.split("/") if part]
    if not parts or any(part in {".", ".."} for part in parts):
        raise ValueError("manifest_path must be a relative repository path")
    return "/".join(parts)


def repository_metadata_warnings(metadata: Mapping[str, Any]) -> list[str]:
    warnings = []
    if bool(metadata.get("archived")):
        warnings.append("repository is archived")
    if bool(metadata.get("disabled")):
        warnings.append("repository is disabled")
    return warnings


def manifest_candidates_from_tree(
    tree: Sequence[Mapping[str, Any]],
) -> list[RepositoryManifestCandidate]:
    candidates: dict[str, RepositoryManifestCandidate] = {}
    for item in tree:
        if str(item.get("type") or "") != "blob":
            continue
        path = normalize_tree_path(str(item.get("path") or ""))
        if not path:
            continue
        basename = path.rsplit("/", 1)[-1]
        parent = parent_path(path)
        if basename == HELM_CHART_FILE:
            add_candidate(candidates, parent, "helm", "Helm chart")
            continue
        if basename in KUSTOMIZATION_FILES:
            add_candidate(candidates, parent, "kustomize", "Kustomize root")
            continue
        if manifest_extension(path) == ".json":
            add_candidate(candidates, path, "raw-json", "JSON manifest")
            continue
        if manifest_extension(path) in {".yaml", ".yml"}:
            add_candidate(candidates, path, "raw-yaml", "YAML manifest")
    return sorted(candidates.values(), key=candidate_sort_key)[:MAX_CANDIDATES]


async def content_aware_manifest_candidates(
    client: GitHubClient,
    repo_ref: str,
    branch: str,
    tree: Sequence[Mapping[str, Any]],
) -> tuple[list[RepositoryManifestCandidate], list[str]]:
    """Disambiguate Kustomize render roots from Kubernetes resources with the same name.

    Flux commonly stores a Kubernetes ``Kustomization`` custom resource in a
    file named ``kustomization.yaml``.  A tree-only classifier mistakes that
    file for a Kustomize build root, so the connection wizard later invokes
    ``kubectl kustomize`` and rejects a perfectly valid multi-document YAML.
    Inspect only the bounded Kustomize candidates and preserve the tree-only
    result when GitHub content cannot be read.
    """

    initial = manifest_candidates_from_tree(tree)
    candidate_directories = {
        candidate.path for candidate in initial if candidate.source_type == "kustomize"
    }
    ambiguous_paths = [
        path
        for item in tree
        if str(item.get("type") or "") == "blob"
        for path in [normalize_tree_path(str(item.get("path") or ""))]
        if path
        and path.rsplit("/", 1)[-1] in KUSTOMIZATION_FILES
        and parent_path(path) in candidate_directories
    ][:MAX_CANDIDATES]
    semaphore = asyncio.Semaphore(MANIFEST_SCAN_CONCURRENCY)

    async def inspect(path: str) -> tuple[str, bool | None]:
        async with semaphore:
            try:
                content = await client.content(repo_ref, branch, path)
            except RepositoryDiscoveryError:
                return path, None
            return path, is_kustomize_render_configuration(content)

    inspected = await asyncio.gather(*(inspect(path) for path in ambiguous_paths))
    render_directories = {
        parent_path(path) for path, is_render_config in inspected if is_render_config is True
    }
    classified_directories = {
        parent_path(path) for path, is_render_config in inspected if is_render_config is not None
    }
    candidates = {
        candidate.path: candidate
        for candidate in initial
        if not (
            candidate.source_type == "kustomize"
            and candidate.path in classified_directories
            and candidate.path not in render_directories
        )
    }
    for path, is_render_config in inspected:
        if is_render_config is False:
            add_candidate(candidates, path, "raw-yaml", "Kubernetes YAML manifest")

    unreadable_count = sum(is_render_config is None for _, is_render_config in inspected)
    warnings = (
        [f"{unreadable_count} Kustomization candidate(s) could not be content-classified"]
        if unreadable_count
        else []
    )
    return sorted(candidates.values(), key=candidate_sort_key)[:MAX_CANDIDATES], warnings


def is_kustomize_render_configuration(content: bytes) -> bool | None:
    """Return True for a Kustomize config, False for a Kubernetes YAML resource."""

    try:
        documents = [
            document
            for document in yaml.safe_load_all(content.decode("utf-8"))
            if document is not None
        ]
    except (UnicodeDecodeError, yaml.YAMLError):
        return None
    if len(documents) != 1 or not isinstance(documents[0], Mapping):
        return False
    document = documents[0]
    api_version = str(document.get("apiVersion") or "")
    kind = str(document.get("kind") or "")
    if api_version.startswith("kustomize.config.k8s.io/"):
        return True
    if api_version or (kind and kind != "Kustomization"):
        return False
    return bool(
        set(document)
        & {
            "resources",
            "bases",
            "components",
            "patches",
            "patchesStrategicMerge",
            "patchesJson6902",
            "configMapGenerator",
            "secretGenerator",
            "generators",
            "transformers",
            "images",
            "replacements",
        }
    )


def normalize_tree_path(path: str) -> str:
    path = path.strip().strip("/")
    if not path or "\\" in path:
        return ""
    parts = [part for part in path.split("/") if part]
    if any(part in {".", ".."} for part in parts):
        return ""
    return "/".join(parts)


def parent_path(path: str) -> str:
    if "/" not in path:
        return "."
    return path.rsplit("/", 1)[0]


def manifest_extension(path: str) -> str:
    lowered = path.lower()
    for extension in MANIFEST_EXTENSIONS:
        if lowered.endswith(extension):
            return extension
    return ""


def add_candidate(
    candidates: dict[str, RepositoryManifestCandidate],
    path: str,
    source_type: str,
    reason: str,
) -> None:
    existing = candidates.get(path)
    if existing is not None and source_priority(existing.source_type) <= source_priority(
        source_type
    ):
        return
    candidates[path] = RepositoryManifestCandidate(
        path=path,
        source_type=source_type,
        display_name=display_name(path, source_type),
        reason=reason,
    )


def display_name(path: str, source_type: str) -> str:
    label = {
        "helm": "Helm",
        "kustomize": "Kustomize",
        "raw-json": "JSON",
        "raw-yaml": "YAML",
    }.get(source_type, source_type)
    return f"{path} ({label})"


def source_priority(source_type: str) -> int:
    return {"kustomize": 0, "helm": 1, "raw-yaml": 2, "raw-json": 3}.get(source_type, 9)


def candidate_sort_key(candidate: RepositoryManifestCandidate) -> tuple[int, int, int, str]:
    common_paths = {
        "deploy.yaml": 0,
        "deployment.yaml": 1,
        "k8s": 2,
        "manifests": 3,
        "deploy": 4,
        ".": 5,
    }
    common_score = common_paths.get(candidate.path, 50)
    return (
        common_score,
        source_priority(candidate.source_type),
        candidate.path.count("/"),
        candidate.path,
    )


def source_type_from_path(path: str) -> str:
    basename = path.rsplit("/", 1)[-1]
    if path == "." or manifest_extension(path) == "":
        return "kustomize"
    if basename == HELM_CHART_FILE:
        return "helm"
    if basename in KUSTOMIZATION_FILES:
        return "kustomize"
    if manifest_extension(path) == ".json":
        return "raw-json"
    return "raw-yaml"


def normalize_source_type(value: str) -> str:
    source_type = value.strip().lower()
    if not source_type:
        return ""
    if source_type not in {"raw-yaml", "raw-json", "kustomize", "helm"}:
        raise ValueError("source_type must be raw-yaml, raw-json, kustomize, or helm")
    return source_type


def validate_manifest_text(
    repo_ref: str,
    branch: str,
    manifest_path: str,
    text: str,
    source_type: str,
    *,
    validation_mode: str = "static-parse",
    parse_warning: str = STATIC_PARSE_WARNING,
    parse_error_prefix: str = "manifest parse failed",
    extra_warnings: Sequence[str] = (),
) -> RepositoryManifestValidationResponse:
    resources: list[RepositoryManifestResource] = []
    warnings = [parse_warning, *extra_warnings] if parse_warning else list(extra_warnings)
    errors: list[str] = []
    try:
        docs = parse_manifest_documents(text, source_type)
    except (ValueError, yaml.YAMLError, json.JSONDecodeError) as exc:
        return RepositoryManifestValidationResponse(
            repo_ref=repo_ref,
            branch=branch,
            manifest_path=manifest_path,
            valid=False,
            status="invalid",
            validation_mode=validation_mode,
            warnings=dedupe(warnings),
            errors=[f"{parse_error_prefix}: {exc}"],
        )
    for index, doc in enumerate(docs, start=1):
        if doc is None:
            continue
        if not isinstance(doc, Mapping):
            warnings.append(f"document {index} is not a Kubernetes object")
            continue
        for resource, resource_warnings in resource_items_from_document(doc, index):
            if resource is None:
                warnings.extend(resource_warnings)
            else:
                resources.append(resource)
                warnings.extend(resource_warnings)
    if not resources:
        warnings.append("no Kubernetes resources with kind and metadata.name were found")
    status = "valid" if resources and not errors else "warning"
    return RepositoryManifestValidationResponse(
        repo_ref=repo_ref,
        branch=branch,
        manifest_path=manifest_path,
        valid=bool(resources) and not errors,
        status=status,
        validation_mode=validation_mode,
        resource_count=len(resources),
        resources=resources,
        warnings=dedupe(warnings),
        errors=errors,
    )


def manifest_kinds(text: str, source_type: str) -> list[str]:
    kinds: list[str] = []
    for doc in parse_manifest_documents(text, source_type):
        if isinstance(doc, Mapping):
            kind = str(doc.get("kind") or "").strip()
            if kind:
                kinds.append(kind)
            if kind == "List" and isinstance(doc.get("items"), list):
                for item in doc["items"]:
                    if isinstance(item, Mapping):
                        item_kind = str(item.get("kind") or "").strip()
                        if item_kind:
                            kinds.append(item_kind)
    return dedupe(kinds)


def parse_manifest_documents(text: str, source_type: str) -> list[Any]:
    if source_type == "raw-json":
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    return list(yaml.safe_load_all(text))


def flatten_manifest_objects(docs: Sequence[Any]) -> list[JsonMap]:
    """파싱된 문서를 개별 Kubernetes 오브젝트 목록으로 평탄화(List kind 전개, 비객체 제외)."""
    objects: list[JsonMap] = []
    for doc in docs:
        if not isinstance(doc, Mapping):
            continue
        if str(doc.get("kind") or "") == "List" and isinstance(doc.get("items"), list):
            for item in doc["items"]:
                if isinstance(item, Mapping):
                    objects.append(dict(item))
            continue
        objects.append(dict(doc))
    return objects


def resource_items_from_document(
    doc: Mapping[str, Any], index: int
) -> list[tuple[RepositoryManifestResource | None, list[str]]]:
    if str(doc.get("kind") or "") == "List" and isinstance(doc.get("items"), list):
        items = doc.get("items")
        assert isinstance(items, list)
        return [
            resource_item_from_object(item, f"document {index} item {item_index}")
            for item_index, item in enumerate(items, start=1)
        ]
    return [resource_item_from_object(doc, f"document {index}")]


def resource_item_from_object(
    value: Any, label: str
) -> tuple[RepositoryManifestResource | None, list[str]]:
    warnings = []
    if not isinstance(value, Mapping):
        return None, [f"{label} is not a Kubernetes object"]
    metadata = value.get("metadata")
    metadata_map = metadata if isinstance(metadata, Mapping) else {}
    kind = str(value.get("kind") or "").strip()
    name = str(metadata_map.get("name") or "").strip()
    api_version = str(value.get("apiVersion") or "").strip()
    namespace_value = metadata_map.get("namespace")
    namespace = str(namespace_value) if namespace_value is not None else None
    if not kind or not name:
        return None, [f"{label} is missing kind or metadata.name"]
    if kind == "Secret":
        warnings.append(f"{label} is a Secret; secret data is not returned by discovery")
    return (
        RepositoryManifestResource(
            api_version=api_version,
            kind=kind,
            namespace=namespace,
            name=name,
        ),
        warnings,
    )


def dedupe(values: Sequence[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result

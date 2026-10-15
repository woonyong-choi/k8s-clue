from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any

import httpx
from kubernetes_api import (
    kubernetes_api_base_url,
    kubernetes_client,
    kubernetes_headers,
    service_account_token,
)
from queries import KubernetesSnapshotQuery
from telemetry_registry import telemetry

from config import (
    KUBERNETES_API_TIMEOUT_SECONDS,
    KUBERNETES_EVENT_CAPTURE_FRESHNESS_SECONDS,
    KUBERNETES_EVENT_CAPTURE_MAX_ITEMS,
    KUBERNETES_EVENT_CAPTURE_MAX_PAGES,
    KUBERNETES_EVENT_CAPTURE_PAGE_SIZE,
    TARGET_CLUSTER_ID_ENV,
)
from packages.config.constants import Target
from packages.contracts.event_bus.interfaces import JsonObject
from packages.contracts.kubernetes_discovery import (
    MAX_API_DISCOVERY_DOCUMENTS,
    ApiResourceDescriptor,
    normalize_api_resource_discovery,
)
from packages.contracts.target import TARGET_NAMESPACE
from packages.kubernetes_provider import detect_kubernetes_provider
from packages.kubernetes_quantity import cpu_millicores, memory_mebibytes
from providers.base import ConfigReader
from providers.collection_limits import (
    attach_collection_limits,
    limit_payload_list,
    limit_payload_size,
)
from providers.kubernetes_utils import (
    K8S_ENDPOINT_SLICE_SERVICE_NAME_LABEL,
    K8S_KIND_DEPLOYMENT,
    K8S_KIND_REPLICA_SET,
    K8S_RESOURCE_CONTROLLER_REVISIONS,
    K8S_RESOURCE_DEPLOYMENTS,
    K8S_RESOURCE_ENDPOINT_SLICES,
    K8S_RESOURCE_PODS,
    K8S_RESOURCE_REPLICASETS,
    K8S_RESOURCE_RESOURCE_QUOTAS,
    K8S_RESOURCE_SERVICES,
    compact_dict,
    items,
    metadata,
    object_or_empty,
    resource_sort_key,
    spec,
    status,
)

K8S_SNAPSHOT_ENDPOINTS_KEY = "endpoints"
K8S_SNAPSHOT_EVENTS_KEY = "events"
K8S_EVENT_CAPTURE_KEY = "event_capture"
K8S_EVENT_CAPTURE_EVENTS_KEY = "events"
K8S_SNAPSHOT_NODES_KEY = "nodes"
K8S_SNAPSHOT_WORKLOADS_KEY = "workloads"
K8S_SNAPSHOT_WORKLOAD_REVISIONS_KEY = "workload_revisions"
K8S_STATEFULSETS_KEY = "statefulsets"
K8S_DAEMONSETS_KEY = "daemonsets"
K8S_JOBS_KEY = "jobs"
K8S_CRONJOBS_KEY = "cronjobs"
K8S_INGRESSES_KEY = "ingresses"
K8S_API_RESOURCE_DISCOVERY_KEY = "api_resource_discovery"
K8S_DYNAMIC_RESOURCE_COLLECTIONS_KEY = "dynamic_resource_collections"
K8S_CUSTOM_RESOURCES_KEY = "custom_resources"
K8S_RESOURCE_ACCESS_KEY = "resource_access"
K8S_COLLECTION_STATUS_KEY = "collection_status"
K8S_LIST_COLLECTION_STATUS_KEY = "kyro_collection_status"
K8S_COLLECTION_RBAC_DENIED_REASON = "collection_rbac_denied"
K8S_COLLECTION_STATUS_REASON_CODES = frozenset((K8S_COLLECTION_RBAC_DENIED_REASON,))
# Marks a list response that was not a JSON object, so an empty fallback is never read as proof.
K8S_LIST_MALFORMED_KEY = "kyro_list_malformed"
# Namespace-scoped completeness proof; the gateway consumes it as inventory collection coverage.
K8S_COLLECTION_COVERAGE_KEY = "collection_coverage"
COVERAGE_REASON_QUERY_NOT_SUCCESSFUL = "query_not_successful"
COVERAGE_REASON_MALFORMED_LIST = "malformed_list"
COVERAGE_REASON_CONTINUATION = "continuation"
COVERAGE_REASON_LABEL_SELECTOR = "label_selector"
COVERAGE_REASON_IDENTITY_MISSING = "identity_missing"
COVERAGE_REASON_NAMESPACE_MISMATCH = "namespace_mismatch"
COVERAGE_REASON_PAYLOAD_LIMIT = "payload_limit"
K8S_CRD_DISCOVERY_PATH = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions"

MAX_KUBERNETES_PODS = 500
MAX_KUBERNETES_EVENTS = 200
MAX_KUBERNETES_NODES = 100
MAX_KUBERNETES_WORKLOADS = 500
MAX_KUBERNETES_SERVICES = 300
MAX_KUBERNETES_ENDPOINTS = 300
MAX_KUBERNETES_INGRESSES = 300
MAX_KUBERNETES_RESOURCE_QUOTAS = 200
MAX_KUBERNETES_CUSTOM_RESOURCES = 1_000
KUBERNETES_ACCESS_PAGE_SIZE = 500
KUBERNETES_ACCESS_MAX_PAGES = 20
KUBERNETES_ACCESS_MAX_ITEMS = 5000
KUBERNETES_LIST_LIMITS = {
    K8S_RESOURCE_PODS: MAX_KUBERNETES_PODS,
    K8S_SNAPSHOT_EVENTS_KEY: MAX_KUBERNETES_EVENTS,
    K8S_SNAPSHOT_NODES_KEY: MAX_KUBERNETES_NODES,
    K8S_SNAPSHOT_WORKLOADS_KEY: MAX_KUBERNETES_WORKLOADS,
    K8S_SNAPSHOT_WORKLOAD_REVISIONS_KEY: MAX_KUBERNETES_WORKLOADS,
    K8S_RESOURCE_SERVICES: MAX_KUBERNETES_SERVICES,
    K8S_SNAPSHOT_ENDPOINTS_KEY: MAX_KUBERNETES_ENDPOINTS,
    K8S_INGRESSES_KEY: MAX_KUBERNETES_INGRESSES,
    K8S_RESOURCE_RESOURCE_QUOTAS: MAX_KUBERNETES_RESOURCE_QUOTAS,
    K8S_CUSTOM_RESOURCES_KEY: MAX_KUBERNETES_CUSTOM_RESOURCES,
}
KUBERNETES_NAMESPACED_LIST_KEYS = {
    K8S_RESOURCE_PODS,
    K8S_SNAPSHOT_EVENTS_KEY,
    K8S_SNAPSHOT_WORKLOADS_KEY,
    K8S_SNAPSHOT_WORKLOAD_REVISIONS_KEY,
    K8S_RESOURCE_SERVICES,
    K8S_SNAPSHOT_ENDPOINTS_KEY,
    K8S_INGRESSES_KEY,
    K8S_RESOURCE_RESOURCE_QUOTAS,
    K8S_CUSTOM_RESOURCES_KEY,
}

DYNAMIC_RESOURCE_REASON_NOT_CONFIGURED = "not_configured"
DYNAMIC_RESOURCE_REASON_DISCOVERY_RBAC = "discovery_rbac_denied"
DYNAMIC_RESOURCE_REASON_DISCOVERY_UNAVAILABLE = "discovery_unavailable"
DYNAMIC_RESOURCE_REASON_RESOURCE_NOT_DISCOVERED = "resource_not_discovered"
DYNAMIC_RESOURCE_REASON_LIST_UNSUPPORTED = "list_not_supported"
DYNAMIC_RESOURCE_REASON_SCOPE_MISMATCH = "namespace_scope_mismatch"
DYNAMIC_RESOURCE_REASON_RBAC_DENIED = "rbac_denied"
DYNAMIC_RESOURCE_REASON_RESOURCE_NOT_FOUND = "resource_not_found"
DYNAMIC_RESOURCE_REASON_INVALID_RESPONSE = "invalid_response"
DYNAMIC_RESOURCE_REASON_IDENTITY_MISMATCH = "identity_mismatch"
DYNAMIC_RESOURCE_REASON_PAGE_LIMIT = "page_limit_exceeded"
DYNAMIC_RESOURCE_REASON_ITEM_LIMIT = "item_limit_exceeded"
DYNAMIC_RESOURCE_REASON_PAYLOAD_LIMIT = "payload_limit_exceeded"
DYNAMIC_RESOURCE_REASON_TIMEOUT = "timeout"
DYNAMIC_RESOURCE_REASON_NETWORK = "network_error"

KUBERNETES_ACCESS_COLLECTIONS = {
    "roles": "/apis/rbac.authorization.k8s.io/v1/roles",
    "cluster_roles": "/apis/rbac.authorization.k8s.io/v1/clusterroles",
    "role_bindings": "/apis/rbac.authorization.k8s.io/v1/rolebindings",
    "cluster_role_bindings": "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
    "service_accounts": "/api/v1/serviceaccounts",
    "pod_subjects": "/api/v1/pods",
}

EVENT_CAPTURE_REASON_COMPLETE = "complete"
EVENT_CAPTURE_REASON_ITEM_LIMIT = "item_limit_exceeded"
EVENT_CAPTURE_REASON_PAGE_LIMIT = "page_limit_exceeded"
EVENT_CAPTURE_REASON_INVALID_RESPONSE = "invalid_response"
EVENT_CAPTURE_REASON_INVALID_EVENT = "invalid_event_contract"
EVENT_CAPTURE_REASON_NETWORK = "network_error"
EVENT_CAPTURE_REASON_NOT_CONFIGURED = "not_configured"
EVENT_CAPTURE_REASON_NOT_REQUESTED = "not_requested"
EVENT_CAPTURE_REASON_RBAC_DENIED = "rbac_denied"
EVENT_CAPTURE_REASON_TIMEOUT = "timeout"

EVENT_REASON_BACK_OFF = "BackOff"
EVENT_REASON_FAILED = "Failed"
EVENT_REASON_FAILED_MOUNT = "FailedMount"
EVENT_REASON_FAILED_SCHEDULING = "FailedScheduling"
EVENT_REASON_OOM_KILLING = "OOMKilling"
EVENT_REASON_UNHEALTHY = "Unhealthy"

EVENT_CATEGORY_BACKOFF = "backoff"
EVENT_CATEGORY_CONFIG_MOUNT = "config_or_volume_mount"
EVENT_CATEGORY_CONTAINER_RESTART = "container_restart"
EVENT_CATEGORY_IMAGE_PULL = "image_pull"
EVENT_CATEGORY_OOM = "oom_killed"
EVENT_CATEGORY_PROBE = "probe"
EVENT_CATEGORY_SCHEDULING = "scheduling"

EVENT_SIGNAL_ERR_IMAGE_PULL = "ErrImagePull"
EVENT_SIGNAL_FAILED_SCHEDULING = EVENT_REASON_FAILED_SCHEDULING
EVENT_SIGNAL_IMAGE_PULL_BACKOFF = "ImagePullBackOff"
EVENT_SIGNAL_OOM_KILLED = "OOMKilled"

EVENT_SYMPTOM_CRASH_LOOP = "CrashLoopBackOff"
EVENT_SYMPTOM_FAILED_MOUNT = EVENT_REASON_FAILED_MOUNT
EVENT_SYMPTOM_FAILED_SCHEDULING = EVENT_REASON_FAILED_SCHEDULING
EVENT_SYMPTOM_IMAGE_PULL = EVENT_SIGNAL_IMAGE_PULL_BACKOFF
EVENT_SYMPTOM_PROBE_FAILURE = "ProbeFailure"

PROBE_SIGNAL_DEFAULT = "ProbeFailed"
PROBE_SIGNAL_LIVENESS = "LivenessProbeFailed"
PROBE_SIGNAL_READINESS = "ReadinessProbeFailed"
PROBE_SIGNAL_STARTUP = "StartupProbeFailed"

SCHEDULING_CAUSE_PATTERNS = (
    ("insufficient_cpu", ("insufficient cpu",)),
    ("insufficient_memory", ("insufficient memory",)),
    ("node_selector_mismatch", ("node affinity/selector", "node selector")),
    ("taint_toleration_mismatch", ("taint", "toleration")),
    ("pod_count_limit", ("too many pods",)),
    ("volume_node_affinity_conflict", ("volume node affinity conflict",)),
)


@telemetry.source(
    source="kubernetes",
    evidence_key="kubernetes",
    query_type=KubernetesSnapshotQuery,
)
class KubernetesSnapshotProvider:
    """Collect Kubernetes state for one target cluster.
    It builds the kubernetes evidence bucket.
    """

    span_name = "kubernetes.collect"
    query_count_attribute = "kubernetes.query_count"
    result_count_attribute = "kubernetes.result_count"
    timeout_seconds = KUBERNETES_API_TIMEOUT_SECONDS
    failure_message = "kubernetes snapshot collection failed"
    queries: tuple[KubernetesSnapshotQuery, ...] = ()

    def __init__(
        self,
        *,
        cluster_id: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Store the cluster id and an optional HTTP transport for tests."""
        self.cluster_id = cluster_id
        self.transport = transport

    @classmethod
    def from_config(cls, read_config: ConfigReader) -> KubernetesSnapshotProvider:
        """Create the provider from agent config values."""
        return cls(cluster_id=read_config(TARGET_CLUSTER_ID_ENV, Target.DEFAULT_CLUSTER_ID))

    async def query(
        self,
        _client: httpx.AsyncClient,
        telemetry_query: KubernetesSnapshotQuery,
    ) -> JsonObject:
        """Read Kubernetes objects from the target namespace.
        Return the raw API results in one payload.
        """
        base_url = kubernetes_api_base_url()
        token = service_account_token()
        if telemetry_query.is_cluster_api_discovery:
            async with kubernetes_client(self.transport) as client:
                return await self.query_cluster_api_discovery(
                    base_url=base_url,
                    token=token,
                    client=client,
                )
        if telemetry_query.is_cluster_access_snapshot:
            async with kubernetes_client(self.transport) as client:
                return await self.query_cluster_access_snapshot(
                    base_url=base_url,
                    token=token,
                    client=client,
                )
        if telemetry_query.is_dynamic_resource_collection:
            async with kubernetes_client(self.transport) as client:
                return await self.query_dynamic_resource_collection(
                    base_url=base_url,
                    token=token,
                    client=client,
                    telemetry_query=telemetry_query,
                )
        if telemetry_query.is_cluster_wide_event_capture:
            async with kubernetes_client(self.transport) as client:
                return await self.query_cluster_wide_event_capture(
                    base_url=base_url,
                    token=token,
                    client=client,
                )
        namespace = telemetry_query.namespace or TARGET_NAMESPACE
        if not base_url or not token:
            return {
                "status": "unavailable",
                "reason": "kubernetes api is not configured",
                "namespace": namespace,
                "cluster_id": self.cluster_id,
            }

        headers = kubernetes_headers(token)
        async with kubernetes_client(self.transport) as client:
            return {
                "status": "success",
                "namespace": namespace,
                "cluster_id": self.cluster_id,
                "collected_at": datetime.now(UTC).isoformat(),
                K8S_RESOURCE_PODS: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/api/v1/namespaces/{namespace}/{K8S_RESOURCE_PODS}",
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_SNAPSHOT_EVENTS_KEY: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/api/v1/namespaces/{namespace}/events",
                ),
                K8S_SNAPSHOT_NODES_KEY: await self.get_json(
                    client, base_url, headers, "/api/v1/nodes"
                ),
                "pod_metrics": await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/{K8S_RESOURCE_PODS}",
                    allow_not_found=True,
                ),
                "node_metrics": await self.get_json(
                    client,
                    base_url,
                    headers,
                    "/apis/metrics.k8s.io/v1beta1/nodes",
                    allow_not_found=True,
                ),
                K8S_RESOURCE_DEPLOYMENTS: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/apps/v1/namespaces/{namespace}/{K8S_RESOURCE_DEPLOYMENTS}",
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_STATEFULSETS_KEY: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/apps/v1/namespaces/{namespace}/{K8S_STATEFULSETS_KEY}",
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_DAEMONSETS_KEY: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/apps/v1/namespaces/{namespace}/{K8S_DAEMONSETS_KEY}",
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_RESOURCE_REPLICASETS: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/apps/v1/namespaces/{namespace}/{K8S_RESOURCE_REPLICASETS}",
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_RESOURCE_CONTROLLER_REVISIONS: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/apps/v1/namespaces/{namespace}/{K8S_RESOURCE_CONTROLLER_REVISIONS}",
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_JOBS_KEY: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/batch/v1/namespaces/{namespace}/{K8S_JOBS_KEY}",
                    allow_not_found=True,
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_CRONJOBS_KEY: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/batch/v1/namespaces/{namespace}/{K8S_CRONJOBS_KEY}",
                    allow_not_found=True,
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_RESOURCE_SERVICES: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/api/v1/namespaces/{namespace}/{K8S_RESOURCE_SERVICES}",
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_RESOURCE_ENDPOINT_SLICES: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/discovery.k8s.io/v1/namespaces/{namespace}/{K8S_RESOURCE_ENDPOINT_SLICES}",
                    allow_not_found=True,
                ),
                K8S_INGRESSES_KEY: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/apis/networking.k8s.io/v1/namespaces/{namespace}/{K8S_INGRESSES_KEY}",
                    allow_not_found=True,
                    label_selector=telemetry_query.label_selector,
                ),
                K8S_RESOURCE_RESOURCE_QUOTAS: await self.get_json(
                    client,
                    base_url,
                    headers,
                    f"/api/v1/namespaces/{namespace}/{K8S_RESOURCE_RESOURCE_QUOTAS}",
                    allow_not_found=True,
                ),
            }

    async def query_cluster_access_snapshot(
        self,
        *,
        base_url: str | None,
        token: str | None,
        client: httpx.AsyncClient,
    ) -> JsonObject:
        """Collect one all-or-nothing RBAC cut used by every reverse lookup."""
        observed_at = datetime.now(UTC).isoformat()
        if not base_url or not token:
            return unavailable_resource_access(
                self.cluster_id,
                observed_at,
                "kubernetes_api_not_configured",
            )
        headers = kubernetes_headers(token)
        collections: JsonObject = {}
        for key, path in KUBERNETES_ACCESS_COLLECTIONS.items():
            rows, reason = await self._paginated_access_collection(
                client=client,
                base_url=base_url,
                headers=headers,
                path=path,
            )
            if reason is not None:
                return unavailable_resource_access(
                    self.cluster_id,
                    observed_at,
                    f"{key}:{reason}",
                )
            collections[key] = rows
        return {
            "status": "success",
            "cluster_id": self.cluster_id,
            "collected_at": observed_at,
            K8S_RESOURCE_ACCESS_KEY: {
                "completeness": "exact",
                "observed_at": observed_at,
                "reason_codes": [],
                **collections,
            },
        }

    async def _paginated_access_collection(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        headers: dict[str, str],
        path: str,
    ) -> tuple[list[JsonObject], str | None]:
        rows: list[JsonObject] = []
        continuation: str | None = None
        for _page in range(KUBERNETES_ACCESS_MAX_PAGES):
            params: dict[str, str | int] = {"limit": KUBERNETES_ACCESS_PAGE_SIZE}
            if continuation:
                params["continue"] = continuation
            try:
                response = await client.get(
                    f"{base_url}{path}",
                    headers=headers,
                    params=params,
                )
            except httpx.TimeoutException:
                return [], "timeout"
            except httpx.NetworkError:
                return [], "network_error"
            if response.status_code in {401, 403}:
                return [], "rbac_denied"
            if response.is_error:
                return [], f"http_{response.status_code}"
            try:
                payload = response.json()
            except ValueError:
                return [], "invalid_response"
            if not isinstance(payload, dict):
                return [], "invalid_response"
            page_items = items(payload)
            if len(rows) + len(page_items) > KUBERNETES_ACCESS_MAX_ITEMS:
                return [], "item_limit_exceeded"
            rows.extend(page_items)
            next_continuation = metadata(payload).get("continue")
            continuation = str(next_continuation) if next_continuation else None
            if continuation is None:
                return rows, None
        return [], "page_limit_exceeded"

    async def query_cluster_api_discovery(
        self,
        *,
        base_url: str | None,
        token: str | None,
        client: httpx.AsyncClient,
    ) -> JsonObject:
        """Collect the authorized API catalog while preserving partial RBAC evidence."""
        collected_at = datetime.now(UTC).isoformat()
        if not base_url or not token:
            return {
                "status": "unavailable",
                "cluster_id": self.cluster_id,
                "collected_at": collected_at,
                "documents": [],
                "custom_resource_definitions": None,
                "reason_codes": ["kubernetes_api_not_configured"],
                "truncated": False,
            }

        headers = kubernetes_headers(token)
        documents: list[JsonObject] = []
        reason_codes: list[str] = []
        core_versions = await self._discovery_versions(
            client=client,
            base_url=base_url,
            headers=headers,
            path="/api",
            collection_key="versions",
            failure_reason="core_versions_failed",
            reason_codes=reason_codes,
        )
        for group_version in core_versions[:MAX_API_DISCOVERY_DOCUMENTS]:
            document = await self._discovery_document(
                client=client,
                base_url=base_url,
                headers=headers,
                path=f"/api/{group_version}",
            )
            if document is None:
                reason_codes.append(f"group_version_failed:{group_version}")
                continue
            documents.append(document)
        group_versions = await self._discovery_group_versions(
            client=client,
            base_url=base_url,
            headers=headers,
            reason_codes=reason_codes,
        )
        version_count = len(core_versions) + len(group_versions)
        truncated = version_count > MAX_API_DISCOVERY_DOCUMENTS
        remaining = max(MAX_API_DISCOVERY_DOCUMENTS - len(core_versions), 0)
        for group_version in group_versions[:remaining]:
            document = await self._discovery_document(
                client=client,
                base_url=base_url,
                headers=headers,
                path=f"/apis/{group_version}",
            )
            if document is None:
                reason_codes.append(f"group_version_failed:{group_version}")
                continue
            documents.append(document)

        custom_resource_definitions = await self._custom_resource_definitions(
            client=client,
            base_url=base_url,
            headers=headers,
            reason_codes=reason_codes,
        )
        return {
            "status": "success" if not reason_codes and not truncated else "partial",
            "cluster_id": self.cluster_id,
            "collected_at": collected_at,
            "documents": documents,
            "custom_resource_definitions": custom_resource_definitions,
            "reason_codes": sorted(set(reason_codes)),
            "truncated": truncated,
        }

    async def _discovery_versions(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        headers: dict[str, str],
        path: str,
        collection_key: str,
        failure_reason: str,
        reason_codes: list[str],
    ) -> list[str]:
        try:
            response = await client.get(f"{base_url}{path}", headers=headers)
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            reason_codes.append(failure_reason)
            return []
        values = payload.get(collection_key) if isinstance(payload, dict) else None
        if not isinstance(values, list):
            reason_codes.append(f"{failure_reason}:invalid")
            return []
        return sorted(
            {value.strip() for value in values if isinstance(value, str) and value.strip()}
        )

    async def _discovery_group_versions(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        headers: dict[str, str],
        reason_codes: list[str],
    ) -> list[str]:
        try:
            response = await client.get(f"{base_url}/apis", headers=headers)
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            reason_codes.append("api_groups_failed")
            return []
        groups = payload.get("groups") if isinstance(payload, dict) else None
        if not isinstance(groups, list):
            reason_codes.append("api_groups_invalid")
            return []
        versions: set[str] = set()
        for group in groups:
            if not isinstance(group, dict):
                continue
            raw_versions = group.get("versions")
            if not isinstance(raw_versions, list):
                continue
            versions.update(
                str(version.get("groupVersion")).strip()
                for version in raw_versions
                if isinstance(version, dict) and str(version.get("groupVersion") or "").strip()
            )
        return sorted(versions)

    async def _discovery_document(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        headers: dict[str, str],
        path: str,
    ) -> JsonObject | None:
        try:
            response = await client.get(f"{base_url}{path}", headers=headers)
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            return None
        return payload if isinstance(payload, dict) else None

    async def _custom_resource_definitions(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        headers: dict[str, str],
        reason_codes: list[str],
    ) -> list[JsonObject] | None:
        try:
            response = await client.get(f"{base_url}{K8S_CRD_DISCOVERY_PATH}", headers=headers)
        except httpx.HTTPError:
            reason_codes.append("crd_discovery_failed")
            return None
        if response.status_code in {401, 403}:
            reason_codes.append("crd_discovery_forbidden")
            return None
        if response.is_error:
            reason_codes.append(f"crd_discovery_http_{response.status_code}")
            return None
        try:
            payload = response.json()
        except ValueError:
            reason_codes.append("crd_discovery_invalid")
            return None
        return items(payload)

    async def query_dynamic_resource_collection(
        self,
        *,
        base_url: str | None,
        token: str | None,
        client: httpx.AsyncClient,
        telemetry_query: KubernetesSnapshotQuery,
    ) -> JsonObject:
        """Collect one typed GVR only after exact live discovery resolution."""

        collected_at = datetime.now(UTC).isoformat()
        spec = telemetry_query.dynamic_resource
        if spec is None:
            return dynamic_resource_query_result(
                cluster_id=self.cluster_id,
                collected_at=collected_at,
                telemetry_query=telemetry_query,
                descriptor=None,
                resources=[],
                reason_codes=(DYNAMIC_RESOURCE_REASON_INVALID_RESPONSE,),
            )
        if not base_url or not token:
            return dynamic_resource_query_result(
                cluster_id=self.cluster_id,
                collected_at=collected_at,
                telemetry_query=telemetry_query,
                descriptor=None,
                resources=[],
                reason_codes=(DYNAMIC_RESOURCE_REASON_NOT_CONFIGURED,),
            )

        headers = kubernetes_headers(token)
        descriptor, discovery_reason = await self._resolve_dynamic_resource_descriptor(
            base_url=base_url,
            headers=headers,
            client=client,
            telemetry_query=telemetry_query,
            collected_at=collected_at,
        )
        if descriptor is None:
            return dynamic_resource_query_result(
                cluster_id=self.cluster_id,
                collected_at=collected_at,
                telemetry_query=telemetry_query,
                descriptor=None,
                resources=[],
                reason_codes=(discovery_reason or DYNAMIC_RESOURCE_REASON_DISCOVERY_UNAVAILABLE,),
            )
        if descriptor.namespaced != bool(spec.namespaces):
            return dynamic_resource_query_result(
                cluster_id=self.cluster_id,
                collected_at=collected_at,
                telemetry_query=telemetry_query,
                descriptor=descriptor,
                resources=[],
                reason_codes=(DYNAMIC_RESOURCE_REASON_SCOPE_MISMATCH,),
            )

        namespaces: tuple[str | None, ...] = (
            tuple(spec.namespaces) if descriptor.namespaced else (None,)
        )
        resources: dict[str, JsonObject] = {}
        reason_codes: set[str] = set()
        page_count = 0
        observed_count = 0
        stop = False
        for namespace in namespaces:
            continuation: str | None = None
            seen_continuations: set[str] = set()
            while not stop:
                if page_count >= spec.max_pages:
                    reason_codes.add(DYNAMIC_RESOURCE_REASON_PAGE_LIMIT)
                    stop = True
                    break
                remaining = spec.max_items - len(resources)
                if remaining <= 0:
                    reason_codes.add(DYNAMIC_RESOURCE_REASON_ITEM_LIMIT)
                    stop = True
                    break
                params: dict[str, str | int] = {"limit": min(spec.page_size, remaining)}
                if continuation:
                    params["continue"] = continuation
                try:
                    response = await client.get(
                        f"{base_url}{dynamic_resource_list_path(descriptor, namespace)}",
                        headers=headers,
                        params=params,
                    )
                except httpx.TimeoutException:
                    reason_codes.add(DYNAMIC_RESOURCE_REASON_TIMEOUT)
                    stop = True
                    break
                except httpx.NetworkError:
                    reason_codes.add(DYNAMIC_RESOURCE_REASON_NETWORK)
                    stop = True
                    break
                if response.status_code in {401, 403}:
                    reason_codes.add(DYNAMIC_RESOURCE_REASON_RBAC_DENIED)
                    stop = True
                    break
                if response.status_code == 404:
                    reason_codes.add(DYNAMIC_RESOURCE_REASON_RESOURCE_NOT_FOUND)
                    stop = True
                    break
                if response.is_error:
                    reason_codes.add(f"http_{response.status_code}")
                    stop = True
                    break
                try:
                    page = response.json()
                except ValueError:
                    page = None
                if not isinstance(page, dict) or not isinstance(page.get("items"), list):
                    reason_codes.add(DYNAMIC_RESOURCE_REASON_INVALID_RESPONSE)
                    stop = True
                    break

                page_count += 1
                page_items = page["items"]
                observed_count += len(page_items)
                for raw_item in page_items:
                    if len(resources) >= spec.max_items:
                        reason_codes.add(DYNAMIC_RESOURCE_REASON_ITEM_LIMIT)
                        stop = True
                        break
                    normalized = canonical_dynamic_resource(raw_item, descriptor, namespace)
                    if normalized is None:
                        reason_codes.add(DYNAMIC_RESOURCE_REASON_IDENTITY_MISMATCH)
                        continue
                    identity = str(normalized["uid"])
                    if identity in resources:
                        reason_codes.add(DYNAMIC_RESOURCE_REASON_IDENTITY_MISMATCH)
                        continue
                    resources[identity] = normalized

                next_continuation = metadata(page).get("continue")
                continuation = str(next_continuation) if next_continuation else None
                if continuation is None:
                    break
                if continuation in seen_continuations:
                    reason_codes.add(DYNAMIC_RESOURCE_REASON_INVALID_RESPONSE)
                    stop = True
                    break
                seen_continuations.add(continuation)
                if len(resources) >= spec.max_items:
                    reason_codes.add(DYNAMIC_RESOURCE_REASON_ITEM_LIMIT)
                    stop = True
                    break

        return dynamic_resource_query_result(
            cluster_id=self.cluster_id,
            collected_at=collected_at,
            telemetry_query=telemetry_query,
            descriptor=descriptor,
            resources=list(resources.values()),
            reason_codes=tuple(sorted(reason_codes)),
            page_count=page_count,
            observed_count=observed_count,
        )

    async def _resolve_dynamic_resource_descriptor(
        self,
        *,
        base_url: str,
        headers: dict[str, str],
        client: httpx.AsyncClient,
        telemetry_query: KubernetesSnapshotQuery,
        collected_at: str,
    ) -> tuple[ApiResourceDescriptor | None, str | None]:
        spec = telemetry_query.dynamic_resource
        if spec is None:
            return None, DYNAMIC_RESOURCE_REASON_INVALID_RESPONSE
        path = dynamic_resource_discovery_path(spec.group, spec.version)
        try:
            response = await client.get(f"{base_url}{path}", headers=headers)
        except httpx.TimeoutException:
            return None, DYNAMIC_RESOURCE_REASON_TIMEOUT
        except httpx.NetworkError:
            return None, DYNAMIC_RESOURCE_REASON_NETWORK
        if response.status_code in {401, 403}:
            return None, DYNAMIC_RESOURCE_REASON_DISCOVERY_RBAC
        if response.is_error:
            return None, DYNAMIC_RESOURCE_REASON_DISCOVERY_UNAVAILABLE
        try:
            document = response.json()
        except ValueError:
            document = None
        expected_api_version = f"{spec.group}/{spec.version}" if spec.group else spec.version
        if not isinstance(document, dict) or document.get("groupVersion") != expected_api_version:
            return None, DYNAMIC_RESOURCE_REASON_DISCOVERY_UNAVAILABLE
        discovery = normalize_api_resource_discovery(
            documents=[document],
            custom_resource_definitions=None,
            observed_at=collected_at,
        )
        matches = [
            descriptor
            for descriptor in discovery.resources
            if descriptor.group == spec.group
            and descriptor.version == spec.version
            and descriptor.name == spec.resource
        ]
        if len(matches) != 1:
            return None, DYNAMIC_RESOURCE_REASON_RESOURCE_NOT_DISCOVERED
        descriptor = matches[0]
        if "list" not in descriptor.verbs:
            return None, DYNAMIC_RESOURCE_REASON_LIST_UNSUPPORTED
        return descriptor, None

    async def query_cluster_wide_event_capture(
        self,
        *,
        base_url: str,
        token: str,
        client: httpx.AsyncClient,
    ) -> JsonObject:
        """List every Event through continuation tokens and return explicit coverage proof.

        This path is deliberately isolated from normal namespace snapshots. It returns
        only narrow Timeline facts and never raises a collection error into the generic
        evidence loop: a denied, timed-out, or bounded list must become visible gap
        evidence while remaining unsafe for Timeline append.
        """
        collected_at = datetime.now(UTC).isoformat()
        if not base_url or not token:
            return event_capture_query_result(
                cluster_id=self.cluster_id,
                collected_at=collected_at,
                capture=event_capture_failure(EVENT_CAPTURE_REASON_NOT_CONFIGURED),
            )

        headers = kubernetes_headers(token)
        continuation: str | None = None
        page_count = 0
        resource_version: str | None = None
        facts: dict[str, JsonObject] = {}
        while True:
            if page_count >= KUBERNETES_EVENT_CAPTURE_MAX_PAGES:
                return event_capture_query_result(
                    cluster_id=self.cluster_id,
                    collected_at=collected_at,
                    capture=event_capture_failure(
                        EVENT_CAPTURE_REASON_PAGE_LIMIT,
                        truncated=True,
                        page_count=page_count,
                        event_count=len(facts),
                        resource_version=resource_version,
                    ),
                )
            params: dict[str, str | int] = {"limit": KUBERNETES_EVENT_CAPTURE_PAGE_SIZE}
            if continuation:
                params["continue"] = continuation
            try:
                response = await client.get(
                    f"{base_url}/api/v1/events",
                    headers=headers,
                    params=params,
                )
            except httpx.TimeoutException:
                return event_capture_query_result(
                    cluster_id=self.cluster_id,
                    collected_at=collected_at,
                    capture=event_capture_failure(
                        EVENT_CAPTURE_REASON_TIMEOUT,
                        page_count=page_count,
                        event_count=len(facts),
                        resource_version=resource_version,
                    ),
                )
            except httpx.NetworkError:
                return event_capture_query_result(
                    cluster_id=self.cluster_id,
                    collected_at=collected_at,
                    capture=event_capture_failure(
                        EVENT_CAPTURE_REASON_NETWORK,
                        page_count=page_count,
                        event_count=len(facts),
                        resource_version=resource_version,
                    ),
                )
            if response.status_code in {401, 403}:
                return event_capture_query_result(
                    cluster_id=self.cluster_id,
                    collected_at=collected_at,
                    capture=event_capture_failure(
                        EVENT_CAPTURE_REASON_RBAC_DENIED,
                        page_count=page_count,
                        event_count=len(facts),
                        resource_version=resource_version,
                    ),
                )
            if response.is_error:
                return event_capture_query_result(
                    cluster_id=self.cluster_id,
                    collected_at=collected_at,
                    capture=event_capture_failure(
                        f"http_{response.status_code}",
                        page_count=page_count,
                        event_count=len(facts),
                        resource_version=resource_version,
                    ),
                )
            try:
                page = response.json()
            except ValueError:
                page = None
            if not isinstance(page, dict):
                return event_capture_query_result(
                    cluster_id=self.cluster_id,
                    collected_at=collected_at,
                    capture=event_capture_failure(
                        EVENT_CAPTURE_REASON_INVALID_RESPONSE,
                        page_count=page_count,
                        event_count=len(facts),
                        resource_version=resource_version,
                    ),
                )
            page_items = items(page)
            if len(facts) + len(page_items) > KUBERNETES_EVENT_CAPTURE_MAX_ITEMS:
                return event_capture_query_result(
                    cluster_id=self.cluster_id,
                    collected_at=collected_at,
                    capture=event_capture_failure(
                        EVENT_CAPTURE_REASON_ITEM_LIMIT,
                        truncated=True,
                        page_count=page_count,
                        event_count=len(facts),
                        resource_version=resource_version,
                    ),
                )
            page_metadata = metadata(page)
            current_resource_version = as_text(page_metadata.get("resourceVersion"))
            if current_resource_version:
                resource_version = current_resource_version
            for item in page_items:
                fact = event_timeline_fact(item)
                if fact is None:
                    return event_capture_query_result(
                        cluster_id=self.cluster_id,
                        collected_at=collected_at,
                        capture=event_capture_failure(
                            EVENT_CAPTURE_REASON_INVALID_EVENT,
                            page_count=page_count,
                            event_count=len(facts),
                            resource_version=resource_version,
                        ),
                    )
                facts[str(fact["uid"])] = fact
            page_count += 1
            next_continuation = page_metadata.get("continue")
            continuation = str(next_continuation) if next_continuation else None
            if continuation is None:
                return event_capture_query_result(
                    cluster_id=self.cluster_id,
                    collected_at=collected_at,
                    capture=event_capture_complete(
                        facts=tuple(facts[uid] for uid in sorted(facts)),
                        page_count=page_count,
                        resource_version=resource_version,
                    ),
                )

    async def get_json(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        headers: dict[str, str],
        path: str,
        *,
        allow_not_found: bool = False,
        label_selector: str | None = None,
    ) -> JsonObject:
        """Call one Kubernetes API path and return a JSON object."""
        response = await client.get(
            f"{base_url}{path}",
            headers=headers,
            params={"labelSelector": label_selector} if label_selector else None,
        )
        if allow_not_found and response.status_code == 404:
            return {"items": []}
        if allow_not_found and response.status_code == 403:
            return unavailable_collection_list(K8S_COLLECTION_RBAC_DENIED_REASON)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        return {"items": [], K8S_LIST_MALFORMED_KEY: True}

    def empty_results(self) -> JsonObject:
        """Create an empty Kubernetes evidence bucket for this cluster."""
        return empty_snapshot(self.cluster_id)

    def append_result(
        self,
        results: JsonObject,
        telemetry_query: KubernetesSnapshotQuery,
        payload: JsonObject,
    ) -> None:
        """Normalize one query payload and merge it into the bucket."""
        normalized = self.normalize_payload(payload, telemetry_query)
        merge_snapshot(results, normalized)

    def build_response(self, results: JsonObject) -> JsonObject:
        """Return the finished Kubernetes evidence bucket."""
        return limit_kubernetes_snapshot(results)

    def normalize_payload(
        self,
        payload: JsonObject,
        telemetry_query: KubernetesSnapshotQuery,
    ) -> JsonObject:
        """Turn raw Kubernetes API lists into small evidence summaries."""
        if telemetry_query.is_cluster_api_discovery:
            return self.normalize_cluster_api_discovery(payload, telemetry_query)
        if telemetry_query.is_cluster_access_snapshot:
            return self.normalize_cluster_access_snapshot(payload, telemetry_query)
        if telemetry_query.is_dynamic_resource_collection:
            return self.normalize_dynamic_resource_collection(payload, telemetry_query)
        if telemetry_query.is_cluster_wide_event_capture:
            return self.normalize_cluster_wide_event_capture(payload, telemetry_query)
        snapshot = empty_snapshot(self.cluster_id)
        status = str(payload.get("status") or "success")
        namespace = str(payload.get("namespace") or telemetry_query.namespace or TARGET_NAMESPACE)
        snapshot["cluster"] = {
            "cluster_id": str(payload.get("cluster_id") or self.cluster_id),
            "namespace": namespace,
            "collected_at": str(payload.get("collected_at") or datetime.now(UTC).isoformat()),
        }
        snapshot["collection_scopes"] = [
            {
                "namespace": namespace,
                "label_selector": telemetry_query.label_selector,
            }
        ]
        pod_payload = payload.get(K8S_RESOURCE_PODS)
        event_payload = payload.get(K8S_SNAPSHOT_EVENTS_KEY)
        node_payload = payload.get(K8S_SNAPSHOT_NODES_KEY)
        deployment_payload = payload.get(K8S_RESOURCE_DEPLOYMENTS)
        statefulset_payload = payload.get(K8S_STATEFULSETS_KEY)
        daemonset_payload = payload.get(K8S_DAEMONSETS_KEY)
        replicaset_payload = payload.get(K8S_RESOURCE_REPLICASETS)
        controller_revision_payload = payload.get(K8S_RESOURCE_CONTROLLER_REVISIONS)
        job_payload = payload.get(K8S_JOBS_KEY)
        cronjob_payload = payload.get(K8S_CRONJOBS_KEY)
        service_payload = payload.get(K8S_RESOURCE_SERVICES)
        endpoint_slice_payload = payload.get(K8S_RESOURCE_ENDPOINT_SLICES)
        ingress_payload = payload.get(K8S_INGRESSES_KEY)
        resource_quota_payload = payload.get(K8S_RESOURCE_RESOURCE_QUOTAS)

        pod_metrics = pod_metrics_by_key(items(payload.get("pod_metrics")))
        node_metrics = node_metrics_by_name(items(payload.get("node_metrics")))
        raw_nodes = items(node_payload)
        detected_provider = detect_kubernetes_provider(raw_nodes)
        if detected_provider is not None:
            snapshot["detected_provider"] = detected_provider
        raw_pods = scoped_items(pod_payload, telemetry_query.label_selector)
        raw_replicasets = scoped_items(replicaset_payload, telemetry_query.label_selector)
        raw_controller_revisions = scoped_items(
            controller_revision_payload, telemetry_query.label_selector
        )
        raw_workloads = {
            K8S_KIND_DEPLOYMENT: scoped_items(deployment_payload, telemetry_query.label_selector),
            "StatefulSet": scoped_items(statefulset_payload, telemetry_query.label_selector),
            "DaemonSet": scoped_items(daemonset_payload, telemetry_query.label_selector),
            K8S_KIND_REPLICA_SET: active_replicasets(raw_replicasets),
            "Job": scoped_items(job_payload, telemetry_query.label_selector),
            "CronJob": scoped_items(cronjob_payload, telemetry_query.label_selector),
        }
        raw_services = scoped_items(service_payload, telemetry_query.label_selector)
        raw_ingresses = scoped_items(ingress_payload, telemetry_query.label_selector)
        raw_resource_quotas = scoped_items(resource_quota_payload, telemetry_query.label_selector)
        selected_names = {
            str(metadata(item).get("name") or "")
            for item in [*raw_pods, *(row for rows in raw_workloads.values() for row in rows)]
            if metadata(item).get("name")
        }
        selected_uids = {
            str(metadata(item).get("uid") or "")
            for item in [*raw_pods, *(row for rows in raw_workloads.values() for row in rows)]
            if metadata(item).get("uid")
        }
        snapshot[K8S_RESOURCE_PODS] = [
            pod_summary(
                item,
                pod_metrics.get(
                    (
                        str(metadata(item).get("namespace") or namespace),
                        str(metadata(item).get("name") or ""),
                    )
                ),
            )
            for item in raw_pods
        ]
        snapshot[K8S_SNAPSHOT_EVENTS_KEY] = [
            event_summary(item)
            for item in scoped_events(
                items(event_payload),
                selected_names,
                selected_uids,
                telemetry_query.label_selector,
            )
        ]
        snapshot[K8S_SNAPSHOT_NODES_KEY] = [
            node_summary(item, node_metrics.get(str(metadata(item).get("name") or "")))
            for item in raw_nodes
        ]
        snapshot[K8S_SNAPSHOT_WORKLOADS_KEY] = [
            *(
                summary
                for kind, rows in raw_workloads.items()
                for summary in workload_summaries(
                    kind,
                    rows,
                    revisions=(
                        raw_replicasets
                        if kind == K8S_KIND_DEPLOYMENT
                        else raw_controller_revisions
                        if kind in {"StatefulSet", "DaemonSet"}
                        else []
                    ),
                )
            ),
        ]
        snapshot[K8S_SNAPSHOT_WORKLOAD_REVISIONS_KEY] = [
            *revision_summaries(K8S_KIND_REPLICA_SET, raw_replicasets),
            *revision_summaries("ControllerRevision", raw_controller_revisions),
        ]
        snapshot[K8S_RESOURCE_SERVICES] = [service_summary(item) for item in raw_services]
        snapshot[K8S_INGRESSES_KEY] = [ingress_summary(item) for item in raw_ingresses]
        snapshot[K8S_RESOURCE_RESOURCE_QUOTAS] = [
            resource_quota_summary(item) for item in raw_resource_quotas
        ]
        service_names = {str(metadata(item).get("name") or "") for item in raw_services}
        snapshot[K8S_SNAPSHOT_ENDPOINTS_KEY] = [
            endpoint_slice_summary(item)
            for item in scoped_endpoint_slices(
                items(endpoint_slice_payload),
                service_names,
                telemetry_query.label_selector,
            )
        ]
        collection_status = collection_statuses_from_sources(
            {
                K8S_RESOURCE_PODS: (pod_payload,),
                K8S_SNAPSHOT_EVENTS_KEY: (event_payload,),
                K8S_SNAPSHOT_NODES_KEY: (node_payload,),
                K8S_SNAPSHOT_WORKLOADS_KEY: (
                    deployment_payload,
                    statefulset_payload,
                    daemonset_payload,
                    replicaset_payload,
                    job_payload,
                    cronjob_payload,
                ),
                K8S_SNAPSHOT_WORKLOAD_REVISIONS_KEY: (
                    replicaset_payload,
                    controller_revision_payload,
                ),
                K8S_RESOURCE_SERVICES: (service_payload,),
                K8S_SNAPSHOT_ENDPOINTS_KEY: (endpoint_slice_payload,),
                K8S_INGRESSES_KEY: (ingress_payload,),
                K8S_RESOURCE_RESOURCE_QUOTAS: (resource_quota_payload,),
            }
        )
        if collection_status:
            snapshot[K8S_COLLECTION_STATUS_KEY] = collection_status
        # Only Pods are declared: Workload lists are filtered (active ReplicaSets) and carry
        # no completeness claim, so no other collection gets an entry.
        snapshot[K8S_COLLECTION_COVERAGE_KEY] = [
            pod_collection_coverage(
                pod_payload,
                namespace=namespace,
                label_selector=telemetry_query.label_selector,
                query_status=status,
            )
        ]
        query_status: JsonObject = {
            "status": "partial" if collection_status and status == "success" else status,
            "namespace": namespace,
            "reason": payload.get("reason", ""),
            "counts": {
                K8S_RESOURCE_PODS: len(snapshot[K8S_RESOURCE_PODS]),
                K8S_SNAPSHOT_EVENTS_KEY: len(snapshot[K8S_SNAPSHOT_EVENTS_KEY]),
                K8S_SNAPSHOT_NODES_KEY: len(snapshot[K8S_SNAPSHOT_NODES_KEY]),
                "pod_metrics": len(pod_metrics),
                "node_metrics": len(node_metrics),
                K8S_SNAPSHOT_WORKLOADS_KEY: len(snapshot[K8S_SNAPSHOT_WORKLOADS_KEY]),
                K8S_SNAPSHOT_WORKLOAD_REVISIONS_KEY: len(
                    snapshot[K8S_SNAPSHOT_WORKLOAD_REVISIONS_KEY]
                ),
                K8S_RESOURCE_SERVICES: len(snapshot[K8S_RESOURCE_SERVICES]),
                K8S_SNAPSHOT_ENDPOINTS_KEY: len(snapshot[K8S_SNAPSHOT_ENDPOINTS_KEY]),
                K8S_INGRESSES_KEY: len(snapshot[K8S_INGRESSES_KEY]),
                K8S_RESOURCE_RESOURCE_QUOTAS: len(snapshot[K8S_RESOURCE_RESOURCE_QUOTAS]),
            },
        }
        if collection_status:
            query_status[K8S_COLLECTION_STATUS_KEY] = collection_status
        snapshot["provider_status"] = {telemetry_query.query_name: query_status}
        return snapshot

    def normalize_cluster_access_snapshot(
        self,
        payload: JsonObject,
        telemetry_query: KubernetesSnapshotQuery,
    ) -> JsonObject:
        snapshot = empty_snapshot(self.cluster_id)
        observed_at = str(payload.get("collected_at") or datetime.now(UTC).isoformat())
        source = payload.get(K8S_RESOURCE_ACCESS_KEY)
        normalized = normalize_resource_access(source, observed_at=observed_at)
        snapshot["cluster"] = {
            "cluster_id": str(payload.get("cluster_id") or self.cluster_id),
            "collected_at": observed_at,
        }
        snapshot[K8S_RESOURCE_ACCESS_KEY] = normalized
        snapshot["provider_status"] = {
            telemetry_query.query_name: {
                "status": normalized["completeness"],
                "reason_codes": normalized["reason_codes"],
            }
        }
        return snapshot

    def normalize_cluster_api_discovery(
        self,
        payload: JsonObject,
        telemetry_query: KubernetesSnapshotQuery,
    ) -> JsonObject:
        """Normalize dynamic resources into a bounded, reusable cluster catalog."""
        snapshot = empty_snapshot(self.cluster_id)
        collected_at = str(payload.get("collected_at") or datetime.now(UTC).isoformat())
        documents = payload.get("documents")
        definitions = payload.get("custom_resource_definitions")
        observation = normalize_api_resource_discovery(
            documents=documents if isinstance(documents, list) else [],
            custom_resource_definitions=definitions if isinstance(definitions, list) else None,
            observed_at=collected_at,
            reason_codes=(
                payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else ()
            ),
            truncated=payload.get("truncated") is True,
        ).model_dump(mode="json")
        snapshot["cluster"] = {
            "cluster_id": str(payload.get("cluster_id") or self.cluster_id),
            "collected_at": collected_at,
        }
        snapshot[K8S_API_RESOURCE_DISCOVERY_KEY] = observation
        snapshot["provider_status"] = {
            telemetry_query.query_name: {
                "status": str(payload.get("status") or observation["completeness"]),
                "reason_codes": observation["reason_codes"],
                "resource_count": len(observation["resources"]),
            }
        }
        return snapshot

    def normalize_dynamic_resource_collection(
        self,
        payload: JsonObject,
        telemetry_query: KubernetesSnapshotQuery,
    ) -> JsonObject:
        """Merge one already validated dynamic list into canonical evidence."""

        snapshot = empty_snapshot(self.cluster_id)
        collected_at = str(payload.get("collected_at") or datetime.now(UTC).isoformat())
        collection = payload.get("dynamic_resource_collection")
        observation = dict(collection) if isinstance(collection, dict) else {}
        custom_resources = [
            dict(item)
            for item in payload.get(K8S_CUSTOM_RESOURCES_KEY, [])
            if isinstance(item, dict)
        ]
        snapshot["cluster"] = {
            "cluster_id": str(payload.get("cluster_id") or self.cluster_id),
            "collected_at": collected_at,
        }
        snapshot[K8S_CUSTOM_RESOURCES_KEY] = custom_resources
        snapshot[K8S_DYNAMIC_RESOURCE_COLLECTIONS_KEY] = [observation] if observation else []
        snapshot["provider_status"] = {
            telemetry_query.query_name: {
                "status": observation.get("completeness", "unavailable"),
                "reason_codes": observation.get("reason_codes", ["invalid_response"]),
                "counts": {K8S_CUSTOM_RESOURCES_KEY: len(custom_resources)},
            }
        }
        return snapshot

    def normalize_cluster_wide_event_capture(
        self,
        payload: JsonObject,
        telemetry_query: KubernetesSnapshotQuery,
    ) -> JsonObject:
        """Keep all-namespace Event coverage separate from user-scoped inventory lists."""
        snapshot = empty_snapshot(self.cluster_id)
        collected_at = str(payload.get("collected_at") or datetime.now(UTC).isoformat())
        capture = event_capture_from_payload(payload.get(K8S_EVENT_CAPTURE_KEY))
        snapshot["cluster"] = {
            "cluster_id": str(payload.get("cluster_id") or self.cluster_id),
            "collected_at": collected_at,
        }
        snapshot[K8S_EVENT_CAPTURE_KEY] = capture
        snapshot["provider_status"] = {
            telemetry_query.query_name: {
                "status": str(payload.get("status") or "success"),
                "reason": capture["reason"],
                "event_capture": capture["coverage"],
            }
        }
        return snapshot


def dynamic_resource_discovery_path(group: str, version: str) -> str:
    return f"/apis/{group}/{version}" if group else f"/api/{version}"


def dynamic_resource_list_path(
    descriptor: ApiResourceDescriptor,
    namespace: str | None,
) -> str:
    base = dynamic_resource_discovery_path(descriptor.group, descriptor.version)
    if descriptor.namespaced:
        if not namespace:
            raise ValueError("namespaced dynamic Kubernetes resource requires a namespace")
        return f"{base}/namespaces/{namespace}/{descriptor.name}"
    return f"{base}/{descriptor.name}"


def canonical_dynamic_resource(
    value: object,
    descriptor: ApiResourceDescriptor,
    namespace: str | None,
) -> JsonObject | None:
    """Keep identity, metadata, spec, and status while dropping unsafe top-level data."""

    if not isinstance(value, dict):
        return None
    expected_api_version = (
        f"{descriptor.group}/{descriptor.version}" if descriptor.group else descriptor.version
    )
    if value.get("apiVersion") != expected_api_version or value.get("kind") != descriptor.kind:
        return None
    source_metadata = metadata(value)
    name = as_text(source_metadata.get("name"))
    uid = as_text(source_metadata.get("uid"))
    resource_version = as_text(source_metadata.get("resourceVersion"))
    observed_namespace = as_text(source_metadata.get("namespace"))
    if not name or not uid or not resource_version:
        return None
    if descriptor.namespaced:
        if not namespace or observed_namespace != namespace:
            return None
    elif observed_namespace:
        return None

    canonical_metadata: JsonObject = {
        "name": name,
        "uid": uid,
        "resourceVersion": resource_version,
    }
    if observed_namespace:
        canonical_metadata["namespace"] = observed_namespace
    for key in ("generation", "creationTimestamp", "deletionTimestamp"):
        if source_metadata.get(key) is not None:
            canonical_metadata[key] = source_metadata[key]
    labels = string_mapping(source_metadata.get("labels"))
    annotations = string_mapping(source_metadata.get("annotations"))
    if labels:
        canonical_metadata["labels"] = labels
    if annotations:
        canonical_metadata["annotations"] = annotations
    owner_references = source_metadata.get("ownerReferences")
    if isinstance(owner_references, list):
        canonical_metadata["ownerReferences"] = [
            dict(reference) for reference in owner_references[:16] if isinstance(reference, dict)
        ]

    raw = {
        "apiVersion": expected_api_version,
        "kind": descriptor.kind,
        "metadata": canonical_metadata,
        "spec": spec(value),
        "status": status(value),
    }
    return {
        "api_version": expected_api_version,
        "kind": descriptor.kind,
        "namespace": observed_namespace or None,
        "name": name,
        "uid": uid,
        "resource_version": resource_version,
        "labels": labels,
        "annotations": annotations,
        "raw": raw,
    }


def string_mapping(value: object) -> JsonObject:
    if not isinstance(value, dict):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def dynamic_resource_query_result(
    *,
    cluster_id: str,
    collected_at: str,
    telemetry_query: KubernetesSnapshotQuery,
    descriptor: ApiResourceDescriptor | None,
    resources: list[JsonObject],
    reason_codes: tuple[str, ...],
    page_count: int = 0,
    observed_count: int = 0,
) -> JsonObject:
    spec = telemetry_query.dynamic_resource
    if spec is None:
        group = version = resource = ""
        namespaces: list[str] = []
    else:
        group = spec.group
        version = spec.version
        resource = spec.resource
        namespaces = list(spec.namespaces)
    completeness = "exact" if not reason_codes else "partial" if resources else "unavailable"
    return {
        "status": completeness,
        "cluster_id": cluster_id,
        "collected_at": collected_at,
        K8S_CUSTOM_RESOURCES_KEY: resources,
        "dynamic_resource_collection": {
            "query_name": telemetry_query.query_name,
            "group": group,
            "version": version,
            "resource": resource,
            "kind": descriptor.kind if descriptor is not None else None,
            "namespaced": descriptor.namespaced if descriptor is not None else None,
            "namespaces": namespaces,
            "completeness": completeness,
            "reason_codes": list(reason_codes),
            "page_count": page_count,
            "observed_count": observed_count,
            "returned_count": len(resources),
        },
    }


def empty_snapshot(cluster_id: str) -> JsonObject:
    """Build the empty shape used by Kubernetes evidence."""
    return {
        "cluster": {"cluster_id": cluster_id},
        "collection_scopes": [],
        K8S_SNAPSHOT_WORKLOADS_KEY: [],
        K8S_SNAPSHOT_WORKLOAD_REVISIONS_KEY: [],
        K8S_RESOURCE_PODS: [],
        K8S_SNAPSHOT_EVENTS_KEY: [],
        K8S_SNAPSHOT_NODES_KEY: [],
        K8S_RESOURCE_SERVICES: [],
        K8S_SNAPSHOT_ENDPOINTS_KEY: [],
        K8S_INGRESSES_KEY: [],
        K8S_RESOURCE_RESOURCE_QUOTAS: [],
        K8S_CUSTOM_RESOURCES_KEY: [],
        K8S_DYNAMIC_RESOURCE_COLLECTIONS_KEY: [],
        K8S_RESOURCE_ACCESS_KEY: unavailable_resource_access_payload("not_requested"),
        # A missing global collector is an explicit coverage gap, not an empty
        # all-namespace Event list. Timeline must therefore fail closed.
        K8S_EVENT_CAPTURE_KEY: event_capture_failure(EVENT_CAPTURE_REASON_NOT_REQUESTED),
        "provider_status": {},
    }


def merge_snapshot(target: JsonObject, source: JsonObject) -> None:
    """Add one normalized snapshot into another snapshot."""
    target["cluster"] = {**dict(target.get("cluster", {})), **dict(source.get("cluster", {}))}
    target.setdefault("collection_scopes", [])
    target["collection_scopes"].extend(source.get("collection_scopes", []))
    source_coverage = source.get(K8S_COLLECTION_COVERAGE_KEY)
    if isinstance(source_coverage, list) and source_coverage:
        target.setdefault(K8S_COLLECTION_COVERAGE_KEY, [])
        target[K8S_COLLECTION_COVERAGE_KEY].extend(source_coverage)
    if "detected_provider" not in target and source.get("detected_provider"):
        target["detected_provider"] = source["detected_provider"]
    if isinstance(source.get(K8S_API_RESOURCE_DISCOVERY_KEY), dict):
        target[K8S_API_RESOURCE_DISCOVERY_KEY] = dict(source[K8S_API_RESOURCE_DISCOVERY_KEY])
    source_collection_status = source.get(K8S_COLLECTION_STATUS_KEY)
    if isinstance(source_collection_status, dict):
        target.setdefault(K8S_COLLECTION_STATUS_KEY, {})
        target[K8S_COLLECTION_STATUS_KEY].update(source_collection_status)
    source_access = source.get(K8S_RESOURCE_ACCESS_KEY)
    if isinstance(source_access, dict) and source_access.get("reason_codes") != ["not_requested"]:
        target[K8S_RESOURCE_ACCESS_KEY] = dict(source_access)
    source_event_capture = source.get(K8S_EVENT_CAPTURE_KEY)
    if isinstance(source_event_capture, dict) and source_event_capture.get("reason") != (
        EVENT_CAPTURE_REASON_NOT_REQUESTED
    ):
        target[K8S_EVENT_CAPTURE_KEY] = dict(source_event_capture)
    for key in (
        K8S_SNAPSHOT_WORKLOADS_KEY,
        K8S_SNAPSHOT_WORKLOAD_REVISIONS_KEY,
        K8S_RESOURCE_PODS,
        K8S_SNAPSHOT_EVENTS_KEY,
        K8S_RESOURCE_SERVICES,
        K8S_SNAPSHOT_ENDPOINTS_KEY,
        K8S_INGRESSES_KEY,
        K8S_RESOURCE_RESOURCE_QUOTAS,
        K8S_CUSTOM_RESOURCES_KEY,
    ):
        target.setdefault(key, [])
        target[key].extend(source.get(key, []))
    target.setdefault(K8S_DYNAMIC_RESOURCE_COLLECTIONS_KEY, [])
    target[K8S_DYNAMIC_RESOURCE_COLLECTIONS_KEY].extend(
        source.get(K8S_DYNAMIC_RESOURCE_COLLECTIONS_KEY, [])
    )
    merge_cluster_scoped_nodes(target, source)
    target.setdefault("provider_status", {})
    target["provider_status"].update(source.get("provider_status", {}))


def unavailable_resource_access(
    cluster_id: str,
    observed_at: str,
    reason_code: str,
) -> JsonObject:
    return {
        "status": "unavailable",
        "cluster_id": cluster_id,
        "collected_at": observed_at,
        K8S_RESOURCE_ACCESS_KEY: unavailable_resource_access_payload(
            reason_code,
            observed_at=observed_at,
        ),
    }


def unavailable_resource_access_payload(
    reason_code: str,
    *,
    observed_at: str | None = None,
) -> JsonObject:
    return {
        "completeness": "unavailable",
        "observed_at": observed_at,
        "reason_codes": [reason_code],
        "roles": [],
        "cluster_roles": [],
        "role_bindings": [],
        "cluster_role_bindings": [],
        "service_accounts": [],
        "pod_subjects": [],
    }


def normalize_resource_access(value: object, *, observed_at: str) -> JsonObject:
    if not isinstance(value, dict) or value.get("completeness") != "exact":
        reasons = value.get("reason_codes") if isinstance(value, dict) else None
        reason = (
            str(reasons[0])
            if isinstance(reasons, list) and reasons and isinstance(reasons[0], str)
            else "invalid_access_observation"
        )
        return unavailable_resource_access_payload(reason, observed_at=observed_at)
    try:
        roles = [role_access_summary(item, "Role") for item in _dict_list(value.get("roles"))]
        cluster_roles = [
            role_access_summary(item, "ClusterRole")
            for item in _dict_list(value.get("cluster_roles"))
        ]
        role_bindings = [
            binding_access_summary(item, "RoleBinding")
            for item in _dict_list(value.get("role_bindings"))
        ]
        cluster_role_bindings = [
            binding_access_summary(item, "ClusterRoleBinding")
            for item in _dict_list(value.get("cluster_role_bindings"))
        ]
        service_accounts = [
            required_identity_summary(item) for item in _dict_list(value.get("service_accounts"))
        ]
        pod_subjects = [pod_subject_summary(item) for item in _dict_list(value.get("pod_subjects"))]
    except (TypeError, ValueError):
        return unavailable_resource_access_payload(
            "invalid_access_observation",
            observed_at=observed_at,
        )
    return {
        "completeness": "exact",
        "observed_at": observed_at,
        "reason_codes": [],
        "roles": sorted(roles, key=resource_sort_key),
        "cluster_roles": sorted(cluster_roles, key=resource_sort_key),
        "role_bindings": sorted(role_bindings, key=resource_sort_key),
        "cluster_role_bindings": sorted(cluster_role_bindings, key=resource_sort_key),
        "service_accounts": sorted(service_accounts, key=resource_sort_key),
        "pod_subjects": sorted(pod_subjects, key=resource_sort_key),
    }


def role_access_summary(item: JsonObject, kind: str) -> JsonObject:
    meta = metadata(item)
    return {
        "kind": kind,
        "namespace": "" if kind == "ClusterRole" else _required_text(meta.get("namespace")),
        "name": _required_text(meta.get("name")),
        "rules": [policy_rule_summary(rule) for rule in _nullable_dict_list(item.get("rules"))],
    }


def binding_access_summary(item: JsonObject, kind: str) -> JsonObject:
    meta = metadata(item)
    binding_namespace = (
        "" if kind == "ClusterRoleBinding" else _required_text(meta.get("namespace"))
    )
    role_ref = item.get("roleRef")
    if not isinstance(role_ref, dict):
        raise ValueError("binding roleRef is invalid")
    role_kind = _required_text(role_ref.get("kind"))
    if role_kind not in {"Role", "ClusterRole"}:
        raise ValueError("binding role kind is invalid")
    return {
        "kind": kind,
        "namespace": binding_namespace,
        "name": _required_text(meta.get("name")),
        "roleRef": {
            "kind": role_kind,
            "name": _required_text(role_ref.get("name")),
        },
        "subjects": [
            subject_summary(
                subject,
                default_service_account_namespace=binding_namespace or None,
            )
            for subject in _nullable_dict_list(item.get("subjects"))
        ],
    }


def policy_rule_summary(value: JsonObject) -> JsonObject:
    return {
        "verbs": _string_list(value.get("verbs")),
        "apiGroups": _string_list(value.get("apiGroups")),
        "resources": _string_list(value.get("resources")),
        "resourceNames": _string_list(value.get("resourceNames")),
        "nonResourceURLs": _string_list(value.get("nonResourceURLs")),
    }


def subject_summary(
    value: JsonObject,
    *,
    default_service_account_namespace: str | None = None,
) -> JsonObject:
    kind = _required_text(value.get("kind"))
    if kind not in {"ServiceAccount", "User", "Group"}:
        raise ValueError("binding subject kind is invalid")
    namespace = ""
    if kind == "ServiceAccount":
        raw_namespace = (
            value["namespace"] if "namespace" in value else default_service_account_namespace
        )
        namespace = _required_text(raw_namespace)
    return {
        "kind": kind,
        "namespace": namespace,
        "name": _required_text(value.get("name")),
    }


def required_identity_summary(value: JsonObject) -> JsonObject:
    meta = metadata(value)
    return {
        "namespace": _required_text(meta.get("namespace")),
        "name": _required_text(meta.get("name")),
    }


def pod_subject_summary(value: JsonObject) -> JsonObject:
    identity = required_identity_summary(value)
    pod_uid = _required_text(metadata(value).get("uid"))
    pod_spec = spec(value)
    return {
        "uid": pod_uid,
        **identity,
        "service_account_name": _required_text(pod_spec.get("serviceAccountName")),
    }


def _dict_list(value: object) -> list[JsonObject]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise TypeError("Kubernetes access collection is invalid")
    return value


def _nullable_dict_list(value: object) -> list[JsonObject]:
    if value is None:
        return []
    return _dict_list(value)


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise TypeError("Kubernetes policy string list is invalid")
    return list(value)


def _required_text(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Kubernetes access identity is invalid")
    return value


def event_capture_query_result(
    *,
    cluster_id: str,
    collected_at: str,
    capture: JsonObject,
) -> JsonObject:
    """Build the isolated raw result for the all-namespace Event collector."""
    normalized_capture = dict(capture)
    freshness = dict(normalized_capture.get("freshness") or {})
    freshness["observed_at"] = collected_at
    normalized_capture["freshness"] = freshness
    return {
        "status": "success" if normalized_capture.get("complete") is True else "partial",
        "cluster_id": cluster_id,
        "collected_at": collected_at,
        K8S_EVENT_CAPTURE_KEY: normalized_capture,
    }


def event_capture_complete(
    *,
    facts: tuple[JsonObject, ...],
    page_count: int,
    resource_version: str | None,
) -> JsonObject:
    """Return the only capture shape Timeline is permitted to consume."""
    coverage: JsonObject = {
        "scope": "all_namespaces",
        "pagination": "continue",
        "page_count": page_count,
        "event_count": len(facts),
    }
    if resource_version:
        coverage["resource_version"] = resource_version
    return {
        "complete": True,
        "truncated": False,
        "reason": EVENT_CAPTURE_REASON_COMPLETE,
        "freshness": event_capture_freshness(),
        "coverage": coverage,
        K8S_EVENT_CAPTURE_EVENTS_KEY: list(facts),
    }


def event_capture_failure(
    reason: str,
    *,
    truncated: bool = False,
    page_count: int = 0,
    event_count: int = 0,
    resource_version: str | None = None,
) -> JsonObject:
    """Return safe coverage/gap evidence without a partial fact list."""
    coverage: JsonObject = {
        "scope": "all_namespaces",
        "pagination": "continue",
        "page_count": page_count,
        "event_count": event_count,
        "gap": reason,
    }
    if resource_version:
        coverage["resource_version"] = resource_version
    return {
        "complete": False,
        "truncated": truncated,
        "reason": reason,
        "freshness": event_capture_freshness(),
        "coverage": coverage,
        K8S_EVENT_CAPTURE_EVENTS_KEY: [],
    }


def event_capture_freshness() -> JsonObject:
    """State the maximum safe age explicitly instead of assuming a polling cadence."""
    return {
        "observed_at": datetime.now(UTC).isoformat(),
        "max_age_seconds": KUBERNETES_EVENT_CAPTURE_FRESHNESS_SECONDS,
    }


def event_capture_from_payload(value: object) -> JsonObject:
    """Copy only the declared capture fields when creating evidence output."""
    if not isinstance(value, dict):
        return event_capture_failure(EVENT_CAPTURE_REASON_INVALID_RESPONSE)
    capture = dict(value)
    facts = capture.get(K8S_EVENT_CAPTURE_EVENTS_KEY)
    capture[K8S_EVENT_CAPTURE_EVENTS_KEY] = list(facts) if isinstance(facts, list) else []
    return capture


def event_timeline_fact(item: JsonObject) -> JsonObject | None:
    """Reduce one Kubernetes Event to the safe UID/count/occurrence fact contract."""
    event_metadata = metadata(item)
    uid = as_text(event_metadata.get("uid"))
    name = as_text(event_metadata.get("name"))
    namespace = as_text(event_metadata.get("namespace"))
    series = item.get("series")
    series_body = series if isinstance(series, dict) else {}
    last_occurrence_at = (
        as_text(series_body.get("lastObservedTime"))
        or as_text(item.get("lastTimestamp"))
        or as_text(item.get("eventTime"))
        or as_text(event_metadata.get("creationTimestamp"))
    )
    if not uid or not name or not last_occurrence_at:
        return None
    return compact_dict(
        {
            "uid": uid,
            "api_version": as_text(item.get("apiVersion")) or "v1",
            "namespace": namespace,
            "name": name,
            "resource_version": as_text(event_metadata.get("resourceVersion")),
            "type": as_text(item.get("type")),
            "count": event_occurrence_count(series_body.get("count") or item.get("count")),
            "last_occurrence_at": last_occurrence_at,
        }
    )


def event_occurrence_count(value: object) -> int:
    """Use the Kubernetes Event's count when valid, otherwise its first occurrence."""
    if isinstance(value, bool):
        return 1
    try:
        count = int(value) if value is not None else 1
    except (TypeError, ValueError):
        return 1
    return count if count >= 1 else 1


def merge_cluster_scoped_nodes(target: JsonObject, source: JsonObject) -> None:
    # namespace별 snapshot이 같은 /api/v1/nodes 결과를 반복 수집하므로 node는 cluster scope로 병합한다.
    by_key: dict[str, JsonObject] = {}
    for node in [
        *target.get(K8S_SNAPSHOT_NODES_KEY, []),
        *source.get(K8S_SNAPSHOT_NODES_KEY, []),
    ]:
        if not isinstance(node, dict):
            continue
        key = str(node.get("uid") or node.get("name") or "")
        if not key:
            continue
        by_key[key] = node
    target[K8S_SNAPSHOT_NODES_KEY] = list(by_key.values())


def limit_kubernetes_snapshot(snapshot: JsonObject) -> JsonObject:
    """Limit large Kubernetes lists before the result is sent."""
    limits: JsonObject = {}
    for key, max_items in KUBERNETES_LIST_LIMITS.items():
        group_key = namespace_group_key if key in KUBERNETES_NAMESPACED_LIST_KEYS else None
        limit_payload_list(snapshot, key, max_items, limits, group_key=group_key)
    limit_payload_size(
        snapshot,
        list_keys=KUBERNETES_LIST_LIMITS,
        limits=limits,
        group_keys={key: namespace_group_key for key in KUBERNETES_NAMESPACED_LIST_KEYS},
    )
    mark_dynamic_resource_payload_limit(snapshot, limits)
    if K8S_RESOURCE_PODS in limits:
        invalidate_pod_coverage(snapshot)
    attach_collection_limits(snapshot, limits)
    return snapshot


def mark_dynamic_resource_payload_limit(snapshot: JsonObject, limits: JsonObject) -> None:
    """Never retain exact dynamic coverage after the shared payload limiter truncates it."""

    if K8S_CUSTOM_RESOURCES_KEY not in limits:
        return
    has_resources = bool(snapshot.get(K8S_CUSTOM_RESOURCES_KEY))
    query_names: set[str] = set()
    collections = snapshot.get(K8S_DYNAMIC_RESOURCE_COLLECTIONS_KEY)
    if isinstance(collections, list):
        for collection in collections:
            if not isinstance(collection, dict):
                continue
            query_name = str(collection.get("query_name") or "").strip()
            if query_name:
                query_names.add(query_name)
            reason_codes = dynamic_reason_codes(collection.get("reason_codes"))
            reason_codes.add(DYNAMIC_RESOURCE_REASON_PAYLOAD_LIMIT)
            collection["reason_codes"] = sorted(reason_codes)
            collection["completeness"] = "partial" if has_resources else "unavailable"

    provider_status = snapshot.get("provider_status")
    if not isinstance(provider_status, dict):
        return
    for query_name in query_names:
        status = provider_status.get(query_name)
        if not isinstance(status, dict):
            continue
        reason_codes = dynamic_reason_codes(status.get("reason_codes"))
        reason_codes.add(DYNAMIC_RESOURCE_REASON_PAYLOAD_LIMIT)
        status["reason_codes"] = sorted(reason_codes)
        status["status"] = "partial" if has_resources else "unavailable"


def dynamic_reason_codes(value: object) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {reason for reason in value if isinstance(reason, str) and reason}


def unavailable_collection_list(reason: str) -> JsonObject:
    return {
        "items": [],
        K8S_LIST_COLLECTION_STATUS_KEY: {
            "observed": False,
            "reason_codes": [reason],
        },
    }


def collection_statuses_from_sources(
    sources_by_collection: dict[str, tuple[object, ...]],
) -> JsonObject:
    statuses: JsonObject = {}
    for collection, sources in sources_by_collection.items():
        observed = True
        reason_codes: set[str] = set()
        for source in sources:
            status = collection_status_from_source(source)
            if not status:
                continue
            if status.get("observed") is False:
                observed = False
            reason_codes.update(collection_status_reason_codes(status.get("reason_codes")))
        if not observed or reason_codes:
            statuses[collection] = {
                "observed": observed,
                "reason_codes": sorted(reason_codes),
            }
    return statuses


def collection_status_from_source(source: object) -> JsonObject:
    if not isinstance(source, dict):
        return {}
    status = source.get(K8S_LIST_COLLECTION_STATUS_KEY)
    return status if isinstance(status, dict) else {}


def collection_status_reason_codes(value: object) -> set[str]:
    return dynamic_reason_codes(value) & K8S_COLLECTION_STATUS_REASON_CODES


def pod_collection_coverage(
    pod_payload: object,
    *,
    namespace: str,
    label_selector: str | None,
    query_status: str,
) -> JsonObject:
    """Prove, or refuse to prove, that one namespace Pod list is the whole namespace.

    Only an unfiltered, fully paged, well-formed list whose rows all belong to the
    namespace and carry an identity may justify reading a missing Pod as deleted.  Every
    field is type-checked: a value of the wrong type is a malformed list, never "absent".
    """
    reasons: set[str] = set()
    observed = True
    truncated = False
    if query_status != "success":
        observed = False
        reasons.add(COVERAGE_REASON_QUERY_NOT_SUCCESSFUL)
    if not _well_formed_pod_list(pod_payload):
        observed = False
        reasons.add(COVERAGE_REASON_MALFORMED_LIST)
    else:
        status_reasons = _list_status_reasons(pod_payload)
        if status_reasons is None:
            observed = False
            reasons.add(COVERAGE_REASON_MALFORMED_LIST)
        else:
            reasons.update(status_reasons)
            collector_status = pod_payload.get(K8S_LIST_COLLECTION_STATUS_KEY)
            if isinstance(collector_status, dict) and collector_status["observed"] is False:
                observed = False
        if _list_is_continued(pod_payload):
            truncated = True
            reasons.add(COVERAGE_REASON_CONTINUATION)
        reasons.update(_row_reasons(pod_payload["items"], namespace))
    if label_selector:
        reasons.add(COVERAGE_REASON_LABEL_SELECTOR)
    complete = observed and not truncated and not reasons
    return _pod_coverage_entry(
        namespace,
        label_selector,
        observed=observed,
        complete=complete,
        truncated=truncated,
        reasons=sorted(reasons),
    )


def _pod_coverage_entry(
    namespace: str,
    label_selector: str | None,
    *,
    observed: bool,
    complete: bool,
    truncated: bool,
    reasons: list[str],
) -> JsonObject:
    return {
        "collection": K8S_RESOURCE_PODS,
        "scope": "namespace",
        "namespace": namespace,
        "observed": observed,
        "complete": complete,
        "delete_safe": complete,
        "truncated": truncated,
        "label_selector": label_selector or None,
        "reason_codes": reasons,
    }


def _row_reasons(rows: list[dict[str, Any]], namespace: str) -> set[str]:
    """Reasons a row breaks the proof: no string name and uid, or another namespace."""
    reasons: set[str] = set()
    for row in rows:
        meta = row.get("metadata")
        meta = meta if isinstance(meta, dict) else {}
        for key in ("name", "uid"):
            if not isinstance(meta.get(key), str) or not meta[key]:
                reasons.add(COVERAGE_REASON_IDENTITY_MISSING)
        if meta.get("namespace") != namespace:
            reasons.add(COVERAGE_REASON_NAMESPACE_MISMATCH)
    return reasons


def _well_formed_pod_list(pod_payload: object) -> bool:
    if not isinstance(pod_payload, dict) or pod_payload.get(K8S_LIST_MALFORMED_KEY) is True:
        return False
    rows = pod_payload.get("items")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return False
    if pod_payload.get("kind") not in (None, "PodList"):
        return False
    list_meta = pod_payload.get("metadata")
    if list_meta is not None and not isinstance(list_meta, dict):
        return False
    if list_meta is None:
        return True
    next_token = list_meta.get("continue")
    if next_token is not None and not isinstance(next_token, str):
        return False
    remaining = list_meta.get("remainingItemCount")
    return remaining is None or _is_item_count(remaining)


def _is_item_count(value: object) -> bool:
    """A real count: an integer, never a bool, and never negative."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _list_is_continued(pod_payload: dict[str, Any]) -> bool:
    list_meta = pod_payload.get("metadata") or {}
    remaining = list_meta.get("remainingItemCount")
    return bool(list_meta.get("continue")) or (remaining is not None and remaining > 0)


def _list_status_reasons(pod_payload: dict[str, Any]) -> list[str] | None:
    """Reason codes the collector attached, or ``None`` when its status is malformed."""
    if K8S_LIST_COLLECTION_STATUS_KEY not in pod_payload:
        return []
    status = pod_payload[K8S_LIST_COLLECTION_STATUS_KEY]
    if not isinstance(status, dict) or not isinstance(status.get("observed"), bool):
        return None
    codes = status.get("reason_codes", [])
    if not isinstance(codes, list) or any(not isinstance(code, str) for code in codes):
        return None
    reasons = sorted(set(codes))
    # An observed list that still names a reason (for example RBAC denied) is not whole.
    if status["observed"] is False and not reasons:
        reasons.append(COVERAGE_REASON_QUERY_NOT_SUCCESSFUL)
    return reasons


def invalidate_pod_coverage(snapshot: JsonObject) -> None:
    """A shared list or payload limit may have dropped any namespace's Pods: nothing is proven."""
    entries = snapshot.get(K8S_COLLECTION_COVERAGE_KEY)
    if not isinstance(entries, list):
        return
    for entry in entries:
        if isinstance(entry, dict) and entry.get("collection") == K8S_RESOURCE_PODS:
            entry["complete"] = False
            entry["delete_safe"] = False
            entry["truncated"] = True
            entry["reason_codes"] = sorted(
                {*dynamic_reason_codes(entry.get("reason_codes")), COVERAGE_REASON_PAYLOAD_LIMIT}
            )


def namespace_group_key(item: object) -> str:
    """Return a namespace key so truncation keeps groups represented."""
    if isinstance(item, dict):
        namespace = item.get("namespace")
        if namespace not in (None, ""):
            return str(namespace)
    return "<cluster>"


EVIDENCE_IDENTITY_LABELS = (
    "node.kubernetes.io/instance-type",
    "beta.kubernetes.io/instance-type",
    "topology.kubernetes.io/zone",
    "failure-domain.beta.kubernetes.io/zone",
    "karpenter.sh/capacity-type",
)
LIVE_SCOPED_EVENT_KINDS = frozenset({"Pod", K8S_KIND_REPLICA_SET})


def scoped_items(payload: Any, label_selector: str | None) -> list[JsonObject]:
    rows = items(payload)
    if label_selector:
        key, separator, value = label_selector.partition("=")
        if not separator:
            return []
        return [row for row in rows if resource_labels(row).get(key) == value]
    return rows


def active_replicasets(rows: list[JsonObject]) -> list[JsonObject]:
    """일반 snapshot에서는 현재 replica가 남은 ReplicaSet만 반환한다."""
    active: list[JsonObject] = []
    for row in rows:
        desired = spec(row).get("replicas")
        replica_status = status(row)
        observed = (
            replica_status.get("replicas"),
            replica_status.get("readyReplicas"),
            replica_status.get("availableReplicas"),
        )
        if desired is None or any(has_positive_replica_count(value) for value in observed):
            active.append(row)
            continue
        if has_positive_replica_count(desired):
            active.append(row)
    return active


def has_positive_replica_count(value: object) -> bool:
    count = as_float(value)
    return count is not None and count > 0


def scoped_events(
    rows: list[JsonObject],
    selected_names: set[str],
    selected_uids: set[str],
    label_selector: str | None,
) -> list[JsonObject]:
    scoped: list[JsonObject] = []
    for row in rows:
        involved = row.get("involvedObject")
        involved_body = involved if isinstance(involved, dict) else {}
        kind = str(involved_body.get("kind") or "")
        name = str(involved_body.get("name") or "")
        uid = str(involved_body.get("uid") or "")
        matches_current_resource = name in selected_names or (uid and uid in selected_uids)
        if label_selector:
            if matches_current_resource:
                scoped.append(row)
        elif kind not in LIVE_SCOPED_EVENT_KINDS or matches_current_resource:
            scoped.append(row)
    return scoped


def scoped_endpoint_slices(
    rows: list[JsonObject],
    service_names: set[str],
    label_selector: str | None,
) -> list[JsonObject]:
    if label_selector:
        return [
            row
            for row in rows
            if str(resource_labels(row).get(K8S_ENDPOINT_SLICE_SERVICE_NAME_LABEL) or "")
            in service_names
            or any(
                str(metadata(row).get("name") or "").startswith(f"{service_name}-")
                for service_name in service_names
            )
        ]
    return rows


def resource_labels(item: JsonObject) -> JsonObject:
    """Return all labels for filtering; output compaction belongs to safe_labels()."""
    labels = metadata(item).get("labels", {})
    return labels if isinstance(labels, dict) else {}


def safe_labels(item: JsonObject, limit: int = 12) -> JsonObject:
    """Copy bounded labels while preserving evidence identity labels first."""
    labels = metadata(item).get("labels", {})
    if not isinstance(labels, dict) or limit <= 0:
        return {}
    compact: JsonObject = {}
    for key in EVIDENCE_IDENTITY_LABELS:
        if key in labels and len(compact) < limit:
            compact[key] = str(labels[key])
    for raw_key, raw_value in labels.items():
        key = str(raw_key)
        if key in compact:
            continue
        if len(compact) >= limit:
            break
        compact[key] = str(raw_value)
    return compact


def bounded_label_summary(item: JsonObject) -> JsonObject:
    """Return bounded labels plus an honest completeness bit for downstream facets."""
    all_labels = resource_labels(item)
    labels = safe_labels(item)
    return {"labels": labels, "labels_complete": len(labels) == len(all_labels)}


def owner_ref(item: JsonObject) -> tuple[str | None, str | None]:
    """Return the first owner kind and name for a Kubernetes object."""
    refs = metadata(item).get("ownerReferences", [])
    if not isinstance(refs, list) or not refs:
        return None, None
    ref = refs[0] if isinstance(refs[0], dict) else {}
    return as_text(ref.get("kind")), as_text(ref.get("name"))


def owner_references_complete(item: JsonObject) -> bool:
    """Whether the compact first-owner representation preserved every owner reference."""
    refs = metadata(item).get("ownerReferences", [])
    return isinstance(refs, list) and len(refs) <= 1


def owner_uid(item: JsonObject) -> str | None:
    """Preserve the Kubernetes owner identity needed to survive same-name recreation."""
    refs = metadata(item).get("ownerReferences", [])
    if not isinstance(refs, list) or not refs or not isinstance(refs[0], dict):
        return None
    return as_text(refs[0].get("uid"))


def as_text(value: Any) -> str | None:
    """Turn a value into text while keeping None as None."""
    return str(value) if value is not None else None


def pod_summary(item: JsonObject, metrics: JsonObject | None = None) -> JsonObject:
    """Build a small pod summary for evidence consumers."""
    meta = metadata(item)
    pod_status = status(item)
    pod_spec = spec(item)
    owner_kind, owner_name = owner_ref(item)
    measured = dict(metrics or {})
    cpu_request_mcores, mem_request_mib = pod_request_totals(pod_spec)
    cpu_limit_mcores, mem_limit_mib = pod_limit_totals(pod_spec)
    containers, container_ports_complete = pod_container_summaries(pod_spec, pod_status)
    ephemeral_containers = (
        [
            {"name": name}
            for name in sorted(
                {
                    str(item.get("name") or "")
                    for item in pod_spec.get("ephemeralContainers", [])
                    if isinstance(item, dict) and str(item.get("name") or "")
                }
            )
        ]
        if isinstance(pod_spec.get("ephemeralContainers"), list)
        else []
    )
    return {
        "uid": meta.get("uid"),
        "resource_version": meta.get("resourceVersion"),
        "name": meta.get("name"),
        "namespace": meta.get("namespace"),
        "node_name": pod_spec.get("nodeName"),
        "service_account_name": as_text(pod_spec.get("serviceAccountName")) or None,
        "phase": pod_status.get("phase"),
        "reason": pod_status.get("reason"),
        "message": pod_status.get("message"),
        "start_time": pod_status.get("startTime"),
        **bounded_label_summary(item),
        "owner_kind": owner_kind,
        "owner_name": owner_name,
        "owner_uid": owner_uid(item),
        "owner_references_complete": owner_references_complete(item),
        "workload_key": workload_key(meta.get("namespace"), owner_kind, owner_name),
        "pod_ip": pod_status.get("podIP"),
        "host_ip": pod_status.get("hostIP"),
        "conditions": pod_status.get("conditions", []),
        "containers": containers,
        "ephemeral_containers": ephemeral_containers,
        "container_ports_complete": container_ports_complete,
        "cpu_mcores": measured.get("cpu_mcores"),
        "mem_mib": measured.get("mem_mib"),
        "metrics_observed_at": measured.get("metrics_observed_at"),
        "metrics_window": measured.get("metrics_window"),
        "container_metrics": measured.get("container_metrics", []),
        "container_metrics_complete": measured.get("container_metrics_complete", False),
        "cpu_request_mcores": cpu_request_mcores,
        "cpu_limit_mcores": cpu_limit_mcores,
        "mem_request_mib": mem_request_mib,
        "mem_limit_mib": mem_limit_mib,
        "restart_total": sum(int(container.get("restart_count", 0)) for container in containers),
        "waiting_reasons": [
            container.get("state_reason")
            for container in containers
            if container.get("state") == "waiting" and container.get("state_reason")
        ],
        # 현재 terminated 상태와 직전(lastState) terminated 사유를 함께 승격 —
        # crashloop 중 waiting 으로 관측돼도 OOMKilled 등 크래시 사유가 보존된다.
        "terminated_reasons": [
            *(
                container.get("state_reason")
                for container in containers
                if container.get("state") == "terminated" and container.get("state_reason")
            ),
            *(
                container.get("last_state_reason")
                for container in containers
                if container.get("last_state") == "terminated"
                and container.get("last_state_reason")
            ),
        ],
    }


def resource_quota_summary(item: JsonObject) -> JsonObject:
    """Preserve exact ResourceQuota identity plus observed hard/used quantities."""
    meta = metadata(item)
    quota_status = status(item)
    return {
        "uid": _required_text(meta.get("uid")),
        "resource_version": _required_text(meta.get("resourceVersion")),
        "name": _required_text(meta.get("name")),
        "namespace": _required_text(meta.get("namespace")),
        **bounded_label_summary(item),
        "hard": compact_dict(object_or_empty(quota_status.get("hard"))),
        "used": compact_dict(object_or_empty(quota_status.get("used"))),
    }


def pod_container_summaries(
    pod_spec: JsonObject,
    pod_status: JsonObject,
) -> tuple[list[JsonObject], bool]:
    """Join regular-container status with exact declared ports without inference."""
    spec_containers = pod_spec.get("containers")
    status_containers = pod_status.get("containerStatuses")
    if not isinstance(spec_containers, list):
        spec_containers = []
        complete = False
    else:
        complete = True
    if not isinstance(status_containers, list):
        status_containers = []

    statuses: dict[str, JsonObject] = {}
    for value in status_containers:
        if not isinstance(value, dict):
            complete = False
            continue
        name = as_text(value.get("name"))
        if not name or name in statuses:
            complete = False
            continue
        statuses[name] = value

    result: list[JsonObject] = []
    observed_names: set[str] = set()
    for value in spec_containers:
        if not isinstance(value, dict):
            complete = False
            continue
        name = as_text(value.get("name"))
        if not name or name in observed_names:
            complete = False
            continue
        observed_names.add(name)
        ports, ports_complete = container_port_observations(value)
        complete = complete and ports_complete
        status_summary = container_summary(statuses.get(name, {"name": name}))
        result.append(
            {
                **status_summary,
                "name": name,
                "declared_image": as_text(value.get("image")) or None,
                "ports": ports,
            }
        )

    for name, value in statuses.items():
        if name in observed_names:
            continue
        complete = False
        result.append({**container_summary(value), "name": name, "ports": []})
    return result, complete


def container_port_observations(container: JsonObject) -> tuple[list[JsonObject], bool]:
    """Return validated declared ports and whether every declaration survived."""
    raw_ports = container.get("ports")
    if raw_ports is None:
        return [], True
    if not isinstance(raw_ports, list):
        return [], False
    result: list[JsonObject] = []
    identities: set[tuple[int, str | None, str]] = set()
    complete = True
    for value in raw_ports:
        if not isinstance(value, dict):
            complete = False
            continue
        port = value.get("containerPort")
        protocol = str(value.get("protocol") or "TCP").upper()
        name = as_text(value.get("name"))
        name = name.strip() if name else None
        if (
            isinstance(port, bool)
            or not isinstance(port, int)
            or not 1 <= port <= 65_535
            or protocol not in {"TCP", "UDP", "SCTP"}
            or (name is not None and (not name or len(name) > 63))
        ):
            complete = False
            continue
        identity = (port, name, protocol)
        if identity in identities:
            complete = False
            continue
        identities.add(identity)
        result.append(
            {
                "container_port": port,
                "name": name,
                "protocol": protocol,
            }
        )
    return result, complete


def pod_request_totals(pod_spec: JsonObject) -> tuple[float | None, float | None]:
    """Sum regular-container requests only when an entire resource axis is observed."""
    return pod_resource_totals(pod_spec, "requests")


def pod_limit_totals(pod_spec: JsonObject) -> tuple[float | None, float | None]:
    """Sum regular-container limits only when an entire resource axis is observed."""
    return pod_resource_totals(pod_spec, "limits")


def pod_resource_totals(
    pod_spec: JsonObject,
    bucket: str,
) -> tuple[float | None, float | None]:
    """Sum one declared resource bucket without turning omissions into zero."""
    containers = pod_spec.get("containers")
    if not isinstance(containers, list) or not containers:
        return None, None

    cpu_total = 0.0
    memory_total = 0.0
    cpu_complete = True
    memory_complete = True
    for container in containers:
        if not isinstance(container, dict):
            cpu_complete = False
            memory_complete = False
            continue
        resources = container.get("resources")
        values = resources.get(bucket) if isinstance(resources, dict) else None
        if not isinstance(values, dict):
            cpu_complete = False
            memory_complete = False
            continue

        cpu = parse_cpu_mcores(values.get("cpu"))
        if _positive_finite(cpu):
            cpu_total += cpu
        else:
            cpu_complete = False

        memory = parse_memory_mib(values.get("memory"))
        if _positive_finite(memory):
            memory_total += memory
        else:
            memory_complete = False

    return (
        cpu_total if cpu_complete else None,
        memory_total if memory_complete else None,
    )


def _positive_finite(value: float | None) -> bool:
    return value is not None and math.isfinite(value) and value > 0


def container_summary(item: JsonObject) -> JsonObject:
    """Build a small container status summary from Kubernetes status data."""
    state_name, state_payload = container_state(item, "state")
    # crashloop 파드는 현재 state 가 waiting(CrashLoopBackOff)이고 직전 크래시의
    # 종료 사유/exit code 는 lastState.terminated 에 있다 — RCA 원인 판별
    # (OOMKilled/137 vs exit 1)에 필수라 함께 요약한다.
    last_state_name, last_state_payload = container_state(item, "lastState")
    return {
        "name": item.get("name"),
        "container_id": item.get("containerID"),
        "image": item.get("image"),
        "image_id": item.get("imageID"),
        "ready": item.get("ready"),
        "restart_count": item.get("restartCount", 0),
        "state": state_name,
        "state_reason": state_payload.get("reason"),
        "state_message": state_payload.get("message"),
        "exit_code": state_payload.get("exitCode"),
        "started_at": state_payload.get("startedAt"),
        "finished_at": state_payload.get("finishedAt"),
        "last_state": last_state_name,
        "last_state_reason": last_state_payload.get("reason"),
        "last_state_message": last_state_payload.get("message"),
        "last_exit_code": last_state_payload.get("exitCode"),
        "last_started_at": last_state_payload.get("startedAt"),
        "last_finished_at": last_state_payload.get("finishedAt"),
    }


def container_state(item: JsonObject, key: str) -> tuple[str | None, JsonObject]:
    state = item.get(key, {}) if isinstance(item.get(key), dict) else {}
    state_name = next(iter(state), None)
    state_payload = (
        state.get(state_name, {}) if state_name and isinstance(state.get(state_name), dict) else {}
    )
    return state_name, state_payload if isinstance(state_payload, dict) else {}


def event_summary(item: JsonObject) -> JsonObject:
    """Build a small event summary with reason, message, and target object."""
    meta = metadata(item)
    involved = item.get("involvedObject", {})
    if not isinstance(involved, dict):
        involved = {}
    source = item.get("source", {})
    if not isinstance(source, dict):
        source = {}
    series = item.get("series", {})
    if not isinstance(series, dict):
        series = {}
    last_occurrence_at = (
        series.get("lastObservedTime") or item.get("lastTimestamp") or item.get("eventTime")
    )
    summary = {
        "uid": meta.get("uid"),
        "name": meta.get("name"),
        "namespace": meta.get("namespace"),
        "resource_version": meta.get("resourceVersion"),
        "type": item.get("type"),
        "reason": item.get("reason"),
        "message": item.get("message"),
        "count": series.get("count") or item.get("count"),
        "first_timestamp": item.get("firstTimestamp") or item.get("eventTime"),
        "last_timestamp": last_occurrence_at,
        "last_occurrence_at": last_occurrence_at,
        "reporting_component": item.get("reportingComponent") or source.get("component"),
        "involved_kind": involved.get("kind"),
        "involved_name": involved.get("name"),
        "involved_uid": involved.get("uid"),
        **bounded_label_summary(item),
    }
    reason_summary = event_reason_summary(item)
    if reason_summary:
        summary["reason_summary"] = reason_summary
    return summary


def event_reason_summary(item: JsonObject) -> JsonObject:
    """Build a small reason summary from Kubernetes Event data."""
    reason = as_text(item.get("reason")) or ""
    message = as_text(item.get("message")) or ""
    message_text = message.casefold()
    category, signal, symptom = event_reason_classification(reason, message_text)
    return compact_dict(
        {
            "category": category,
            "signal": signal,
            "symptom": symptom,
            "scheduling_causes": scheduling_causes(message_text)
            if reason == EVENT_REASON_FAILED_SCHEDULING
            else [],
        }
    )


def event_reason_classification(
    reason: str,
    message_text: str,
) -> tuple[str | None, str | None, str | None]:
    """Return a stable RCA hint for known Kubernetes Event reasons."""
    if reason == EVENT_REASON_FAILED_SCHEDULING:
        return (
            EVENT_CATEGORY_SCHEDULING,
            EVENT_SIGNAL_FAILED_SCHEDULING,
            EVENT_SYMPTOM_FAILED_SCHEDULING,
        )
    if reason == EVENT_REASON_OOM_KILLING or "oomkilled" in message_text:
        return (EVENT_CATEGORY_OOM, EVENT_SIGNAL_OOM_KILLED, EVENT_SYMPTOM_CRASH_LOOP)
    if reason == EVENT_REASON_FAILED and (
        "pull image" in message_text or "errimagepull" in message_text
    ):
        return (EVENT_CATEGORY_IMAGE_PULL, EVENT_SIGNAL_ERR_IMAGE_PULL, EVENT_SYMPTOM_IMAGE_PULL)
    if reason == EVENT_REASON_BACK_OFF and "pulling image" in message_text:
        return (
            EVENT_CATEGORY_IMAGE_PULL,
            EVENT_SIGNAL_IMAGE_PULL_BACKOFF,
            EVENT_SYMPTOM_IMAGE_PULL,
        )
    if reason == EVENT_REASON_BACK_OFF and "restarting failed container" in message_text:
        return (
            EVENT_CATEGORY_CONTAINER_RESTART,
            EVENT_SYMPTOM_CRASH_LOOP,
            EVENT_SYMPTOM_CRASH_LOOP,
        )
    if reason == EVENT_REASON_BACK_OFF:
        return (EVENT_CATEGORY_BACKOFF, EVENT_REASON_BACK_OFF, None)
    if reason == EVENT_REASON_UNHEALTHY and "probe" in message_text:
        return (EVENT_CATEGORY_PROBE, probe_signal_label(message_text), EVENT_SYMPTOM_PROBE_FAILURE)
    if reason == EVENT_REASON_FAILED_MOUNT:
        return (EVENT_CATEGORY_CONFIG_MOUNT, EVENT_REASON_FAILED_MOUNT, EVENT_SYMPTOM_FAILED_MOUNT)
    return (None, None, None)


def probe_signal_label(message_text: str) -> str:
    """Return the probe signal type from an Event message."""
    if "readiness probe" in message_text:
        return PROBE_SIGNAL_READINESS
    if "liveness probe" in message_text:
        return PROBE_SIGNAL_LIVENESS
    if "startup probe" in message_text:
        return PROBE_SIGNAL_STARTUP
    return PROBE_SIGNAL_DEFAULT


def scheduling_causes(message_text: str) -> list[str]:
    """Return stable scheduling cause labels from a FailedScheduling message."""
    causes: list[str] = []
    for cause, patterns in SCHEDULING_CAUSE_PATTERNS:
        if any(pattern in message_text for pattern in patterns):
            causes.append(cause)
    return causes


def node_summary(item: JsonObject, metrics: JsonObject | None = None) -> JsonObject:
    """Build a node summary with readiness, taints, and capacity data."""
    node_status = status(item)
    allocatable = node_status.get("allocatable", {}) if isinstance(node_status, dict) else {}
    measured = dict(metrics or {})
    cpu_mcores = as_float(measured.get("cpu_mcores"))
    mem_mib = as_float(measured.get("mem_mib"))
    allocatable_cpu = parse_cpu_mcores(
        allocatable.get("cpu") if isinstance(allocatable, dict) else None
    )
    allocatable_mem = parse_memory_mib(
        allocatable.get("memory") if isinstance(allocatable, dict) else None
    )
    conditions = node_status.get("conditions", [])
    ready_condition = next(
        (
            condition
            for condition in conditions
            if isinstance(condition, dict) and condition.get("type") == "Ready"
        ),
        {},
    )
    meta = metadata(item)
    return {
        "uid": meta.get("uid"),
        "resource_version": meta.get("resourceVersion"),
        "name": meta.get("name"),
        **bounded_label_summary(item),
        "ready": ready_condition.get("status") == "True",
        "conditions": conditions,
        "taints": spec(item).get("taints", []),
        "provider_id": as_text(spec(item).get("providerID")),
        "capacity": node_status.get("capacity", {}),
        "allocatable": allocatable,
        "cpu_mcores": cpu_mcores,
        "mem_mib": mem_mib,
        "metrics_observed_at": measured.get("metrics_observed_at"),
        "metrics_window": measured.get("metrics_window"),
        "cpu_ratio": safe_ratio(cpu_mcores, allocatable_cpu),
        "mem_ratio": safe_ratio(mem_mib, allocatable_mem),
        "node_info": node_status.get("nodeInfo", {}),
    }


def pod_metrics_by_key(rows: list[JsonObject]) -> dict[tuple[str, str], JsonObject]:
    result: dict[tuple[str, str], JsonObject] = {}
    for item in rows:
        meta = metadata(item)
        namespace = str(meta.get("namespace") or "")
        name = str(meta.get("name") or "")
        if namespace and name:
            result[(namespace, name)] = pod_metric_summary(item)
    return result


def node_metrics_by_name(rows: list[JsonObject]) -> dict[str, JsonObject]:
    result: dict[str, JsonObject] = {}
    for item in rows:
        name = str(metadata(item).get("name") or "")
        if name:
            result[name] = metric_usage_summary(item)
    return result


def pod_metric_summary(item: JsonObject) -> JsonObject:
    raw_containers = item.get("containers")
    containers = raw_containers if isinstance(raw_containers, list) else []
    cpu = 0.0
    memory = 0.0
    seen = False
    names: set[str] = set()
    container_metrics: list[JsonObject] = []
    complete = isinstance(raw_containers, list)
    for container in containers:
        if not isinstance(container, dict):
            complete = False
            continue
        name = as_text(container.get("name"))
        if not name or name in names:
            complete = False
            continue
        names.add(name)
        usage = container.get("usage") if isinstance(container.get("usage"), dict) else {}
        cpu_value = parse_cpu_mcores(usage.get("cpu"))
        mem_value = parse_memory_mib(usage.get("memory"))
        if cpu_value is None and mem_value is None:
            complete = False
            continue
        container_metrics.append({"name": name, "cpu_mcores": cpu_value, "mem_mib": mem_value})
        if cpu_value is not None:
            cpu += cpu_value
            seen = True
        if mem_value is not None:
            memory += mem_value
            seen = True
    return {
        "cpu_mcores": cpu if seen else None,
        "mem_mib": memory if seen else None,
        "metrics_observed_at": as_text(item.get("timestamp")),
        "metrics_window": as_text(item.get("window")),
        "container_metrics": sorted(container_metrics, key=lambda value: str(value["name"])),
        "container_metrics_complete": complete and len(container_metrics) == len(containers),
    }


def metric_usage_summary(item: JsonObject) -> JsonObject:
    usage = item.get("usage") if isinstance(item.get("usage"), dict) else {}
    return {
        "cpu_mcores": parse_cpu_mcores(usage.get("cpu")),
        "mem_mib": parse_memory_mib(usage.get("memory")),
        "metrics_observed_at": as_text(item.get("timestamp")),
        "metrics_window": as_text(item.get("window")),
    }


def parse_cpu_mcores(value: Any) -> float | None:
    return cpu_millicores(value)


def parse_memory_mib(value: Any) -> float | None:
    return memory_mebibytes(value)


def safe_ratio(value: float | None, total: float | None) -> float | None:
    if value is None or total is None or total <= 0:
        return None
    return value / total


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def workload_summaries(
    kind: str,
    rows: list[JsonObject],
    *,
    revisions: list[JsonObject] | None = None,
) -> list[JsonObject]:
    """Build workload summaries for all objects of one workload kind."""
    revision_rows = revisions or []
    return [workload_summary(kind, item, revisions=revision_rows) for item in rows]


def workload_summary(
    kind: str,
    item: JsonObject,
    *,
    revisions: list[JsonObject] | None = None,
) -> JsonObject:
    """Build a small workload summary for deployments and similar objects."""
    meta = metadata(item)
    workload_status = status(item)
    owner_kind, owner_name = owner_ref(item)
    summary: JsonObject = {
        "kind": kind,
        "api_version": "batch/v1" if kind in {"Job", "CronJob"} else "apps/v1",
        **bounded_label_summary(item),
        "uid": meta.get("uid"),
        "resource_version": meta.get("resourceVersion"),
        "namespace": meta.get("namespace"),
        "name": meta.get("name"),
        "owner_kind": owner_kind,
        "owner_name": owner_name,
        "owner_uid": owner_uid(item),
        "owner_references_complete": owner_references_complete(item),
        "generation": meta.get("generation"),
        "creation_timestamp": meta.get("creationTimestamp"),
        "observed_generation": workload_status.get("observedGeneration"),
        "desired_replicas": spec(item).get("replicas"),
        "ready_replicas": workload_status.get("readyReplicas", 0),
        "available_replicas": workload_status.get("availableReplicas", 0),
        "updated_replicas": workload_status.get("updatedReplicas", 0),
        "unavailable_replicas": workload_status.get("unavailableReplicas", 0),
        "conditions": workload_status.get("conditions", []),
        "selector": spec(item).get("selector", {}),
        "active": len(workload_status.get("active", []))
        if isinstance(workload_status.get("active"), list)
        else int(workload_status.get("active") or 0),
        "succeeded": int(workload_status.get("succeeded") or 0),
        "failed": int(workload_status.get("failed") or 0),
        "completions": int(spec(item).get("completions") or 1),
        "start_time": workload_status.get("startTime"),
        "completion_time": workload_status.get("completionTime"),
        "scheduled_run_kinds": ["Job"] if kind == "CronJob" else [],
    }
    if kind == "DaemonSet":
        summary.update(
            {
                "desired_replicas": workload_status.get("desiredNumberScheduled"),
                "ready_replicas": workload_status.get("numberReady", 0),
                "available_replicas": workload_status.get("numberAvailable", 0),
                "updated_replicas": workload_status.get("updatedNumberScheduled", 0),
                "unavailable_replicas": workload_status.get("numberUnavailable", 0),
            }
        )
    if kind == "CronJob":
        workload_spec = spec(item)
        summary.update(
            {
                "schedule": workload_spec.get("schedule"),
                "timezone": workload_spec.get("timeZone"),
                "suspend": workload_spec.get("suspend", False),
                "last_schedule_time": workload_status.get("lastScheduleTime"),
            }
        )
    if kind in {K8S_KIND_DEPLOYMENT, "StatefulSet", "DaemonSet"}:
        owned = owned_workload_revisions(item, kind, revisions or [])
        summary.update(
            {
                "pod_template": spec(item).get("template"),
                "revision_history_count": len(owned),
                "revision_history_complete": True,
            }
        )
    return summary


def revision_summaries(kind: str, rows: list[JsonObject]) -> list[JsonObject]:
    return [summary for row in rows if (summary := revision_summary(kind, row)) is not None]


def revision_summary(kind: str, item: JsonObject) -> JsonObject | None:
    meta = metadata(item)
    owner_kind, owner_name = owner_ref(item)
    annotations = meta.get("annotations") if isinstance(meta.get("annotations"), dict) else {}
    revision = (
        annotations.get("deployment.kubernetes.io/revision")
        if kind == K8S_KIND_REPLICA_SET
        else item.get("revision")
    )
    template = (
        spec(item).get("template")
        if kind == K8S_KIND_REPLICA_SET
        else controller_revision_template(item)
    )
    if not all(
        (
            meta.get("uid"),
            meta.get("resourceVersion"),
            meta.get("namespace"),
            meta.get("name"),
            owner_kind,
            owner_name,
            owner_uid(item),
            revision is not None,
            isinstance(template, dict) and bool(template),
        )
    ):
        return None
    return {
        "api_version": "apps/v1",
        "kind": kind,
        "uid": meta["uid"],
        "resource_version": meta["resourceVersion"],
        "namespace": meta["namespace"],
        "name": meta["name"],
        "owner_kind": owner_kind,
        "owner_name": owner_name,
        "owner_uid": owner_uid(item),
        "owner_references_complete": owner_references_complete(item),
        "revision": str(revision),
        "created_at": meta.get("creationTimestamp"),
        "template": template,
    }


def owned_workload_revisions(
    workload: JsonObject,
    workload_kind: str,
    revisions: list[JsonObject],
) -> list[JsonObject]:
    workload_meta = metadata(workload)
    workload_uid = str(workload_meta.get("uid") or "")
    workload_name = str(workload_meta.get("name") or "")
    return [
        revision
        for revision in revisions
        if any(
            str(owner.get("kind") or "") == workload_kind
            and (
                str(owner.get("uid") or "") == workload_uid
                if workload_uid
                else str(owner.get("name") or "") == workload_name
            )
            for owner in (
                metadata(revision).get("ownerReferences")
                if isinstance(metadata(revision).get("ownerReferences"), list)
                else []
            )
            if isinstance(owner, dict)
        )
    ]


def controller_revision_template(item: JsonObject) -> JsonObject:
    data = item.get("data")
    data_object = data if isinstance(data, dict) else {}
    data_spec = data_object.get("spec")
    spec_object = data_spec if isinstance(data_spec, dict) else {}
    template = spec_object.get("template")
    return template if isinstance(template, dict) else {}


def service_summary(item: JsonObject) -> JsonObject:
    """Build a small service summary with ports and selector data."""
    service_spec = spec(item)
    service_status = status(item)
    load_balancer = service_status.get("loadBalancer", {})
    ingress = load_balancer.get("ingress", []) if isinstance(load_balancer, dict) else []
    external_hosts = [
        str(entry.get("hostname") or entry.get("ip"))
        for entry in ingress
        if isinstance(entry, dict) and (entry.get("hostname") or entry.get("ip"))
    ]
    meta = metadata(item)
    return {
        "uid": meta.get("uid"),
        "resource_version": meta.get("resourceVersion"),
        "namespace": meta.get("namespace"),
        "name": meta.get("name"),
        **bounded_label_summary(item),
        "type": service_spec.get("type"),
        "cluster_ip": service_spec.get("clusterIP"),
        "ports": service_spec.get("ports", []),
        "selector": service_spec.get("selector", {}),
        "load_balancer": {"ingress": ingress},
        "external_hosts": external_hosts,
        "external_url": f"http://{external_hosts[0]}" if external_hosts else None,
    }


def ingress_summary(item: JsonObject) -> JsonObject:
    """Build a bounded Ingress summary without exposing annotations or TLS secret data."""
    ingress_spec = spec(item)
    ingress_status = status(item)
    load_balancer = ingress_status.get("loadBalancer", {})
    addresses = load_balancer.get("ingress", []) if isinstance(load_balancer, dict) else []
    external_hosts = [
        str(entry.get("hostname") or entry.get("ip"))
        for entry in addresses
        if isinstance(entry, dict) and (entry.get("hostname") or entry.get("ip"))
    ]
    rules = ingress_spec.get("rules") if isinstance(ingress_spec.get("rules"), list) else []
    hosts = sorted(
        {str(rule.get("host")) for rule in rules if isinstance(rule, dict) and rule.get("host")}
    )
    backend_names: set[str] = set()
    for rule in rules:
        http = rule.get("http") if isinstance(rule, dict) else None
        paths = http.get("paths") if isinstance(http, dict) else None
        for path in paths if isinstance(paths, list) else []:
            backend = path.get("backend") if isinstance(path, dict) else None
            service = backend.get("service") if isinstance(backend, dict) else None
            if isinstance(service, dict) and service.get("name"):
                backend_names.add(str(service["name"]))
    default_backend = ingress_spec.get("defaultBackend")
    default_service = default_backend.get("service") if isinstance(default_backend, dict) else None
    if isinstance(default_service, dict) and default_service.get("name"):
        backend_names.add(str(default_service["name"]))
    meta = metadata(item)
    return {
        "uid": meta.get("uid"),
        "resource_version": meta.get("resourceVersion"),
        "namespace": meta.get("namespace"),
        "name": meta.get("name"),
        **bounded_label_summary(item),
        "ingress_class_name": ingress_spec.get("ingressClassName"),
        "hosts": hosts,
        "backend_service_names": sorted(backend_names),
        "external_hosts": external_hosts,
        "address_count": len(external_hosts),
        "tls_enabled": bool(ingress_spec.get("tls")),
    }


def endpoint_slice_summary(item: JsonObject) -> JsonObject:
    """Build a small endpoint slice summary with endpoint and port counts."""
    endpoint_spec = item
    endpoints = endpoint_spec.get("endpoints")
    ports = endpoint_spec.get("ports")
    meta = metadata(item)
    return {
        "uid": meta.get("uid"),
        "resource_version": meta.get("resourceVersion"),
        "namespace": meta.get("namespace"),
        "name": meta.get("name"),
        **bounded_label_summary(item),
        # Relationship identity is promoted before bounded labels so collection order cannot
        # erase the authoritative EndpointSlice -> Service association.
        "service_name": resource_labels(item).get("kubernetes.io/service-name"),
        "address_type": endpoint_spec.get("addressType"),
        "endpoint_count": len(endpoints) if isinstance(endpoints, list) else 0,
        "ports": ports if isinstance(ports, list) else [],
    }


def workload_key(namespace: Any, kind: str | None, name: str | None) -> str | None:
    """Build a stable workload key when namespace, kind, and name exist."""
    if not namespace or not kind or not name:
        return None
    return f"{namespace}/{kind}/{name}"

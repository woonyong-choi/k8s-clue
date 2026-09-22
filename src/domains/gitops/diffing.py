"""managed-field GitOps diff 헬퍼.

"무엇을 비교할지" 정책을 워커와 분리 — live Kubernetes 객체에서 이 서비스가
관리하기로 한 필드만 남긴다. 3-way 비교와 adoption 판정은 워커에 없었고
호출자도 없어 걷어냈다(`domains.gitops.repository` 가 유일한 사용자).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

JsonObject = dict[str, Any]
MISSING = "<missing>"


@dataclass(frozen=True)
class ManagedFieldSnapshot:
    resource: str
    namespace: str
    fields: JsonObject
    source: str


def resource_ref(kind: str, name: str) -> str:
    return f"{kind.lower()}/{name}"


def snapshot_from_kubernetes_object(obj: Mapping[str, Any], *, source: str) -> ManagedFieldSnapshot:
    metadata = _mapping(obj.get("metadata"))
    kind = str(obj.get("kind", "Unknown"))
    name = str(metadata.get("name", "unknown"))
    namespace = str(metadata.get("namespace", "default"))
    return ManagedFieldSnapshot(
        resource=resource_ref(kind, name),
        namespace=namespace,
        fields=extract_managed_fields(obj),
        source=source,
    )


def extract_managed_fields(obj: Mapping[str, Any]) -> JsonObject:
    kind = str(obj.get("kind", ""))
    if kind == "Deployment":
        return _deployment_fields(obj)
    if kind == "Service":
        return _service_fields(obj)
    if kind == "ConfigMap":
        return _configmap_fields(obj)
    return {}


def _deployment_fields(obj: Mapping[str, Any]) -> JsonObject:
    spec = _mapping(obj.get("spec"))
    fields: JsonObject = {}
    if "replicas" in spec:
        fields["spec.replicas"] = spec["replicas"]

    pod_spec = _mapping(_mapping(_mapping(spec.get("template")).get("spec")))
    containers = pod_spec.get("containers", [])
    if isinstance(containers, list):
        for item in containers:
            container = _mapping(item)
            name = str(container.get("name", "unnamed"))
            prefix = f"spec.template.spec.containers[name={name}]"
            if "image" in container:
                fields[f"{prefix}.image"] = container["image"]
            if "env" in container:
                fields[f"{prefix}.env"] = container["env"]
            if "envFrom" in container:
                fields[f"{prefix}.envFrom"] = container["envFrom"]
            if "resources" in container:
                fields[f"{prefix}.resources"] = container["resources"]
            for probe in ("readinessProbe", "livenessProbe", "startupProbe"):
                if probe in container:
                    fields[f"{prefix}.{probe}"] = container[probe]
    volumes = pod_spec.get("volumes", [])
    if isinstance(volumes, list):
        for item in volumes:
            volume = _mapping(item)
            name = str(volume.get("name", "unnamed"))
            prefix = f"spec.template.spec.volumes[name={name}]"
            if "configMap" in volume:
                fields[f"{prefix}.configMap"] = volume["configMap"]
            if "secret" in volume:
                fields[f"{prefix}.secret"] = volume["secret"]
    return fields


def _service_fields(obj: Mapping[str, Any]) -> JsonObject:
    spec = _mapping(obj.get("spec"))
    fields: JsonObject = {}
    for key in ("type", "selector", "ports"):
        if key in spec:
            fields[f"spec.{key}"] = spec[key]
    return fields


def _configmap_fields(obj: Mapping[str, Any]) -> JsonObject:
    fields: JsonObject = {}
    for key in ("data", "binaryData"):
        if key in obj:
            fields[key] = obj[key]
    return fields


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _value_or_missing(values: Mapping[str, Any] | None, field_path: str) -> Any:
    if values is None or field_path not in values:
        return MISSING
    return values[field_path]

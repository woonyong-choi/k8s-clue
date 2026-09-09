#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

RENDERED_MANIFEST="${TMP_DIR}/clue.yaml"

helm lint "${ROOT_DIR}/charts/clue"
helm template clue "${ROOT_DIR}/charts/clue" \
  --namespace clue-system \
  --set image.tag=local \
  --set console.image.tag=local \
  --set postgresql.auth.password=manifest-check \
  --set agent.token=manifest-check \
  --set initialAdmin.password=manifest-check \
  > "${RENDERED_MANIFEST}"

uv run python - "${RENDERED_MANIFEST}" <<'PY'
from pathlib import Path
import sys

import yaml

manifest = Path(sys.argv[1])
objects = [item for item in yaml.safe_load_all(manifest.read_text()) if item]
if not objects:
    raise SystemExit(f"{manifest}: no Kubernetes objects rendered")
for index, item in enumerate(objects, start=1):
    missing = [
        field
        for field, value in (
            ("apiVersion", item.get("apiVersion")),
            ("kind", item.get("kind")),
            ("metadata.name", (item.get("metadata") or {}).get("name")),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"{manifest}: document {index} missing {', '.join(missing)}")

forbidden = {
    ("", "pods/exec"),
    ("", "pods/attach"),
    ("", "pods/portforward"),
    ("", "nodes/proxy"),
}
read_only_verbs = {"get", "list", "watch"}
for item in objects:
    if item.get("kind") not in {"Role", "ClusterRole"}:
        continue
    for rule in item.get("rules") or []:
        groups = rule.get("apiGroups") or [""]
        resources = rule.get("resources") or []
        if any((group, resource) in forbidden for group in groups for resource in resources):
            raise SystemExit(
                f"{item['metadata']['name']}: forbidden direct-execution RBAC resource"
            )
        verbs = set(rule.get("verbs") or [])
        if not verbs or not verbs <= read_only_verbs:
            raise SystemExit(
                f"{item['metadata']['name']}: only get/list/watch RBAC verbs are allowed"
            )
        if "*" in groups or "*" in resources or "*" in verbs:
            raise SystemExit(f"{item['metadata']['name']}: wildcard RBAC is forbidden")

print(f"rendered Kubernetes objects: {len(objects)}")
PY

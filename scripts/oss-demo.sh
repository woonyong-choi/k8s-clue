#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

# ── 데모 플래그 ────────────────────────────────────────────────────────────
# DEMO_SKIP_PR=1 (기본): 실제 GitHub Draft PR을 만들지 않는다. Draft PR 장면은
#   계약 테스트(모킹된 GitOps 경로)만 실행한다. 토큰 없이 CI에서 돌리기 위한 값.
# DEMO_SKIP_PR=0: 실제 PR을 만들려는 의도이므로 GITHUB_TOKEN을 요구한다.
# DEMO_DRY_RUN=1: 하위 호환 별칭 (make demo DEMO_DRY_RUN=1).
DEMO_DRY_RUN="${DEMO_DRY_RUN:-}"
if [[ "${DEMO_DRY_RUN}" == "1" ]]; then
  DEMO_SKIP_PR="${DEMO_SKIP_PR:-1}"
fi
DEMO_SKIP_PR="${DEMO_SKIP_PR:-1}"

# DEMO_KIND_CONTEXT: 비어 있지 않으면 해당 kubectl context에서 실제 kind 장면을
#   실행한다. 비어 있으면 kind 장면을 건너뛴다(로컬 pytest 전용 실행).
DEMO_KIND_CONTEXT="${DEMO_KIND_CONTEXT:-}"
DEMO_KIND_NAMESPACE="${DEMO_KIND_NAMESPACE:-clue-demo}"

scene() {
  echo "[demo] $1"
}

skip() {
  echo "[demo][skip] $1"
}

# ── 장면 0: kind 위에서 ImagePullBackOff를 실제로 재현 ─────────────────────
kind_scene() {
  if [[ -z "${DEMO_KIND_CONTEXT}" ]]; then
    skip "kind scene (DEMO_KIND_CONTEXT unset)"
    return 0
  fi
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "[demo][error] DEMO_KIND_CONTEXT is set but kubectl is missing" >&2
    return 1
  fi

  scene "kind cluster reproduces ImagePullBackOff"
  local kc=(kubectl --context "${DEMO_KIND_CONTEXT}")

  "${kc[@]}" create namespace "${DEMO_KIND_NAMESPACE}" \
    --dry-run=client -o yaml | "${kc[@]}" apply -f -

  "${kc[@]}" -n "${DEMO_KIND_NAMESPACE}" apply -f - <<'MANIFEST'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: clue-demo-broken
  labels:
    app.kubernetes.io/name: clue-demo-broken
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: clue-demo-broken
  template:
    metadata:
      labels:
        app.kubernetes.io/name: clue-demo-broken
    spec:
      containers:
        - name: app
          # 존재하지 않는 태그 — ImagePullBackOff를 의도적으로 유발한다
          image: ghcr.io/woonyong-choi/clue-demo-nonexistent:v0.0.0-missing
MANIFEST

  # 최대 120초 동안 ImagePullBackOff/ErrImagePull 상태를 기다린다
  local waited=0
  local reason=""
  while (( waited < 120 )); do
    reason="$("${kc[@]}" -n "${DEMO_KIND_NAMESPACE}" get pods \
      -l app.kubernetes.io/name=clue-demo-broken \
      -o jsonpath='{.items[*].status.containerStatuses[*].state.waiting.reason}' 2>/dev/null || true)"
    if [[ "${reason}" == *ImagePullBackOff* || "${reason}" == *ErrImagePull* ]]; then
      break
    fi
    sleep 5
    waited=$(( waited + 5 ))
  done

  if [[ "${reason}" != *ImagePullBackOff* && "${reason}" != *ErrImagePull* ]]; then
    echo "[demo][error] expected ImagePullBackOff within ${waited}s, got: '${reason}'" >&2
    "${kc[@]}" -n "${DEMO_KIND_NAMESPACE}" describe pods \
      -l app.kubernetes.io/name=clue-demo-broken >&2 || true
    return 1
  fi
  echo "[demo] observed waiting reason: ${reason} (after ${waited}s)"

  "${kc[@]}" delete namespace "${DEMO_KIND_NAMESPACE}" --wait=false >/dev/null
}

kind_scene

scene "ImagePullBackOff evidence and deterministic RCA"
uv run pytest -q \
  tests/test_alertmanager_alert_event.py \
  tests/test_recovery_gitops_authority.py

if [[ "${DEMO_SKIP_PR}" == "1" ]]; then
  scene "base-SHA-pinned GitOps Draft PR (contract only, no live PR)"
else
  if [[ -z "${GITHUB_TOKEN:-}" ]]; then
    echo "[demo][error] DEMO_SKIP_PR=0 requires GITHUB_TOKEN for live Draft PR creation" >&2
    exit 1
  fi
  scene "base-SHA-pinned GitOps Draft PR"
fi
uv run pytest -q \
  tests/test_recovery_pr_lifecycle.py \
  tests/test_safe_pr_structured_base_advance.py

scene "post-deploy evidence verification"
uv run pytest -q tests/test_recovery_verification.py

scene "Golden Path contract verified"
uv run pytest -q tests/test_golden_path_safety_contracts.py

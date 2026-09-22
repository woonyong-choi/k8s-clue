#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

scene() {
  echo "[demo] $1"
}

scene "ImagePullBackOff evidence and deterministic RCA"
uv run pytest -q \
  tests/test_alertmanager_alert_event.py \
  tests/test_recovery_gitops_authority.py

scene "base-SHA-pinned GitOps Draft PR"
uv run pytest -q \
  tests/test_recovery_pr_lifecycle.py \
  tests/test_safe_pr_structured_base_advance.py

scene "post-deploy evidence verification"
uv run pytest -q tests/test_recovery_verification.py

scene "Golden Path contract verified"
uv run pytest -q tests/test_golden_path_safety_contracts.py

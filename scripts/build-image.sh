#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_SLUG="${PROJECT_SLUG:-clue}"
IMAGE_NAME="${IMAGE_NAME:-${PROJECT_SLUG}:local}"

docker build -f "${ROOT_DIR}/src/services/Dockerfile" -t "${IMAGE_NAME}" "${ROOT_DIR}"

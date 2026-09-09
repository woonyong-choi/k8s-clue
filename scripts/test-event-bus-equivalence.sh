#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NATS_IMAGE="${NATS_EQUIVALENCE_IMAGE:-nats:2.10-alpine}"
CONTAINER_NAME="clue-event-bus-equivalence-$$"

cleanup() {
  docker rm --force "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null || {
  echo "missing required command: docker" >&2
  exit 1
}
docker info >/dev/null
docker run --detach --rm \
  --name "${CONTAINER_NAME}" \
  --publish 127.0.0.1::4222 \
  "${NATS_IMAGE}" -js >/dev/null

mapping="$(docker port "${CONTAINER_NAME}" 4222/tcp)"
port="${mapping##*:}"
for _ in $(seq 1 60); do
  if python3 - "${port}" <<'PY'
import socket
import sys

with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.25):
    pass
PY
  then
    break
  fi
  sleep 0.25
done

if ! docker logs "${CONTAINER_NAME}" 2>&1 | grep -q "Server is ready"; then
  docker logs "${CONTAINER_NAME}" >&2
  exit 1
fi

cd "${ROOT_DIR}"
PYTHONPATH="${ROOT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}" \
  uv run python scripts/event_bus_equivalence.py \
  --nats-url "nats://127.0.0.1:${port}"

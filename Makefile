SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

IMAGE_NAME ?= clue:local
ENV_TEMPLATE ?= config/env/app.env.example

export IMAGE_NAME

.PHONY: help setup setup-hooks env sync hooks doctor lint format test manifest-check product-brand-boundary-check gate gate-backend gate-frontend gate-fast events services event-bus-equivalence build-image demo clean

help: ## 사용 가능한 명령어 출력
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  %-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: env sync setup-hooks ## 최초 개발 환경 준비

env: ## 로컬 .env 생성
	@if [[ -f .env ]]; then \
		echo ".env already exists"; \
	else \
		cp "$(ENV_TEMPLATE)" .env; \
		echo "created .env"; \
	fi

sync: ## Python 의존성 설치/동기화
	uv sync

doctor: ## 로컬 필수 도구 점검
	bash scripts/doctor.sh

lint: ## Python lint
	uv run ruff check .

format: ## Python format
	uv run ruff format .

hooks: ## pre-commit과 pre-push hook 설치
	uv run pre-commit install --hook-type pre-commit --hook-type pre-push

setup-hooks: hooks ## commit message gate 설치
	@hook_path="$$(git rev-parse --git-path hooks)/commit-msg"; \
	mkdir -p "$$(dirname "$$hook_path")"; \
	printf '%s\n' '#!/usr/bin/env sh' 'exec "$$(git rev-parse --show-toplevel)/scripts/commit-msg-gate.sh" "$$1"' > "$$hook_path"; \
	chmod +x "$$hook_path"; \
	echo "installed $$hook_path"

test: ## Backend lint와 pytest
	bash scripts/test.sh

manifest-check: ## Helm과 Kubernetes manifest 검증
	bash scripts/manifest-check.sh

product-brand-boundary-check: ## 과거 제품명과 개인 운영 경계 검사
	node scripts/verify-product-brand-boundary.mjs

gate-backend: ## Backend와 manifest 전체 gate
	bash scripts/test.sh
	bash scripts/manifest-check.sh

gate-frontend: ## Frontend 전체 gate
	cd frontend && npm ci --include=dev --no-audit --no-fund
	cd frontend && npm run check
	test -s frontend/dist/index.html

gate: gate-backend gate-frontend ## 저장소 전체 gate

gate-fast: ## pre-push 빠른 정적 검사
	uv run python -m compileall -q src scripts
	uv run ruff check .
	cd frontend && npm run typecheck

events: ## 등록된 이벤트/구독자 출력
	uv run python scripts/events.py

services: ## 기본 runtime 서비스 출력
	uv run python scripts/services.py

event-bus-equivalence: ## in-process/NATS 결과 동등성
	bash scripts/test-event-bus-equivalence.sh

build-image: ## 로컬 container image 빌드
	bash scripts/build-image.sh

demo: ## Kind ImagePullBackOff → Draft PR → 검증 데모
	bash -c "DEMO_DRY_RUN='$(DEMO_DRY_RUN)' bash scripts/oss-demo.sh"

clean: ## 재생성 가능한 캐시와 빌드 산출물 삭제
	rm -rf -- .pytest_cache .ruff_cache .import_linter_cache .playwright-cli
	rm -rf -- frontend/.playwright-cli frontend/dist
	find alembic src tests scripts -type d -name __pycache__ -prune -exec rm -rf -- {} +

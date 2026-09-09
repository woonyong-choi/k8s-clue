"""GitHub App *manifest flow* — 운영자 1회 등록을 거의 원클릭으로.

흐름:
  1. 프론트가 ``build_app_manifest`` 로 만든 manifest 를 GitHub 새 App 생성
     페이지로 폼 POST 한다(운영자는 값 타이핑 없이 "Create" 만 누른다).
  2. GitHub 이 임시 ``code`` 를 달아 ``redirect_url`` 로 되돌린다.
  3. ``convert_manifest_code`` 로 code 를 교환해 App ID·slug·개인키·웹훅시크릿을
     **자동 수신**한다(운영자가 복사할 필요 없음).
  4. ``store_app_config_from_conversion`` 이 암호화해 workspace_credentials 에
     저장한다(마이그레이션 불필요). 이후 ``resolve_github_app_config`` 가 그 값을
     읽어 즉시 ``configured`` 가 된다(env 는 폴백).

개인키·시크릿은 Fernet 로 암호화해 저장하며 응답/로그로 노출하지 않는다.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from domains.scm.github_app import GithubAppConfig, load_github_app_config
from packages.config.settings import env
from packages.contracts.gitops import DEFAULT_GITHUB_API_BASE, GITHUB_API_BASE_ENV
from packages.security.credentials import decrypt_credential, encrypt_credential

APP_CONFIG_PROVIDER = "github-app"
APP_CONFIG_SCOPE = "platform"

_DEFAULT_APP_NAME = "Clue GitOps"
_HTTP_TIMEOUT = 20.0


def build_app_manifest(
    *,
    base_url: str,
    name: str | None = None,
    homepage_url: str | None = None,
) -> dict[str, Any]:
    """GitHub 에 그대로 넘길 App manifest.

    ``base_url`` 은 GitHub 이 웹훅/콜백을 보낼 **공개** 백엔드 주소다.
    """
    base = base_url.rstrip("/")
    return {
        "name": (name or _DEFAULT_APP_NAME).strip() or _DEFAULT_APP_NAME,
        "url": homepage_url or base,
        "hook_attributes": {"url": f"{base}/github/webhook", "active": True},
        "redirect_url": f"{base}/api/integrations/github/app/manifest/callback",
        "callback_urls": [f"{base}/api/integrations/github/app/callback"],
        "setup_url": f"{base}/api/integrations/github/app/callback",
        "setup_on_update": True,
        "public": False,
        "default_permissions": {
            "contents": "write",
            "pull_requests": "write",
            "metadata": "read",
        },
        # installation·installation_repositories 는 App 이 자동 수신하는 관리 이벤트라
        # default_events 로 구독하면 GitHub 이 거부한다(권한 매핑 없음). 빼도 자동 수신됨.
        # 여기엔 권한에 매핑되는 이벤트만 둔다: push→contents, pull_request→pull_requests,
        # repository→metadata.
        "default_events": [
            "push",
            "pull_request",
            "repository",
        ],
    }


def new_app_action_url(*, org: str | None, state: str) -> str:
    """manifest 를 POST 할 GitHub 새 App 생성 URL(조직/개인)."""
    from urllib.parse import quote

    query = f"?state={quote(state)}" if state else ""
    if org:
        return f"https://github.com/organizations/{quote(org)}/settings/apps/new{query}"
    return f"https://github.com/settings/apps/new{query}"


async def convert_manifest_code(
    code: str,
    *,
    api_base: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """임시 code 를 App 자격증명으로 교환한다.

    반환: ``{id, slug, client_id, client_secret, pem, webhook_secret, html_url, ...}``.
    """
    base = (api_base or env(GITHUB_API_BASE_ENV, DEFAULT_GITHUB_API_BASE)).rstrip("/")
    url = f"{base}/app-manifests/{code}/conversions"
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    owns = client is None
    http = client or httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
    try:
        resp = await http.post(url, headers=headers)
        resp.raise_for_status()
        return resp.json()
    finally:
        if owns:
            await http.aclose()


def store_app_config_from_conversion(
    db: Any,
    workspace_id: str,
    conversion: dict[str, Any],
) -> None:
    """교환 결과를 암호화해 workspace_credentials 에 저장한다(비밀은 encrypted_value)."""
    upsert = getattr(db, "upsert_workspace_credential", None)
    if not callable(upsert):
        raise RuntimeError("workspace credential store unavailable")
    secret_blob = json.dumps(
        {
            "app_id": str(conversion.get("id") or ""),
            "slug": str(conversion.get("slug") or ""),
            "client_id": str(conversion.get("client_id") or ""),
            "private_key": str(conversion.get("pem") or ""),
            "webhook_secret": str(conversion.get("webhook_secret") or ""),
        }
    )
    upsert(
        {
            "workspace_id": workspace_id,
            "provider": APP_CONFIG_PROVIDER,
            "scope": APP_CONFIG_SCOPE,
            "encrypted_value": encrypt_credential(secret_blob),
            "metadata": {
                # 비밀 아님 — 빠른 표시/판정용
                "app_id": str(conversion.get("id") or ""),
                "slug": str(conversion.get("slug") or ""),
                "html_url": str(conversion.get("html_url") or ""),
            },
        }
    )


def resolve_github_app_config(db: Any, workspace_id: str) -> GithubAppConfig:
    """DB 저장 App 자격증명을 우선 사용하고, 없으면 env 로 폴백한다."""
    getter = getattr(db, "get_workspace_credential", None)
    if callable(getter):
        stored = getter(workspace_id, APP_CONFIG_PROVIDER, APP_CONFIG_SCOPE)
        if stored and stored.get("encrypted_value"):
            try:
                data = json.loads(decrypt_credential(str(stored["encrypted_value"])))
            except Exception:  # noqa: BLE001 - 손상 시 env 폴백
                data = None
            if data and data.get("app_id") and data.get("private_key"):
                api_base = env(GITHUB_API_BASE_ENV, DEFAULT_GITHUB_API_BASE).rstrip("/")
                return GithubAppConfig(
                    app_id=str(data["app_id"]),
                    private_key=str(data["private_key"]),
                    slug=str(data.get("slug") or ""),
                    client_id=str(data.get("client_id") or ""),
                    api_base=api_base,
                )
    return load_github_app_config()


def resolve_webhook_secret(db: Any, workspace_id: str) -> str:
    """저장된 웹훅 시크릿(없으면 env)."""
    getter = getattr(db, "get_workspace_credential", None)
    if callable(getter):
        stored = getter(workspace_id, APP_CONFIG_PROVIDER, APP_CONFIG_SCOPE)
        if stored and stored.get("encrypted_value"):
            try:
                data = json.loads(decrypt_credential(str(stored["encrypted_value"])))
                if data.get("webhook_secret"):
                    return str(data["webhook_secret"])
            except Exception:  # noqa: BLE001
                pass
    from domains.scm.github_app import webhook_secret

    return webhook_secret()

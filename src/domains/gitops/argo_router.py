"""Session-protected Argo CD Application connection for one deployment binding."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field, SecretStr

from domains.gitops.argo_observation import (
    ARGOCD_PROVIDER,
    ArgoApplicationClient,
    ArgoApplicationTarget,
    ArgoObservationError,
    argo_credential_scope,
)
from domains.identity.dependencies import require_session, resolve_allowed_cluster_ids
from packages.config.settings import env
from packages.contracts.gateway import routes as gateway_routes
from packages.contracts.gateway.base import StrictModel
from packages.contracts.identity import Permission
from packages.runtime.dependencies import get_db
from packages.security.credentials import (
    CredentialEncryptionError,
    credential_ref,
    encrypt_credential,
)
from packages.storage.engine import unit_of_work_or_null

router = APIRouter()


class ArgoBindingRequest(StrictModel):
    project: str = Field(min_length=1, max_length=253, pattern=r"^[a-z0-9][a-z0-9.-]*$")
    application: str = Field(min_length=1, max_length=253, pattern=r"^[a-z0-9][a-z0-9.-]*$")
    token: SecretStr = Field(min_length=1, max_length=4096)


@router.put(gateway_routes.ARGOCD_BINDING_PATH)
async def configure_argocd_binding(
    binding_id: str,
    payload: ArgoBindingRequest,
    current: Any = Depends(require_session),
    db: Any = Depends(get_db),
) -> dict[str, Any]:
    workspace_id = str(getattr(current, "workspace_id", "") or "")
    allowed_clusters = resolve_allowed_cluster_ids(
        db, current, workspace_id, Permission.APPLICATION_MANAGE.value
    )
    if not allowed_clusters:
        raise HTTPException(status_code=403, detail="resource access denied")
    targets = db.list_active_github_poll_targets(workspace_id, binding_id=binding_id)
    if len(targets) != 1 or str(targets[0]["cluster_id"]) not in allowed_clusters:
        raise HTTPException(status_code=404, detail="deployment binding unavailable")
    server_url = env("ARGOCD_SERVER_URL", "").strip()
    if not server_url:
        raise HTTPException(status_code=503, detail="argocd server is not configured")
    target = targets[0]
    token = payload.token.get_secret_value()
    try:
        observed = await ArgoApplicationClient(token).observe(
            ArgoApplicationTarget(
                server_url=server_url,
                project=payload.project,
                application=payload.application,
                repo_ref=str(target["repo_ref"]),
                manifest_path=str(target["manifest_path"]),
            )
        )
    except ArgoObservationError as exc:
        status_code = 503 if exc.retryable else 422
        raise HTTPException(status_code=status_code, detail=exc.reason) from exc
    scope = argo_credential_scope(binding_id)
    ref = credential_ref(ARGOCD_PROVIDER, scope)
    try:
        encrypted = encrypt_credential(token)
        with unit_of_work_or_null(db):
            db.lock_workspace_credential_scope(workspace_id, ARGOCD_PROVIDER, scope)
            db.upsert_workspace_credential(
                {
                    "workspace_id": workspace_id,
                    "provider": ARGOCD_PROVIDER,
                    "scope": scope,
                    "encrypted_value": encrypted,
                    "metadata": {"credential_ref": ref, "binding_id": binding_id},
                }
            )
            db.configure_argocd_binding(
                workspace_id, binding_id, payload.project, payload.application
            )
    except CredentialEncryptionError as exc:
        raise HTTPException(status_code=503, detail="credential store unavailable") from exc
    return {
        "binding_id": binding_id,
        "project": payload.project,
        "application": payload.application,
        "sync_revision": observed.sync_revision,
        "sync_status": observed.sync_status,
        "health_status": observed.health_status,
    }

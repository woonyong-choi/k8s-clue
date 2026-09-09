"""Initialize the single-controller OSS database and its self-observing agent identity."""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GATEWAY_DIR = ROOT / "src" / "services" / "gateway" / "api-gateway"
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
if str(GATEWAY_DIR) not in sys.path:
    sys.path.insert(0, str(GATEWAY_DIR))

from passwords import default_display_name, hash_password, normalize_email  # noqa: E402

from domains.identity.dependencies import hash_agent_token  # noqa: E402
from packages.contracts.identity import DEFAULT_WORKSPACE_ID  # noqa: E402
from packages.storage.database import Database  # noqa: E402


def bootstrap() -> None:
    email = normalize_email(os.environ["AUTH_EMAIL"])
    password = os.environ["AUTH_PASSWORD"]
    agent_token = os.environ["OSS_AGENT_TOKEN"]
    cluster_id = os.environ.get("OSS_AGENT_CLUSTER_ID", "kyro-self").strip()
    if not password or not agent_token or not cluster_id:
        raise ValueError("OSS bootstrap credentials and cluster id must be non-empty")

    user_id = "user-" + str(uuid.uuid5(uuid.NAMESPACE_URL, f"kyro:{email}"))
    db = Database()
    db.init()
    db.upsert_admin_account(
        user_id=user_id,
        email=email,
        password_hash=hash_password(password),
        display_name=default_display_name(email),
    )
    db.register_target_cluster(
        {
            "workspace_id": DEFAULT_WORKSPACE_ID,
            "user_id": user_id,
            "cluster_id": cluster_id,
            "name": "Clue self cluster",
            "environment": "oss",
            "status": "registered",
            "agent_token_hash": hash_agent_token(agent_token),
            "settings": {
                "provider": "kind",
                "cluster_role": "management",
                "install_source": "helm",
            },
        }
    )


if __name__ == "__main__":
    bootstrap()

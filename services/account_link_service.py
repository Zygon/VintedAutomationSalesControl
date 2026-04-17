from __future__ import annotations

import random
import string
from datetime import datetime, timezone
from typing import Any

from services.access_service import get_role_for_account
from services.firestore_queries import (
    get_account_by_id,
    get_link_code_by_code,
    get_user_account_by_id,
    list_link_code_rows,
    list_user_account_rows,
    upsert_account_link_code,
    upsert_user_account,
)


class AccountLinkError(Exception):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_code(raw: str) -> str:
    value = (raw or "").strip().upper().replace(" ", "")
    if "-" in value:
        return value
    if len(value) == 8:
        return f"{value[:4]}-{value[4:]}"
    return value


def _generate_code_value() -> str:
    alphabet = string.ascii_uppercase + string.digits
    left = "".join(random.choices(alphabet, k=4))
    right = "".join(random.choices(alphabet, k=4))
    return f"{left}-{right}"


def list_link_codes_for_account(account_id: str) -> list[dict[str, Any]]:
    return list_link_code_rows(account_id)


def create_link_code(
    user_id: str,
    account_id: str,
    max_uses: int = 5,
    expires_at: str | None = None,
) -> dict[str, Any]:
    role = get_role_for_account(user_id, account_id)
    if role != "OWNER":
        raise AccountLinkError("Só um OWNER pode gerar códigos de associação.")

    account = get_account_by_id(account_id)
    if not account:
        raise AccountLinkError("A conta operacional não existe.")

    if account.get("status") != "ACTIVE":
        raise AccountLinkError("A conta operacional não está ativa.")

    for _ in range(20):
        code = _generate_code_value()
        existing = get_link_code_by_code(code)
        if existing:
            continue

        now_iso = utc_now_iso()
        payload = {
            "code": code,
            "accountId": account_id,
            "status": "ACTIVE",
            "createdAt": now_iso,
            "updatedAt": now_iso,
            "expiresAt": expires_at,
            "maxUses": int(max_uses),
            "usedCount": 0,
            "createdByUserId": user_id,
        }
        upsert_account_link_code(code, payload)
        return payload

    raise AccountLinkError("Não foi possível gerar um código único.")


def _parse_expires_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def claim_link_code(user_id: str, code: str) -> dict[str, Any]:
    normalized_code = _normalize_code(code)
    if not normalized_code:
        raise AccountLinkError("Código inválido.")

    code_data = get_link_code_by_code(normalized_code)
    if not code_data:
        raise AccountLinkError("Código não encontrado.")

    if code_data.get("status") != "ACTIVE":
        raise AccountLinkError("Código inativo.")

    expires_at = _parse_expires_at(code_data.get("expiresAt"))
    if expires_at and expires_at < datetime.now(timezone.utc):
        raise AccountLinkError("Código expirado.")

    max_uses = int(code_data.get("maxUses", 1))
    used_count = int(code_data.get("usedCount", 0))
    if used_count >= max_uses:
        raise AccountLinkError("Código esgotado.")

    account_id = code_data.get("accountId")
    if not account_id:
        raise AccountLinkError("Código sem account associada.")

    account = get_account_by_id(str(account_id))
    if not account:
        raise AccountLinkError("A conta operacional associada não existe.")

    if account.get("status") != "ACTIVE":
        raise AccountLinkError("A conta operacional não está ativa.")

    rel_id = f"{user_id}__{account_id}"
    existing_rel = get_user_account_by_id(rel_id)

    now_iso = utc_now_iso()

    if existing_rel and existing_rel.get("status") == "ACTIVE":
        raise AccountLinkError("Esta conta já está associada ao utilizador.")

    existing_active = list_user_account_rows(user_id, only_active=True)
    has_any_active = len(existing_active) > 0

    rel_payload = {
        "id": rel_id,
        "userAccountId": rel_id,
        "userId": user_id,
        "accountId": account_id,
        "role": "VIEWER",
        "isDefault": not has_any_active,
        "status": "ACTIVE",
        "createdAt": now_iso if not existing_rel else existing_rel.get("createdAt", now_iso),
        "updatedAt": now_iso,
        "linkedByCode": normalized_code,
    }

    upsert_user_account(rel_id, rel_payload)
    upsert_account_link_code(normalized_code, {
        "usedCount": used_count + 1,
        "updatedAt": now_iso,
    })

    return rel_payload


def revoke_link_code(user_id: str, account_id: str, code: str):
    role = get_role_for_account(user_id, account_id)
    if role != "OWNER":
        raise AccountLinkError("Só um OWNER pode revogar códigos.")

    normalized_code = _normalize_code(code)
    data = get_link_code_by_code(normalized_code)

    if not data:
        raise AccountLinkError("Código não encontrado.")

    if data.get("accountId") != account_id:
        raise AccountLinkError("Esse código não pertence à conta selecionada.")

    upsert_account_link_code(normalized_code, {
        "status": "REVOKED",
        "updatedAt": utc_now_iso(),
    })
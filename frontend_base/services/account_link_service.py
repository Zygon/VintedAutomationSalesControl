from __future__ import annotations

import random
import string
from datetime import datetime, timezone
from typing import Any

from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from services.access_service import get_role_for_account
from services.firestore_queries import get_db


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
    db = get_db()
    docs = (
        db.collection("accountLinkCodes")
        .where(filter=FieldFilter("accountId", "==", account_id))
        .stream()
    )

    rows: list[dict[str, Any]] = []
    for doc in docs:
        data = doc.to_dict() or {}
        data.setdefault("id", doc.id)
        rows.append(data)

    rows.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return rows


def create_link_code(
    user_id: str,
    account_id: str,
    max_uses: int = 5,
    expires_at: str | None = None,
) -> dict[str, Any]:
    role = get_role_for_account(user_id, account_id)
    if role != "OWNER":
        raise AccountLinkError("Só um OWNER pode gerar códigos de associação.")

    db = get_db()
    account_doc = db.collection("accounts").document(account_id).get()
    if not account_doc.exists:
        raise AccountLinkError("A conta operacional não existe.")

    if (account_doc.to_dict() or {}).get("status") != "ACTIVE":
        raise AccountLinkError("A conta operacional não está ativa.")

    for _ in range(20):
        code = _generate_code_value()
        ref = db.collection("accountLinkCodes").document(code)
        if not ref.get().exists:
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
            ref.set(payload)
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

    db = get_db()
    code_ref = db.collection("accountLinkCodes").document(normalized_code)
    code_doc = code_ref.get()

    if not code_doc.exists:
        raise AccountLinkError("Código não encontrado.")

    code_data = code_doc.to_dict() or {}

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

    account_doc = db.collection("accounts").document(account_id).get()
    if not account_doc.exists:
        raise AccountLinkError("A conta operacional associada não existe.")

    account_data = account_doc.to_dict() or {}
    if account_data.get("status") != "ACTIVE":
        raise AccountLinkError("A conta operacional não está ativa.")

    rel_id = f"{user_id}__{account_id}"
    rel_ref = db.collection("userAccounts").document(rel_id)
    existing_rel = rel_ref.get()

    now_iso = utc_now_iso()

    if existing_rel.exists:
        rel_data = existing_rel.to_dict() or {}
        if rel_data.get("status") == "ACTIVE":
            raise AccountLinkError("Esta conta já está associada ao utilizador.")

    existing_active = (
        db.collection("userAccounts")
        .where(filter=FieldFilter("userId", "==", user_id))
        .where(filter=FieldFilter("status", "==", "ACTIVE"))
        .stream()
    )
    has_any_active = any(True for _ in existing_active)

    rel_payload = {
        "userAccountId": rel_id,
        "userId": user_id,
        "accountId": account_id,
        "role": "VIEWER",
        "isDefault": not has_any_active,
        "status": "ACTIVE",
        "createdAt": now_iso,
        "updatedAt": now_iso,
        "linkedByCode": normalized_code,
    }

    batch = db.batch()
    batch.set(rel_ref, rel_payload, merge=True)
    batch.set(code_ref, {
        "usedCount": used_count + 1,
        "updatedAt": now_iso,
    }, merge=True)
    batch.commit()

    return rel_payload


def revoke_link_code(user_id: str, account_id: str, code: str):
    role = get_role_for_account(user_id, account_id)
    if role != "OWNER":
        raise AccountLinkError("Só um OWNER pode revogar códigos.")

    normalized_code = _normalize_code(code)
    db = get_db()
    ref = db.collection("accountLinkCodes").document(normalized_code)
    doc = ref.get()

    if not doc.exists:
        raise AccountLinkError("Código não encontrado.")

    data = doc.to_dict() or {}
    if data.get("accountId") != account_id:
        raise AccountLinkError("Esse código não pertence à conta selecionada.")

    ref.set({
        "status": "REVOKED",
        "updatedAt": utc_now_iso(),
    }, merge=True)
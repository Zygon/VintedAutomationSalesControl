from __future__ import annotations

from datetime import datetime, timezone

from google.cloud.firestore_v1.base_query import FieldFilter

from services.firestore_queries import get_db


class AccountManagementError(Exception):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def link_account_to_user(user_id: str, account_id: str):
    db = get_db()

    # 1. validar se account existe
    account_doc = db.collection("accounts").document(account_id).get()
    if not account_doc.exists:
        raise AccountManagementError("Conta não existe.")

    account_data = account_doc.to_dict() or {}
    if account_data.get("status") != "ACTIVE":
        raise AccountManagementError("Conta não está ativa.")

    # 2. verificar se já existe ligação
    existing = (
        db.collection("userAccounts")
        .where(filter=FieldFilter("userId", "==", user_id))
        .where(filter=FieldFilter("accountId", "==", account_id))
        .where(filter=FieldFilter("status", "==", "ACTIVE"))
        .stream()
    )

    if any(True for _ in existing):
        raise AccountManagementError("Conta já associada ao utilizador.")

    # 3. verificar se é a primeira conta → default
    existing_any = (
        db.collection("userAccounts")
        .where(filter=FieldFilter("userId", "==", user_id))
        .where(filter=FieldFilter("status", "==", "ACTIVE"))
        .stream()
    )

    is_first = not any(True for _ in existing_any)

    # 4. criar ligação
    rel_id = f"{user_id}__{account_id}"
    now_iso = utc_now_iso()

    db.collection("userAccounts").document(rel_id).set({
        "userAccountId": rel_id,
        "userId": user_id,
        "accountId": account_id,
        "role": "OWNER",
        "isDefault": is_first,
        "status": "ACTIVE",
        "createdAt": now_iso,
        "updatedAt": now_iso,
    })

    return rel_id
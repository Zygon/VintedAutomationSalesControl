from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from google.cloud.firestore_v1.base_query import FieldFilter

from services.firestore_queries import get_db


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_user_accounts(user_id: str) -> list[dict[str, Any]]:
    db = get_db()

    rel_docs = (
        db.collection("userAccounts")
        .where(filter=FieldFilter("userId", "==", user_id))
        .where(filter=FieldFilter("status", "==", "ACTIVE"))
        .stream()
    )

    relations: list[dict[str, Any]] = []
    for doc in rel_docs:
        data = doc.to_dict() or {}
        data.setdefault("id", doc.id)
        relations.append(data)

    relations.sort(key=lambda x: (not bool(x.get("isDefault")), x.get("accountId", "")))
    return relations


def get_allowed_account_ids(user_id: str) -> list[str]:
    relations = get_user_accounts(user_id)
    return [r["accountId"] for r in relations if r.get("accountId")]


def get_allowed_accounts(user_id: str) -> list[dict[str, Any]]:
    db = get_db()
    relations = get_user_accounts(user_id)

    result: list[dict[str, Any]] = []
    for rel in relations:
        account_id = rel.get("accountId")
        if not account_id:
            continue

        doc = db.collection("accounts").document(account_id).get()
        if not doc.exists:
            continue

        account = doc.to_dict() or {}
        if account.get("status") != "ACTIVE":
            continue

        result.append({
            "accountId": account.get("accountId") or doc.id,
            "emailAddress": account.get("emailAddress") or doc.id,
            "status": account.get("status"),
            "role": rel.get("role", "VIEWER"),
            "isDefault": bool(rel.get("isDefault")),
        })

    result.sort(key=lambda x: (not x["isDefault"], x["emailAddress"]))
    return result


def get_role_for_account(user_id: str, account_id: str) -> str | None:
    relations = get_user_accounts(user_id)
    for rel in relations:
        if rel.get("accountId") == account_id:
            return rel.get("role", "VIEWER")
    return None


def can_manage_account(user_id: str, account_id: str) -> bool:
    role = get_role_for_account(user_id, account_id)
    return role in {"OWNER", "MANAGER"}


def can_view_account(user_id: str, account_id: str) -> bool:
    role = get_role_for_account(user_id, account_id)
    return role in {"OWNER", "MANAGER", "VIEWER"}


def set_default_account(user_id: str, account_id: str):
    db = get_db()
    relations = get_user_accounts(user_id)
    now_iso = utc_now_iso()

    batch = db.batch()
    found = False

    for rel in relations:
        rel_id = rel.get("userAccountId") or rel.get("id")
        if not rel_id:
            continue

        ref = db.collection("userAccounts").document(rel_id)
        is_target = rel.get("accountId") == account_id
        if is_target:
            found = True

        batch.set(ref, {
            "isDefault": is_target,
            "updatedAt": now_iso,
        }, merge=True)

    if not found:
        raise ValueError("Conta não associada ao utilizador.")

    batch.commit()


def remove_user_account(user_id: str, account_id: str):
    db = get_db()
    relations = get_user_accounts(user_id)
    now_iso = utc_now_iso()

    target = None
    for rel in relations:
        if rel.get("accountId") == account_id:
            target = rel
            break

    if not target:
        raise ValueError("Conta não associada ao utilizador.")

    rel_id = target.get("userAccountId") or target.get("id")
    if not rel_id:
        raise ValueError("Ligação inválida.")

    db.collection("userAccounts").document(rel_id).set({
        "status": "REVOKED",
        "isDefault": False,
        "updatedAt": now_iso,
    }, merge=True)

    remaining = [
        rel for rel in relations
        if rel.get("accountId") != account_id
    ]

    if target.get("isDefault") and remaining:
        first_remaining = remaining[0]
        remaining_id = first_remaining.get("userAccountId") or first_remaining.get("id")
        if remaining_id:
            db.collection("userAccounts").document(remaining_id).set({
                "isDefault": True,
                "updatedAt": now_iso,
            }, merge=True)
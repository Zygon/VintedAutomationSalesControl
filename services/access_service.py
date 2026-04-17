from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.firestore_queries import (
    get_account_by_id,
    list_user_account_rows,
    revoke_user_account_link,
    set_default_user_account,
    upsert_user_account,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_user_accounts(user_id: str) -> list[dict[str, Any]]:
    return list_user_account_rows(user_id, only_active=True)


def get_allowed_account_ids(user_id: str) -> list[str]:
    relations = get_user_accounts(user_id)
    return [str(r["accountId"]) for r in relations if r.get("accountId")]


def get_allowed_accounts(user_id: str) -> list[dict[str, Any]]:
    relations = get_user_accounts(user_id)

    result: list[dict[str, Any]] = []
    for rel in relations:
        account_id = rel.get("accountId")
        if not account_id:
            continue

        account = get_account_by_id(str(account_id))
        if not account:
            continue

        if str(account.get("status")) != "ACTIVE":
            continue

        result.append({
            "accountId": account.get("accountId") or account.get("id"),
            "emailAddress": account.get("emailAddress") or account.get("accountId") or account.get("id"),
            "status": account.get("status"),
            "role": rel.get("role", "VIEWER"),
            "isDefault": bool(rel.get("isDefault")),
        })

    result.sort(key=lambda x: (not x["isDefault"], str(x["emailAddress"])))
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
    set_default_user_account(user_id, account_id)


def remove_user_account(user_id: str, account_id: str):
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

    revoke_user_account_link(str(rel_id))

    remaining = [
        rel for rel in relations
        if rel.get("accountId") != account_id
    ]

    if target.get("isDefault") and remaining:
        first_remaining = remaining[0]
        remaining_id = first_remaining.get("userAccountId") or first_remaining.get("id")
        if remaining_id:
            upsert_user_account(str(remaining_id), {
                "isDefault": True,
                "updatedAt": now_iso,
            })
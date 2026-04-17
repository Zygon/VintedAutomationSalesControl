from __future__ import annotations

from datetime import datetime, timezone

from services.firestore_queries import (
    get_account_by_id,
    list_user_account_rows,
    upsert_user_account,
)


class AccountManagementError(Exception):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def link_account_to_user(user_id: str, account_id: str):
    account = get_account_by_id(account_id)

    if not account:
        raise AccountManagementError("Conta não existe.")

    if account.get("status") != "ACTIVE":
        raise AccountManagementError("Conta não está ativa.")

    existing = list_user_account_rows(user_id, only_active=True)
    for row in existing:
        if str(row.get("accountId")) == str(account_id):
            raise AccountManagementError("Conta já associada ao utilizador.")

    is_first = len(existing) == 0

    rel_id = f"{user_id}__{account_id}"
    now_iso = utc_now_iso()

    upsert_user_account(rel_id, {
        "id": rel_id,
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
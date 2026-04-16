from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import streamlit as st

from services.firestore_queries import get_db


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_logged_in() -> bool:
    return bool(st.user.is_logged_in)


def login():
    st.login()


def logout():
    for key in [
        "_bootstrapped_user",
        "_bootstrapped_user_id",
        "allowed_accounts",
        "_allowed_accounts_user_id",
        "allowed_account_ids",
        "selected_account_id",
        "selected_account_role",
    ]:
        if key in st.session_state:
            del st.session_state[key]

    st.logout()


def get_current_identity() -> dict[str, Any] | None:
    if not is_logged_in():
        return None

    user = st.user

    user_id = getattr(user, "sub", None) or getattr(user, "email", None)
    email = getattr(user, "email", None)
    display_name = getattr(user, "name", None) or email
    photo_url = getattr(user, "picture", None)

    if not user_id or not email:
        return None

    return {
        "userId": str(user_id),
        "email": str(email).strip().lower(),
        "displayName": display_name,
        "photoURL": photo_url,
        "provider": "google_oidc",
    }


def bootstrap_user(force_refresh: bool = False) -> dict[str, Any] | None:
    identity = get_current_identity()
    if not identity:
        return None

    cached_user = st.session_state.get("_bootstrapped_user")
    cached_user_id = st.session_state.get("_bootstrapped_user_id")

    if not force_refresh and cached_user and cached_user_id == identity["userId"]:
        return cached_user

    db = get_db()
    user_ref = db.collection("users").document(identity["userId"])
    existing = user_ref.get()

    now_iso = utc_now_iso()

    if existing.exists:
        existing_data = existing.to_dict() or {}

        result = {
            "userId": identity["userId"],
            "email": identity["email"],
            "displayName": identity["displayName"],
            "photoURL": identity["photoURL"],
            "provider": identity["provider"],
            "status": existing_data.get("status", "ACTIVE"),
            "globalRole": existing_data.get("globalRole", "USER"),
            "createdAt": existing_data.get("createdAt"),
            "updatedAt": now_iso,
            "lastLoginAt": now_iso,
        }

        # write mínimo, só merge simples
        user_ref.set(
            {
                "email": identity["email"],
                "displayName": identity["displayName"],
                "photoURL": identity["photoURL"],
                "provider": identity["provider"],
                "updatedAt": now_iso,
                "lastLoginAt": now_iso,
            },
            merge=True,
        )
    else:
        result = {
            "userId": identity["userId"],
            "email": identity["email"],
            "displayName": identity["displayName"],
            "photoURL": identity["photoURL"],
            "provider": identity["provider"],
            "status": "ACTIVE",
            "globalRole": "USER",
            "createdAt": now_iso,
            "updatedAt": now_iso,
            "lastLoginAt": now_iso,
        }

        user_ref.set(result, merge=True)

    st.session_state["_bootstrapped_user"] = result
    st.session_state["_bootstrapped_user_id"] = identity["userId"]

    return result
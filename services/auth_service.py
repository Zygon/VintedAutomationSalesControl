from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import streamlit as st


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_user_dict() -> dict[str, Any]:
    try:
        data = st.user.to_dict()
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def is_logged_in() -> bool:
    user_data = _safe_user_dict()
    return bool(user_data.get("is_logged_in", False))


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
    user_data = _safe_user_dict()

    if not user_data.get("is_logged_in", False):
        return None

    user_id = user_data.get("sub") or user_data.get("email")
    email = user_data.get("email")
    display_name = user_data.get("name") or email
    photo_url = user_data.get("picture")

    if not user_id or not email:
        return None

    return {
        "userId": str(user_id),
        "email": str(email).strip().lower(),
        "displayName": display_name,
        "photoURL": photo_url,
        "provider": "google_oidc",
        "status": "ACTIVE",
        "globalRole": "USER",
        "lastLoginAt": utc_now_iso(),
    }


def bootstrap_user(force_refresh: bool = False) -> dict[str, Any] | None:
    """
    NÃO bate no Firestore no arranque.
    Resolve o utilizador autenticado só a partir do st.user
    e guarda-o em session_state.

    Isto evita que a app morra logo no login por causa de Firebase.
    """
    identity = get_current_identity()
    if not identity:
        return None

    cached_user = st.session_state.get("_bootstrapped_user")
    cached_user_id = st.session_state.get("_bootstrapped_user_id")

    if not force_refresh and cached_user and cached_user_id == identity["userId"]:
        return cached_user

    result = {
        "userId": identity["userId"],
        "email": identity["email"],
        "displayName": identity["displayName"],
        "photoURL": identity["photoURL"],
        "provider": identity["provider"],
        "status": identity.get("status", "ACTIVE"),
        "globalRole": identity.get("globalRole", "USER"),
        "createdAt": st.session_state.get("_bootstrapped_user_created_at") or utc_now_iso(),
        "updatedAt": utc_now_iso(),
        "lastLoginAt": utc_now_iso(),
    }

    st.session_state["_bootstrapped_user"] = result
    st.session_state["_bootstrapped_user_id"] = identity["userId"]
    st.session_state["_bootstrapped_user_created_at"] = result["createdAt"]

    return result
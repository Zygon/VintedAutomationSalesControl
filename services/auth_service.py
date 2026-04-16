from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import streamlit as st

from services.firestore_queries import get_db


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_user_dict() -> dict[str, Any]:
    """
    Lê st.user da forma mais defensiva possível.
    Quando a auth do Streamlit não está configurada/carregada,
    st.user pode existir mas não ter atributos utilizáveis.
    """
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

    # Neste ponto, o teu código atual assume Firebase.
    # Quando mudares mesmo para Postgres no auth bootstrap,
    # adaptas aqui. Para já, não vou inventar e partir mais coisas.
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
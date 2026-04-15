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
    # Usa o provider default configurado em [auth]
    st.login()


def logout():
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


def bootstrap_user() -> dict[str, Any] | None:
    identity = get_current_identity()
    if not identity:
        return None

    db = get_db()
    user_ref = db.collection("users").document(identity["userId"])
    existing = user_ref.get()

    now_iso = utc_now_iso()

    base_payload = {
        "userId": identity["userId"],
        "email": identity["email"],
        "displayName": identity["displayName"],
        "photoURL": identity["photoURL"],
        "provider": identity["provider"],
        "status": "ACTIVE",
        "updatedAt": now_iso,
        "lastLoginAt": now_iso,
    }

    if existing.exists:
        existing_data = existing.to_dict() or {}
        user_ref.set(base_payload, merge=True)
        return {
            **base_payload,
            "globalRole": existing_data.get("globalRole", "USER"),
        }

    user_ref.set(
        {
            **base_payload,
            "globalRole": "USER",
            "createdAt": now_iso,
        },
        merge=True,
    )

    return {
        **base_payload,
        "globalRole": "USER",
    }
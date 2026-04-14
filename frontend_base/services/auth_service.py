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
    st.login("google")


def logout():
    st.logout()


def get_current_identity() -> dict[str, Any] | None:
    if not is_logged_in():
        return None

    # Streamlit expõe o utilizador autenticado em st.user
    # atributos típicos: email, name, sub, picture
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
        user_ref.set(base_payload, merge=True)
    else:
        user_ref.set(
            {
                **base_payload,
                "globalRole": "USER",
                "createdAt": now_iso,
            },
            merge=True,
        )

    return {**base_payload, "globalRole": "USER" if not existing.exists else (existing.to_dict() or {}).get("globalRole", "USER")}
from __future__ import annotations

import streamlit as st

from services.auth_service import bootstrap_user, is_logged_in, login, logout


def require_login() -> dict:
    if not is_logged_in():
        st.title("Login necessário")
        st.write("Tens de iniciar sessão com Google para usar esta app.")

        if st.button("Entrar com Google", type="primary"):
            login()

        st.stop()

    user = bootstrap_user()
    if not user:
        st.error("Não foi possível resolver os dados do utilizador autenticado.")
        if st.button("Sair"):
            logout()
        st.stop()

    return user


def render_user_box(user: dict):
    with st.sidebar:
        st.markdown("### Utilizador")
        st.write(user.get("displayName") or user.get("email"))
        st.caption(user.get("email"))

        if st.button("Logout"):
            logout()
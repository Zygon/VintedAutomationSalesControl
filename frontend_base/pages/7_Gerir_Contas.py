from __future__ import annotations

import pandas as pd
import streamlit as st

from components.auth_guard import render_user_box, require_login
from components.filters import render_global_filters
from components.tables import render_dataframe

from services.access_service import (
    get_user_accounts,
    set_default_account,
    remove_user_account,
)
from services.account_management_service import (
    link_account_to_user,
    AccountManagementError,
)

user = require_login()
render_user_box(user)
render_global_filters(user)

st.title("Gerir Contas Operacionais")

tab1, tab2 = st.tabs(["Associar Conta", "As Minhas Contas"])

# ----------------------------
# 🔹 TAB 1 — Associar Conta
# ----------------------------
with tab1:
    st.subheader("Associar nova conta operacional")

    with st.form("link_account_form", clear_on_submit=True):
        account_id = st.text_input(
            "Email / Account ID",
            placeholder="ex: beaver3dcrafts@gmail.com"
        )

        submitted = st.form_submit_button("Associar")

        if submitted:
            try:
                link_account_to_user(
                    user_id=user["userId"],
                    account_id=account_id.strip().lower(),
                )
                st.success("Conta associada com sucesso.")
                st.rerun()
            except AccountManagementError as exc:
                st.error(str(exc))

# ----------------------------
# 🔹 TAB 2 — As Minhas Contas
# ----------------------------
with tab2:
    st.subheader("Contas associadas")

    user_accounts = get_user_accounts(user["userId"])

    rows = []
    for rel in user_accounts:
        rows.append({
            "accountId": rel.get("accountId"),
            "role": rel.get("role"),
            "isDefault": rel.get("isDefault"),
            "status": rel.get("status"),
            "createdAt": rel.get("createdAt"),
        })

    df = pd.DataFrame(rows)
    render_dataframe(df)

    if not df.empty:
        selected_account = st.selectbox(
            "Seleciona uma conta",
            options=df["accountId"].tolist(),
        )

        col1, col2 = st.columns(2)

        with col1:
            if st.button("Definir como Default"):
                try:
                    set_default_account(user["userId"], selected_account)
                    st.success("Default atualizado.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

        with col2:
            if st.button("Remover Associação"):
                try:
                    remove_user_account(user["userId"], selected_account)
                    st.success("Associação removida.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
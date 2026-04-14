from __future__ import annotations

import streamlit as st

from components.auth_guard import render_user_box, require_login
from components.filters import get_active_filters, render_global_filters
from components.tables import render_dataframe
from services.firestore_queries import load_generated_skus_df
from services.sku_service import (
    DEFAULT_PADDING,
    DEFAULT_PREFIX,
    SKUGenerationError,
    generate_next_sku,
)

user = require_login()
render_user_box(user)
render_global_filters(user)

filters = get_active_filters()
selected_account_id = filters["accountId"]
allowed_account_ids = filters["allowedAccountIds"]
selected_role = st.session_state.get("selected_account_role")
account_filter = None if selected_account_id in (None, "__ALL__") else selected_account_id

st.title("SKU Helper")

col1, col2 = st.columns([1, 2])

with col1:
    prefix = st.text_input("Prefixo", value=DEFAULT_PREFIX)
    padding = st.number_input("Padding", min_value=1, max_value=10, value=DEFAULT_PADDING)

    generate_disabled = (
        account_filter is None
        or selected_role not in {"OWNER", "MANAGER"}
    )

    if st.button("Gerar próximo SKU", disabled=generate_disabled):
        try:
            result = generate_next_sku(
                account_id=account_filter,
                prefix=prefix.strip() or DEFAULT_PREFIX,
                padding=int(padding),
            )
            st.success(f"Novo SKU: {result['sku']}")
        except SKUGenerationError as exc:
            st.error(str(exc))

with col2:
    if account_filter is None:
        st.warning("Seleciona uma conta específica para gerar SKU.")
    elif selected_role not in {"OWNER", "MANAGER"}:
        st.error("Não tens permissões para gerar SKU nesta conta.")
    else:
        st.info(f"Conta selecionada: {account_filter}")

st.subheader("Histórico de SKUs gerados")
generated_df = load_generated_skus_df(
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
)
visible_cols = [
    c for c in [
        "accountId",
        "sku",
        "numericValue",
        "prefix",
        "isAssigned",
        "assignedProductId",
        "createdAt",
    ] if c in generated_df.columns
]
render_dataframe(generated_df[visible_cols] if visible_cols else generated_df)
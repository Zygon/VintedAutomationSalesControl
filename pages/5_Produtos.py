from __future__ import annotations

from components.layout import apply_page_layout

apply_page_layout()


import streamlit as st

from components.auth_guard import render_user_box, require_login
from components.filters import get_active_filters, render_global_filters
from components.tables import render_dataframe
from services.firestore_queries import load_products_df
from services.sku_service import SKUGenerationError, upsert_product

user = require_login()
render_user_box(user)
render_global_filters(user)

filters = get_active_filters()
selected_account_id = filters["accountId"]
allowed_account_ids = filters["allowedAccountIds"]
selected_role = st.session_state.get("selected_account_role")
account_filter = None if selected_account_id in (None, "__ALL__") else selected_account_id

st.title("Produtos")

if account_filter is None:
    st.warning("Seleciona uma conta específica para gerir produtos.")
else:
    if selected_role not in {"OWNER", "MANAGER"}:
        st.error("Não tens permissões para gerir produtos nesta conta.")
    else:
        with st.form("product_form", clear_on_submit=True):
            st.subheader("Criar / atualizar produto")
            name = st.text_input("Nome do produto")
            category = st.text_input("Categoria")
            latest_sku = st.text_input("Último SKU")
            is_active = st.checkbox("Ativo", value=True)
            description_template = st.text_area("Template de descrição")
            submitted = st.form_submit_button("Guardar produto")

            if submitted:
                try:
                    product_id = upsert_product(
                        account_id=account_filter,
                        name=name,
                        category=category,
                        latest_sku=latest_sku,
                        is_active=is_active,
                        description_template=description_template,
                    )
                    st.success(f"Produto guardado: {product_id}")
                except SKUGenerationError as exc:
                    st.error(str(exc))

products_df = load_products_df(
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
)

st.subheader("Catálogo")
visible_cols = [
    c for c in [
        "accountId",
        "productId",
        "name",
        "category",
        "latestSku",
        "isActive",
        "updatedAt",
    ] if c in products_df.columns
]
render_dataframe(products_df[visible_cols] if visible_cols else products_df)
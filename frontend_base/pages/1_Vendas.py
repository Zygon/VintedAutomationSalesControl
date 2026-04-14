from __future__ import annotations

import pandas as pd
import streamlit as st

from components.auth_guard import render_user_box, require_login
from components.charts import render_bar_chart, render_line_chart
from components.filters import get_active_filters, render_global_filters
from components.tables import render_dataframe, render_kpis
from services.firestore_queries import (
    load_collection_df,
    load_label_by_id,
    load_sale_items_for_sale,
)
from services.metrics import (
    format_currency,
    safe_avg,
    safe_count,
    safe_sum,
    status_breakdown,
    time_series,
)

user = require_login()
render_user_box(user)
render_global_filters(user)

filters = get_active_filters()
selected_account_id = filters["accountId"]
allowed_account_ids = filters["allowedAccountIds"]
account_filter = None if selected_account_id in (None, "__ALL__") else selected_account_id
date_range = filters["dateRange"]

st.title("Vendas")

sales_df = load_collection_df(
    "sales",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="soldAt",
    date_range=date_range,
)

render_kpis([
    ("Total de vendas", str(safe_count(sales_df))),
    ("Faturação", format_currency(safe_sum(sales_df, "value"))),
    ("Ticket médio", format_currency(safe_avg(sales_df, "value"))),
])

left, right = st.columns([1.2, 1])

with left:
    series_df = (
        time_series(sales_df, "soldAtParsed", "value")
        if not sales_df.empty
        else pd.DataFrame()
    )
    render_line_chart(series_df, "period", "value", "Valor de vendas por período")

with right:
    render_bar_chart(status_breakdown(sales_df), "status", "count", "Estados das vendas")

st.subheader("Tabela de vendas")
visible_cols = [
    c for c in [
        "accountId",
        "saleId",
        "soldAt",
        "buyerUsername",
        "articleTitle",
        "sku",
        "value",
        "status",
        "orderId",
        "matchedLabelId",
    ] if c in sales_df.columns
]
render_dataframe(sales_df[visible_cols] if visible_cols else sales_df)

st.subheader("Detalhe da venda")
sale_options: list[str] = []
if not sales_df.empty and "saleId" in sales_df.columns:
    sale_options = sales_df["saleId"].dropna().astype(str).tolist()

selected_sale_id = st.selectbox("Seleciona uma saleId", [""] + sale_options)

if selected_sale_id:
    sale_row = sales_df[sales_df["saleId"].astype(str) == selected_sale_id].iloc[0].to_dict()
    st.json(sale_row)

    sale_items_df = load_sale_items_for_sale(
        selected_sale_id,
        account_id=account_filter,
        allowed_account_ids=allowed_account_ids,
    )
    st.markdown("#### saleItems")
    render_dataframe(sale_items_df)

    matched_label_id = sale_row.get("matchedLabelId")
    if matched_label_id:
        st.markdown("#### label")
        label = load_label_by_id(
            str(matched_label_id),
            account_id=account_filter,
            allowed_account_ids=allowed_account_ids,
        )
        if label:
            st.json(label)
        else:
            st.warning("Label associada não encontrada.")
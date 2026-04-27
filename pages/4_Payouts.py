from __future__ import annotations

from components.layout import apply_page_layout

apply_page_layout()


import pandas as pd
import streamlit as st

from components.auth_guard import render_user_box, require_login
from components.charts import render_bar_chart, render_line_chart
from components.filters import get_active_filters, render_global_filters
from components.tables import render_dataframe, render_kpis
from services.firestore_queries import load_collection_df
from services.metrics import format_currency, safe_count, safe_sum, time_series

user = require_login()
render_user_box(user)
render_global_filters(user)

filters = get_active_filters()
selected_account_id = filters["accountId"]
allowed_account_ids = filters["allowedAccountIds"]
account_filter = None if selected_account_id in (None, "__ALL__") else selected_account_id
date_range = filters["dateRange"]

st.title("Payouts")

payouts_df = load_collection_df(
    "payouts",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="receivedAt",
    date_range=date_range,
)

completed_sales_df = load_collection_df(
    "sales",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="completedAt",
    date_range=date_range,
)

if not completed_sales_df.empty and "status" in completed_sales_df.columns:
    completed_sales_df = completed_sales_df[completed_sales_df["status"] == "COMPLETED"]

render_kpis([
    ("Total payouts", format_currency(safe_sum(payouts_df, "amount"))),
    ("Vendas concluídas", format_currency(safe_sum(completed_sales_df, "completedItemAmount"))),
    ("Registos", str(safe_count(payouts_df))),
])

left, right = st.columns([1.2, 1])

with left:
    series_df = time_series(payouts_df, "receivedAtParsed", "amount")
    render_line_chart(series_df, "period", "amount", "Payouts por período")

with right:
    compare_df = pd.DataFrame([
        {"metric": "Payouts", "value": safe_sum(payouts_df, "amount")},
        {"metric": "Completed Sales", "value": safe_sum(completed_sales_df, "completedItemAmount")},
    ])
    render_bar_chart(compare_df, "metric", "value", "Payouts vs Sales Completed")

st.subheader("Tabela de payouts")
visible_cols = [
    c for c in [
        "accountId",
        "payoutId",
        "amount",
        "transferReference",
        "method",
        "estimatedArrivalRaw",
        "receivedAt",
        "status",
    ] if c in payouts_df.columns
]
render_dataframe(payouts_df[visible_cols] if visible_cols else payouts_df)
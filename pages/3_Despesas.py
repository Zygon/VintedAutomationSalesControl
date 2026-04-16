from __future__ import annotations

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

st.title("Despesas")

expenses_df = load_collection_df(
    "expenses",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="receivedAt",
    date_range=date_range,
)

render_kpis([
    ("Total despesas", format_currency(safe_sum(expenses_df, "totalAmount"))),
    ("IVA", format_currency(safe_sum(expenses_df, "vatAmount"))),
    ("Registos", str(safe_count(expenses_df))),
])

left, right = st.columns([1.2, 1])

with left:
    series_df = time_series(expenses_df, "receivedAtParsed", "totalAmount")
    render_line_chart(series_df, "period", "totalAmount", "Despesas por período")

with right:
    vat_df = (
        time_series(expenses_df, "receivedAtParsed", "vatAmount")
        if "vatAmount" in expenses_df.columns
        else pd.DataFrame()
    )
    render_bar_chart(vat_df, "period", "vatAmount", "IVA por período")

st.subheader("Tabela de despesas")
visible_cols = [
    c for c in [
        "accountId",
        "expenseId",
        "invoiceNumber",
        "billingPeriodRaw",
        "totalAmount",
        "vatAmount",
        "currency",
        "receivedAt",
        "status",
    ] if c in expenses_df.columns
]
render_dataframe(expenses_df[visible_cols] if visible_cols else expenses_df)
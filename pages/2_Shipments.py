from __future__ import annotations

import pandas as pd
import streamlit as st

from components.auth_guard import render_user_box, require_login
from components.charts import render_bar_chart, render_line_chart
from components.filters import get_active_filters, render_global_filters
from components.tables import render_dataframe, render_kpis
from services.firestore_queries import load_collection_df
from services.metrics import safe_count, status_breakdown, time_series

user = require_login()
render_user_box(user)
render_global_filters(user)

filters = get_active_filters()
selected_account_id = filters["accountId"]
allowed_account_ids = filters["allowedAccountIds"]
account_filter = None if selected_account_id in (None, "__ALL__") else selected_account_id
date_range = filters["dateRange"]
start_dt = date_range["start"]
end_dt = date_range["end"]

st.title("Shipment Status")

sales_df = load_collection_df(
    "sales",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
)

if not sales_df.empty and "status" in sales_df.columns:
    shipment_df = sales_df[
        sales_df["status"].isin([
            "READY_TO_PRINT",
            "PRINTING",
            "PRINTED",
            "PRINT_DISPATCHED",
            "SHIPPED",
            "COMPLETED",
            "REVIEW",
            "PRINT_ERROR",
        ])
    ]
else:
    shipment_df = sales_df

if not shipment_df.empty and "shippedAtParsed" in shipment_df.columns:
    shipment_df = shipment_df[
        shipment_df["shippedAtParsed"].isna() | (
            (shipment_df["shippedAtParsed"] >= start_dt)
            & (shipment_df["shippedAtParsed"] <= end_dt)
        )
    ]

shipped_count = 0
completed_count = 0

if not shipment_df.empty and "status" in shipment_df.columns:
    shipped_count = safe_count(shipment_df[shipment_df["status"] == "SHIPPED"])
    completed_count = safe_count(shipment_df[shipment_df["status"] == "COMPLETED"])

render_kpis([
    ("Registos", str(safe_count(shipment_df))),
    ("Shipped", str(shipped_count)),
    ("Completed", str(completed_count)),
])

left, right = st.columns([1.2, 1])

with left:
    series_df = (
        time_series(shipment_df, "updatedAtParsed")
        if "updatedAtParsed" in shipment_df.columns
        else pd.DataFrame()
    )
    render_line_chart(series_df, "period", "count", "Movimento do pipeline")

with right:
    render_bar_chart(status_breakdown(shipment_df), "status", "count", "Distribuição por estado")

st.subheader("Tabela de shipment status")
visible_cols = [
    c for c in [
        "accountId",
        "saleId",
        "articleTitle",
        "shippingCode",
        "status",
        "orderId",
        "shippedAt",
        "estimatedDeliveryStartRaw",
        "estimatedDeliveryEndRaw",
        "completedAt",
    ] if c in shipment_df.columns
]
render_dataframe(shipment_df[visible_cols] if visible_cols else shipment_df)
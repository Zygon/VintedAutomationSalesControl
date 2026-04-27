from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4
import json

from components.layout import apply_page_layout

apply_page_layout()

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from components.auth_guard import render_user_box, require_login
from components.charts import render_line_chart
from components.filters import get_active_filters, render_global_filters
from components.tables import render_kpis
from services.firestore_queries import load_collection_df, upsert_row, clear_all_caches
from services.metrics import format_currency, safe_count, safe_sum, time_series

STATUS_COLORS = {
    "PAID": "#d1e7dd",
    "APPROVED": "#d1e7dd",
    "COMPLETED": "#d1e7dd",
    "RECEIVED": "#d1e7dd",
    "PENDING": "#fff3cd",
    "SUBMITTED": "#fff3cd",
    "IN_REVIEW": "#fff3cd",
    "PROCESSING": "#fff3cd",
    "OVERDUE": "#f8d7da",
    "REJECTED": "#f8d7da",
    "CANCELED": "#e2e3e5",
    "CANCELLED": "#e2e3e5",
    "ERROR": "#dc3545",
}

STATUS_TEXT_COLORS = {
    "PAID": "#155724",
    "APPROVED": "#155724",
    "COMPLETED": "#155724",
    "RECEIVED": "#155724",
    "PENDING": "#856404",
    "SUBMITTED": "#856404",
    "IN_REVIEW": "#856404",
    "PROCESSING": "#856404",
    "OVERDUE": "#721c24",
    "REJECTED": "#721c24",
    "CANCELED": "#41464b",
    "CANCELLED": "#41464b",
    "ERROR": "#ffffff",
}


@st.dialog("Nova despesa", width="large")
def _render_create_expense_modal(account_id: str | None) -> None:
    if not account_id:
        st.warning("Seleciona uma conta específica antes de inserir uma despesa.")
        return

    with st.form("create_expense_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            invoice_number = st.text_input("Invoice Number")
            total_amount = st.number_input("Total Amount", min_value=0.0, step=0.01, format="%.2f")
            currency = st.text_input("Currency", value="EUR")
        with col2:
            billing_period = st.text_input("Billing Period")
            vat_amount = st.number_input("VAT Amount", min_value=0.0, step=0.01, format="%.2f")
            status = st.text_input("Status", value="RECEIVED")

        received_at = st.date_input("Received At", value=datetime.now(timezone.utc).date())
        notes = st.text_area("Notes")

        submitted = st.form_submit_button("Guardar despesa")

        if submitted:
            now_iso = datetime.now(timezone.utc).isoformat()
            expense_id = str(uuid4())

            payload = {
                "expenseId": expense_id,
                "accountId": str(account_id),
                "invoiceNumber": str(invoice_number).strip() or None,
                "billingPeriodRaw": str(billing_period).strip() or None,
                "totalAmount": float(total_amount),
                "vatAmount": float(vat_amount),
                "currency": str(currency).strip() or "EUR",
                "receivedAt": datetime.combine(received_at, datetime.min.time(), tzinfo=timezone.utc).isoformat(),
                "status": str(status).strip() or "RECEIVED",
                "notes": str(notes).strip() or None,
                "createdAt": now_iso,
                "updatedAt": now_iso,
            }

            upsert_row("expenses", expense_id, payload)
            clear_all_caches()
            st.success("Despesa criada com sucesso.")
            st.rerun()


def _sanitize_for_aggrid(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    work = df.copy()
    for col in work.columns:
        series = work[col]

        if pd.api.types.is_datetime64_any_dtype(series):
            work[col] = series.dt.strftime("%Y-%m-%d %H:%M:%S").where(series.notna(), None)
            continue

        work[col] = series.astype("object")
        work[col] = work[col].where(pd.notna(work[col]), None)
        work[col] = work[col].map(lambda x: str(x) if x is not None else None)

    return work


def _build_expenses_table(df: pd.DataFrame) -> None:
    visible_df = df.copy()

    if "expenseId" in visible_df.columns:
        visible_df = visible_df.drop(columns=["expenseId"])

    if visible_df.empty:
        st.info("Sem dados para apresentar.")
        return

    grid_df = _sanitize_for_aggrid(visible_df)
    gb = GridOptionsBuilder.from_dataframe(grid_df)

    gb.configure_default_column(
        resizable=True,
        sortable=True,
        filter=True,
    )

    for numeric_col in ["totalAmount", "vatAmount"]:
        if numeric_col in grid_df.columns:
            gb.configure_column(numeric_col, type=["numericColumn"])

    status_colors_js = json.dumps(STATUS_COLORS)
    status_text_colors_js = json.dumps(STATUS_TEXT_COLORS)

    row_style_jscode = JsCode(
        f"""
        function(params) {{
            const rawStatus = params.data && params.data.status ? String(params.data.status).trim().toUpperCase() : '';
            const bgColors = {status_colors_js};
            const textColors = {status_text_colors_js};

            if (bgColors[rawStatus]) {{
                const style = {{ backgroundColor: bgColors[rawStatus] }};
                if (textColors[rawStatus]) {{
                    style.color = textColors[rawStatus];
                }}
                return style;
            }}

            if (rawStatus) {{
                return {{ backgroundColor: '#e7f1ff', color: '#0b2239' }};
            }}

            return {{}};
        }}
        """
    )

    gb.configure_grid_options(
        rowHeight=40,
        animateRows=False,
        suppressRowClickSelection=True,
        suppressCellFocus=False,
        getRowStyle=row_style_jscode,
    )

    grid_options = gb.build()

    AgGrid(
        grid_df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.NO_UPDATE,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False,
        use_container_width=True,
        height=550,
        theme="streamlit",
        reload_data=False,
    )


user = require_login()
render_user_box(user)
render_global_filters(user)

filters = get_active_filters()
selected_account_id = filters["accountId"]
allowed_account_ids = filters["allowedAccountIds"]
account_filter = None if selected_account_id in (None, "__ALL__") else selected_account_id
date_range = filters["dateRange"]

st.title("Despesas")

action_col, _ = st.columns([1, 5])
with action_col:
    if st.button("Nova despesa"):
        _render_create_expense_modal(account_filter)

expenses_df = load_collection_df(
    "expenses",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="receivedAt",
    date_range=date_range,
)

total_expenses = safe_sum(expenses_df, "totalAmount")
expense_count = safe_count(expenses_df)
average_expense = (total_expenses / expense_count) if expense_count > 0 else 0

render_kpis([
    ("Total despesas", format_currency(total_expenses)),
    ("Despesa média", format_currency(average_expense)),
    ("Registos", str(expense_count)),
])

series_df = time_series(expenses_df, "receivedAtParsed", "totalAmount")
render_line_chart(series_df, "period", "totalAmount", "Despesas por período")

st.subheader("Tabela de despesas")
_build_expenses_table(expenses_df)

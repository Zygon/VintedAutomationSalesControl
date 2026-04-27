from __future__ import annotations

from components.layout import apply_page_layout

apply_page_layout()

import json
from datetime import datetime, time, timezone
from uuid import uuid4

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from components.auth_guard import render_user_box, require_login
from components.charts import render_bar_chart, render_line_chart
from components.filters import get_active_filters, render_global_filters
from components.tables import render_kpis
from services.firestore_queries import clear_all_caches, load_collection_df, upsert_row
from services.metrics import format_currency, safe_count, safe_sum, time_series

STATUS_COLORS = {
    "PENDING": "#fff3cd",
    "PAID": "#d1e7dd",
    "CANCELED": "#e2e3e5",
    "CANCELLED": "#e2e3e5",
    "OVERDUE": "#f8d7da",
    "ERROR": "#dc3545",
    "REJECTED": "#dc3545",
    "REVIEW": "#cfe2ff",
}

STATUS_TEXT_COLORS = {
    "PENDING": "#856404",
    "PAID": "#155724",
    "CANCELED": "#41464b",
    "CANCELLED": "#41464b",
    "OVERDUE": "#721c24",
    "ERROR": "#ffffff",
    "REJECTED": "#ffffff",
    "REVIEW": "#0c5460",
}


def _sanitize_for_aggrid(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    work = df.copy()

    for col in work.columns:
        series = work[col]

        if pd.api.types.is_datetime64_any_dtype(series):
            work[col] = series.dt.strftime("%Y-%m-%d %H:%M:%S").where(series.notna(), None)
            continue

        if pd.api.types.is_string_dtype(series) or series.dtype == "object":
            work[col] = series.astype("object")
            work[col] = work[col].where(pd.notna(work[col]), None)
            work[col] = work[col].map(lambda x: str(x) if x is not None else None)
            continue

        if pd.api.types.is_integer_dtype(series) or pd.api.types.is_float_dtype(series):
            work[col] = pd.to_numeric(series, errors="coerce")
            continue

        work[col] = series.astype("object")
        work[col] = work[col].where(pd.notna(work[col]), None)

    return work


@st.dialog("Inserir despesa", width="large")
def _render_insert_expense_modal(account_filter, allowed_account_ids):
    account_options = list(dict.fromkeys([acc for acc in allowed_account_ids if acc]))
    default_account = account_filter if account_filter else (account_options[0] if account_options else None)

    with st.form("insert_expense_form", clear_on_submit=False):
        st.markdown("#### Nova despesa")

        col1, col2 = st.columns(2)
        with col1:
            if default_account:
                selected_account_id = st.selectbox(
                    "Conta",
                    options=account_options,
                    index=account_options.index(default_account) if default_account in account_options else 0,
                )
            else:
                selected_account_id = None
                st.warning("Sem contas disponíveis para inserir despesa.")

            invoice_number = st.text_input("Invoice Number")
            billing_period_raw = st.text_input("Billing Period")
            currency = st.text_input("Currency", value="EUR")

        with col2:
            received_date = st.date_input("Data de receção", value=datetime.now(timezone.utc).date())
            total_amount = st.number_input("Valor total", min_value=0.0, step=0.01, format="%.2f")
            vat_amount = st.number_input("IVA", min_value=0.0, step=0.01, format="%.2f")
            status = st.selectbox(
                "Status",
                options=["PENDING", "PAID", "REVIEW", "CANCELED", "OVERDUE"],
                index=0,
            )

        submitted = st.form_submit_button("Guardar despesa", use_container_width=True)

    if not submitted:
        return

    if not selected_account_id:
        st.error("Não existe accountId disponível para gravar a despesa.")
        return

    expense_id = uuid4().hex
    received_at = datetime.combine(received_date, time.min, tzinfo=timezone.utc).isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()

    payload = {
        "expenseId": expense_id,
        "accountId": selected_account_id,
        "invoiceNumber": invoice_number.strip() or None,
        "billingPeriodRaw": billing_period_raw.strip() or None,
        "totalAmount": float(total_amount),
        "vatAmount": float(vat_amount),
        "currency": (currency or "EUR").strip().upper(),
        "receivedAt": received_at,
        "status": str(status).strip().upper(),
        "createdAt": now_iso,
        "updatedAt": now_iso,
    }

    upsert_row("expenses", expense_id, payload)
    clear_all_caches()
    st.success("Despesa inserida com sucesso.")
    st.rerun()


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
    if st.button("Inserir despesa", use_container_width=True):
        _render_insert_expense_modal(account_filter, allowed_account_ids)

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
        "invoiceNumber",
        "billingPeriodRaw",
        "totalAmount",
        "vatAmount",
        "currency",
        "receivedAt",
        "status",
    ] if c in expenses_df.columns
]

support_cols = [c for c in ["expenseId"] if c in expenses_df.columns]
table_df = expenses_df[support_cols + visible_cols].copy() if not expenses_df.empty else expenses_df.copy()
grid_df = _sanitize_for_aggrid(table_df)

gb = GridOptionsBuilder.from_dataframe(grid_df)
gb.configure_default_column(resizable=True, sortable=True, filter=True)

if "expenseId" in grid_df.columns:
    gb.configure_column("expenseId", hide=True)

if "totalAmount" in grid_df.columns:
    gb.configure_column("totalAmount", type=["numericColumn"])

if "vatAmount" in grid_df.columns:
    gb.configure_column("vatAmount", type=["numericColumn"])

status_colors_js = json.dumps(STATUS_COLORS)
status_text_colors_js = json.dumps(STATUS_TEXT_COLORS)

row_style_jscode = JsCode(
    f"""
    function(params) {{
        const rawStatus = String((params.data && params.data.status) || '').toUpperCase().trim();
        const bgColors = {status_colors_js};
        const textColors = {status_text_colors_js};

        if (bgColors[rawStatus]) {{
            const style = {{ backgroundColor: bgColors[rawStatus] }};
            if (textColors[rawStatus]) {{
                style.color = textColors[rawStatus];
            }}
            return style;
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

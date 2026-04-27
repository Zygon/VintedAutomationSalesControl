from __future__ import annotations

from components.layout import apply_page_layout

apply_page_layout()

import json
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from components.auth_guard import render_user_box, require_login
from components.charts import render_bar_chart, render_line_chart
from components.filters import get_active_filters, render_global_filters
from components.tables import render_kpis
from services.firestore_queries import load_collection_df, upsert_row
from services.metrics import format_currency, safe_count, safe_sum, time_series

COMMON_STATUS_COLORS = {
    "PAID": "#d1e7dd",
    "COMPLETED": "#d1e7dd",
    "DONE": "#d1e7dd",
    "APPROVED": "#d1e7dd",
    "PENDING": "#fff3cd",
    "WAITING": "#fff3cd",
    "OPEN": "#fff3cd",
    "RECEIVED": "#cfe2ff",
    "ISSUED": "#cfe2ff",
    "IN_PROGRESS": "#cfe2ff",
    "CANCELED": "#e2e3e5",
    "CANCELLED": "#e2e3e5",
    "VOID": "#e2e3e5",
    "ERROR": "#dc3545",
    "FAILED": "#dc3545",
    "REJECTED": "#dc3545",
}

COMMON_STATUS_TEXT_COLORS = {
    "PAID": "#155724",
    "COMPLETED": "#155724",
    "DONE": "#155724",
    "APPROVED": "#155724",
    "PENDING": "#856404",
    "WAITING": "#856404",
    "OPEN": "#856404",
    "RECEIVED": "#0c5460",
    "ISSUED": "#0c5460",
    "IN_PROGRESS": "#0c5460",
    "CANCELED": "#41464b",
    "CANCELLED": "#41464b",
    "VOID": "#41464b",
    "ERROR": "#ffffff",
    "FAILED": "#ffffff",
    "REJECTED": "#ffffff",
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

        if pd.api.types.is_numeric_dtype(series):
            work[col] = pd.to_numeric(series, errors="coerce")
            continue

        work[col] = series.astype("object")
        work[col] = work[col].where(pd.notna(work[col]), None)
        work[col] = work[col].map(lambda x: str(x) if x is not None else None)

    return work



def _display_columns(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    return [c for c in df.columns if c != "expenseId" and not str(c).endswith("Parsed")]



def _status_options(df: pd.DataFrame) -> list[str]:
    if "status" not in df.columns or df.empty:
        return ["PENDING", "RECEIVED", "PAID", "CANCELED", "ERROR"]

    values = []
    for value in df["status"].dropna().astype(str).str.strip():
        if value and value not in values:
            values.append(value)

    base = ["PENDING", "RECEIVED", "PAID", "CANCELED", "ERROR"]
    for value in base:
        if value not in values:
            values.append(value)
    return values



def _build_row_style_js() -> JsCode:
    bg_map = json.dumps(COMMON_STATUS_COLORS)
    text_map = json.dumps(COMMON_STATUS_TEXT_COLORS)
    return JsCode(
        f"""
        function(params) {{
            const raw = String((params.data && params.data.status) || '').trim();
            if (!raw) {{
                return {{}};
            }}

            const status = raw.toUpperCase();
            const bgColors = {bg_map};
            const textColors = {text_map};

            let bg = bgColors[status] || null;
            let fg = textColors[status] || null;

            if (!bg) {{
                if (status.includes('ERR') || status.includes('FAIL') || status.includes('REJECT')) {{
                    bg = '#dc3545';
                    fg = '#ffffff';
                }} else if (status.includes('CANCEL') || status.includes('VOID')) {{
                    bg = '#e2e3e5';
                    fg = '#41464b';
                }} else if (status.includes('PAID') || status.includes('DONE') || status.includes('COMPLETE') || status.includes('APPROV')) {{
                    bg = '#d1e7dd';
                    fg = '#155724';
                }} else if (status.includes('RECEIV') || status.includes('ISSU') || status.includes('PROGRESS')) {{
                    bg = '#cfe2ff';
                    fg = '#0c5460';
                }} else if (status.includes('PEND') || status.includes('WAIT') || status.includes('OPEN')) {{
                    bg = '#fff3cd';
                    fg = '#856404';
                }} else {{
                    bg = '#f8f9fa';
                    fg = '#212529';
                }}
            }}

            return {{
                backgroundColor: bg,
                color: fg || '#212529'
            }};
        }}
        """
    )



def _build_status_chart_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "status" not in df.columns:
        return pd.DataFrame()

    work = df.copy()
    work["status"] = work["status"].fillna("SEM_STATUS").astype(str)
    chart_df = work.groupby("status", dropna=False).size().reset_index(name="count")
    return chart_df.sort_values("count", ascending=False)



def _pick_form_account_id(selected_account_id: str | None, allowed_account_ids: list[str]) -> str:
    if selected_account_id and selected_account_id != "__ALL__":
        return selected_account_id
    if allowed_account_ids:
        return str(allowed_account_ids[0])
    return ""


@st.dialog("Nova despesa", width="large")
def _render_expense_modal(selected_account_id: str | None, allowed_account_ids: list[str]) -> None:
    default_account = _pick_form_account_id(selected_account_id, allowed_account_ids)

    with st.form("create_expense_form"):
        st.markdown("#### Inserir despesa manual")

        account_id = st.text_input("Account ID", value=default_account)

        col1, col2 = st.columns(2)
        with col1:
            invoice_number = st.text_input("Invoice Number")
            billing_period_raw = st.text_input("Billing Period")
            total_amount = st.number_input("Total Amount", min_value=0.0, step=0.01, format="%.2f")
        with col2:
            currency = st.text_input("Currency", value="EUR")
            status = st.text_input("Status", value="RECEIVED")
            vat_amount = st.number_input("VAT Amount", min_value=0.0, step=0.01, format="%.2f")

        received_at = st.date_input("Received At", value=datetime.now(timezone.utc).date())

        submitted = st.form_submit_button("Guardar despesa")

        if submitted:
            if not account_id.strip():
                st.error("Account ID é obrigatório.")
                return

            expense_id = str(uuid4())
            received_dt = datetime.combine(received_at, datetime.min.time(), tzinfo=timezone.utc)
            now_iso = datetime.now(timezone.utc).isoformat()

            payload = {
                "expenseId": expense_id,
                "accountId": account_id.strip(),
                "invoiceNumber": invoice_number.strip() or None,
                "billingPeriodRaw": billing_period_raw.strip() or None,
                "totalAmount": float(total_amount),
                "vatAmount": float(vat_amount),
                "currency": (currency.strip() or "EUR"),
                "receivedAt": received_dt.isoformat(),
                "status": (status.strip() or "RECEIVED"),
                "createdAt": now_iso,
                "updatedAt": now_iso,
            }

            upsert_row("expenses", expense_id, payload)
            st.success("Despesa criada com sucesso.")
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

header_left, header_right = st.columns([3, 1])
with header_right:
    if st.button("Nova despesa", use_container_width=True):
        _render_expense_modal(selected_account_id, allowed_account_ids)

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

status_chart_df = _build_status_chart_df(expenses_df)
if not status_chart_df.empty:
    st.subheader("Distribuição por estado")
    render_bar_chart(status_chart_df, "status", "count", "Distribuição por estado")

st.subheader("Tabela de despesas")

visible_cols = _display_columns(expenses_df)
table_df = expenses_df[visible_cols].copy() if visible_cols else expenses_df.copy()
grid_df = _sanitize_for_aggrid(table_df)

gb = GridOptionsBuilder.from_dataframe(grid_df)
gb.configure_default_column(resizable=True, sortable=True, filter=True)

status_values = _status_options(expenses_df)
if "status" in grid_df.columns:
    gb.configure_column(
        "status",
        editable=True,
        cellEditor="agSelectCellEditor",
        cellEditorParams={"values": status_values},
    )

for numeric_col in ["totalAmount", "vatAmount"]:
    if numeric_col in grid_df.columns:
        gb.configure_column(numeric_col, type=["numericColumn"])

gb.configure_grid_options(
    rowHeight=40,
    animateRows=False,
    suppressRowClickSelection=True,
    rowSelection="single",
    getRowStyle=_build_row_style_js(),
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

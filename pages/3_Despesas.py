from __future__ import annotations

from components.layout import apply_page_layout

apply_page_layout()

import json
import uuid
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

from components.auth_guard import render_user_box, require_login
from components.filters import get_active_filters, render_global_filters
from components.tables import render_kpis
from services.firestore_queries import load_collection_df, upsert_row
from services.metrics import format_currency, safe_count, safe_sum

STATUS_COLORS = {
    "PENDING": "#fff3cd",
    "OPEN": "#fff3cd",
    "RECEIVED": "#cfe2ff",
    "PAID": "#d1e7dd",
    "DONE": "#d1e7dd",
    "COMPLETED": "#d1e7dd",
    "CANCELED": "#e2e3e5",
    "CANCELLED": "#e2e3e5",
    "ERROR": "#dc3545",
}

STATUS_TEXT_COLORS = {
    "PENDING": "#856404",
    "OPEN": "#856404",
    "RECEIVED": "#0c5460",
    "PAID": "#155724",
    "DONE": "#155724",
    "COMPLETED": "#155724",
    "CANCELED": "#41464b",
    "CANCELLED": "#41464b",
    "ERROR": "#ffffff",
}

WEEKDAY_LABELS_PT = {
    0: "Seg",
    1: "Ter",
    2: "Qua",
    3: "Qui",
    4: "Sex",
    5: "Sáb",
    6: "Dom",
}


@st.dialog("Inserir despesa", width="large")
def _render_create_expense_modal(account_id: str | None) -> None:
    st.markdown("#### Nova despesa")

    with st.form("create_expense_form"):
        invoice_number = st.text_input("Invoice Number")
        billing_period = st.text_input("Billing Period")
        total_amount = st.number_input("Total Amount", min_value=0.0, step=0.01, format="%.2f")
        vat_amount = st.number_input("VAT Amount", min_value=0.0, step=0.01, format="%.2f")
        currency = st.text_input("Currency", value="EUR")
        received_at = st.date_input("Received At")
        status = st.text_input("Status", value="RECEIVED")
        notes = st.text_area("Notes")

        submitted = st.form_submit_button("Guardar despesa")

    if not submitted:
        return

    expense_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "expenseId": expense_id,
        "accountId": account_id,
        "invoiceNumber": invoice_number.strip() or None,
        "billingPeriodRaw": billing_period.strip() or None,
        "totalAmount": float(total_amount),
        "vatAmount": float(vat_amount),
        "currency": (currency or "EUR").strip() or "EUR",
        "receivedAt": datetime.combine(received_at, datetime.min.time(), tzinfo=timezone.utc).isoformat(),
        "status": (status or "RECEIVED").strip(),
        "notes": notes.strip() or None,
        "createdAt": now_iso,
        "updatedAt": now_iso,
    }

    upsert_row("expenses", expense_id, payload)
    st.success("Despesa inserida com sucesso.")
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

        if pd.api.types.is_string_dtype(series) or series.dtype == "object":
            obj = series.astype("object")
            obj = obj.where(pd.notna(obj), None)
            work[col] = obj.map(lambda x: str(x) if x is not None else None)
            continue

        if pd.api.types.is_integer_dtype(series) or pd.api.types.is_float_dtype(series):
            work[col] = pd.to_numeric(series, errors="coerce")
            continue

        obj = series.astype("object")
        work[col] = obj.where(pd.notna(obj), None)

    return work


def _to_naive_timestamp(value):
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts).tz_convert(None)


def _extract_current_range(date_range):
    if not isinstance(date_range, dict):
        return None, None

    start = _to_naive_timestamp(date_range.get("start"))
    end = _to_naive_timestamp(date_range.get("end"))

    if start is None or end is None:
        return None, None

    if start > end:
        start, end = end, start

    return start, end


def _start_of_month(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=ts.year, month=ts.month, day=1)


def _end_of_month(ts: pd.Timestamp) -> pd.Timestamp:
    month_start = _start_of_month(ts)
    next_month = month_start + pd.offsets.MonthBegin(1)
    return pd.Timestamp(next_month) - pd.Timedelta(microseconds=1)


def _range_dict(start: pd.Timestamp, end: pd.Timestamp) -> dict:
    return {
        "start": start.tz_localize("UTC").to_pydatetime(),
        "end": end.tz_localize("UTC").to_pydatetime(),
    }


def _previous_range_for_mode(current_start: pd.Timestamp, current_end: pd.Timestamp, period_mode: str):
    if period_mode == "Dia":
        return current_start - pd.Timedelta(days=1), current_end - pd.Timedelta(days=1)

    if period_mode == "Semana":
        return current_start - pd.Timedelta(days=7), current_end - pd.Timedelta(days=7)

    if period_mode == "Mês":
        prev_month_anchor = current_start - pd.offsets.MonthBegin(1)
        prev_month_anchor = pd.Timestamp(prev_month_anchor)
        prev_start = _start_of_month(prev_month_anchor)
        prev_month_end = _end_of_month(prev_month_anchor)
        same_elapsed = current_end - current_start
        prev_end_candidate = prev_start + same_elapsed
        prev_end = min(prev_end_candidate, prev_month_end)
        return prev_start, prev_end

    duration = current_end - current_start
    prev_end = current_start - pd.Timedelta(microseconds=1)
    prev_start = prev_end - duration
    return prev_start, prev_end


def _get_datetime_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="datetime64[ns]")

    if "receivedAtParsed" in df.columns:
        series = pd.to_datetime(df["receivedAtParsed"], errors="coerce", utc=True)
        if series.notna().any():
            return series.dt.tz_convert(None)

    if "receivedAt" in df.columns:
        series = pd.to_datetime(df["receivedAt"], errors="coerce", utc=True)
        if series.notna().any():
            return series.dt.tz_convert(None)

    return pd.Series(dtype="datetime64[ns]")


def _get_numeric_value_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")

    if "totalAmount" in df.columns:
        return pd.to_numeric(df["totalAmount"], errors="coerce").fillna(0)

    return pd.Series([0.0] * len(df), index=df.index, dtype="float64")


def _build_comparison_df_day(current_df, previous_df, current_start, current_end, previous_start, previous_end):
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["value"] = _get_numeric_value_series(current_work)
    previous_work["value"] = _get_numeric_value_series(previous_work)

    current_work["_hour"] = current_work["_dt"].dt.hour
    previous_work["_hour"] = previous_work["_dt"].dt.hour

    current_series = current_work.groupby("_hour")["value"].sum()
    previous_series = previous_work.groupby("_hour")["value"].sum()

    hours = list(range(24))
    labels = [f"{hour:02d}:00" for hour in hours]

    return pd.DataFrame({
        "label": labels,
        "current_value": [float(current_series.get(hour, 0)) for hour in hours],
        "previous_value": [float(previous_series.get(hour, 0)) for hour in hours],
    })


def _build_comparison_df_week(current_df, previous_df, current_start, current_end, previous_start, previous_end):
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["value"] = _get_numeric_value_series(current_work)
    previous_work["value"] = _get_numeric_value_series(previous_work)

    current_work["_weekday"] = current_work["_dt"].dt.weekday
    previous_work["_weekday"] = previous_work["_dt"].dt.weekday

    current_series = current_work.groupby("_weekday")["value"].sum()
    previous_series = previous_work.groupby("_weekday")["value"].sum()

    start_weekday = current_start.weekday()
    end_weekday = current_end.weekday()
    weekdays = list(range(start_weekday, end_weekday + 1))
    labels = [WEEKDAY_LABELS_PT[d] for d in weekdays]

    return pd.DataFrame({
        "label": labels,
        "current_value": [float(current_series.get(day, 0)) for day in weekdays],
        "previous_value": [float(previous_series.get(day, 0)) for day in weekdays],
    })


def _build_comparison_df_month(current_df, previous_df, current_start, current_end, previous_start, previous_end):
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["value"] = _get_numeric_value_series(current_work)
    previous_work["value"] = _get_numeric_value_series(previous_work)

    current_work["_day"] = current_work["_dt"].dt.day
    previous_work["_day"] = previous_work["_dt"].dt.day

    current_series = current_work.groupby("_day")["value"].sum()
    previous_series = previous_work.groupby("_day")["value"].sum()

    days = list(range(1, current_end.day + 1))
    labels = [str(day) for day in days]

    return pd.DataFrame({
        "label": labels,
        "current_value": [float(current_series.get(day, 0)) for day in days],
        "previous_value": [float(previous_series.get(day, 0)) for day in days],
    })


def _build_comparison_df_fallback(current_df, previous_df, current_start, current_end, previous_start, previous_end):
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["value"] = _get_numeric_value_series(current_work)
    previous_work["value"] = _get_numeric_value_series(previous_work)

    current_work["_day"] = current_work["_dt"].dt.normalize()
    previous_work["_day"] = previous_work["_dt"].dt.normalize()

    current_series = current_work.groupby("_day")["value"].sum()
    previous_series = previous_work.groupby("_day")["value"].sum()

    current_days = pd.date_range(
        start=pd.Timestamp(year=current_start.year, month=current_start.month, day=current_start.day),
        end=pd.Timestamp(year=current_end.year, month=current_end.month, day=current_end.day),
        freq="1D",
    )
    previous_days = pd.date_range(
        start=pd.Timestamp(year=previous_start.year, month=previous_start.month, day=previous_start.day),
        end=pd.Timestamp(year=previous_end.year, month=previous_end.month, day=previous_end.day),
        freq="1D",
    )

    slot_count = max(len(current_days), len(previous_days))
    labels = []
    current_values = []
    previous_values = []

    for i in range(slot_count):
        if i < len(current_days):
            labels.append(current_days[i].strftime("%d/%m"))
            current_values.append(float(current_series.get(current_days[i], 0)))
        else:
            labels.append(str(i + 1))
            current_values.append(0.0)

        if i < len(previous_days):
            previous_values.append(float(previous_series.get(previous_days[i], 0)))
        else:
            previous_values.append(0.0)

    return pd.DataFrame({
        "label": labels,
        "current_value": current_values,
        "previous_value": previous_values,
    })


def _build_comparison_df(current_df, previous_df, current_start, current_end, previous_start, previous_end, period_mode):
    if period_mode == "Dia":
        return _build_comparison_df_day(current_df, previous_df, current_start, current_end, previous_start, previous_end)
    if period_mode == "Semana":
        return _build_comparison_df_week(current_df, previous_df, current_start, current_end, previous_start, previous_end)
    if period_mode == "Mês":
        return _build_comparison_df_month(current_df, previous_df, current_start, current_end, previous_start, previous_end)
    return _build_comparison_df_fallback(current_df, previous_df, current_start, current_end, previous_start, previous_end)


def _render_expenses_comparison_chart(current_df, previous_df, current_start, current_end, previous_start, previous_end, period_mode, title):
    aligned_df = _build_comparison_df(
        current_df=current_df,
        previous_df=previous_df,
        current_start=current_start,
        current_end=current_end,
        previous_start=previous_start,
        previous_end=previous_end,
        period_mode=period_mode,
    )

    if aligned_df.empty:
        st.info("Sem dados para apresentar.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=aligned_df["label"],
            y=aligned_df["current_value"],
            mode="lines+markers",
            name="Período atual",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=aligned_df["label"],
            y=aligned_df["previous_value"],
            mode="lines+markers",
            name="Período anterior",
            line=dict(color="red"),
        )
    )
    fig.update_layout(
        title=title,
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis_title=None,
        yaxis_title=None,
        height=350,
        legend_title_text=None,
    )
    st.plotly_chart(fig, use_container_width=True)


user = require_login()
render_user_box(user)
render_global_filters(user)

filters = get_active_filters()
selected_account_id = filters["accountId"]
allowed_account_ids = filters["allowedAccountIds"]
account_filter = None if selected_account_id in (None, "__ALL__") else selected_account_id
date_range = filters["dateRange"]
period_mode = st.session_state.get("period_mode", "Mês")

st.title("Despesas")

header_left, header_right = st.columns([3, 1])
with header_right:
    if st.button("Inserir despesa"):
        _render_create_expense_modal(account_filter)

expenses_df = load_collection_df(
    "expenses",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="receivedAt",
    date_range=date_range,
)

current_start, current_end = _extract_current_range(date_range)
previous_expenses_df = pd.DataFrame()
previous_start, previous_end = None, None

if current_start is not None and current_end is not None:
    previous_start, previous_end = _previous_range_for_mode(current_start, current_end, period_mode)
    previous_expenses_df = load_collection_df(
        "expenses",
        account_id=account_filter,
        allowed_account_ids=allowed_account_ids,
        date_field="receivedAt",
        date_range=_range_dict(previous_start, previous_end),
    )

if not expenses_df.empty:
    if "receivedAtParsed" in expenses_df.columns:
        expenses_df = expenses_df.sort_values("receivedAtParsed", ascending=False, na_position="last")
    elif "receivedAt" in expenses_df.columns:
        expenses_df = expenses_df.sort_values("receivedAt", ascending=False, na_position="last")

if not previous_expenses_df.empty:
    if "receivedAtParsed" in previous_expenses_df.columns:
        previous_expenses_df = previous_expenses_df.sort_values("receivedAtParsed", ascending=False, na_position="last")
    elif "receivedAt" in previous_expenses_df.columns:
        previous_expenses_df = previous_expenses_df.sort_values("receivedAt", ascending=False, na_position="last")

total_expenses = safe_sum(expenses_df, "totalAmount")
expense_count = safe_count(expenses_df)
average_expense = (total_expenses / expense_count) if expense_count > 0 else 0

render_kpis([
    ("Total despesas", format_currency(total_expenses)),
    ("Despesa média", format_currency(average_expense)),
    ("Registos", str(expense_count)),
])

if (
    current_start is not None
    and current_end is not None
    and previous_start is not None
    and previous_end is not None
):
    _render_expenses_comparison_chart(
        current_df=expenses_df,
        previous_df=previous_expenses_df,
        current_start=current_start,
        current_end=current_end,
        previous_start=previous_start,
        previous_end=previous_end,
        period_mode=period_mode,
        title="Despesas por período",
    )
else:
    st.info("Sem dados para apresentar.")

st.subheader("Tabela de despesas")

visible_cols = [c for c in expenses_df.columns if c != "expenseId"]
table_df = expenses_df[visible_cols].copy() if visible_cols else expenses_df.copy()
grid_df = _sanitize_for_aggrid(table_df)

gb = GridOptionsBuilder.from_dataframe(grid_df)
gb.configure_default_column(resizable=True, sortable=True, filter=True)
gb.configure_grid_options(rowHeight=40, animateRows=False)

status_colors_js = json.dumps(STATUS_COLORS)
status_text_colors_js = json.dumps(STATUS_TEXT_COLORS)
row_style_jscode = JsCode(
    f"""
    function(params) {{
        const status = String((params.data && params.data.status) || '').toUpperCase();
        const bgColors = {status_colors_js};
        const textColors = {status_text_colors_js};

        if (bgColors[status]) {{
            const style = {{ backgroundColor: bgColors[status] }};
            if (textColors[status]) {{
                style.color = textColors[status];
            }}
            return style;
        }}

        return {{}};
    }}
    """
)
gb.configure_grid_options(getRowStyle=row_style_jscode)

grid_options = gb.build()
AgGrid(
    grid_df,
    gridOptions=grid_options,
    allow_unsafe_jscode=True,
    fit_columns_on_grid_load=False,
    use_container_width=True,
    height=550,
    theme="streamlit",
    reload_data=False,
)

from __future__ import annotations

import json

from components.layout import apply_page_layout

apply_page_layout()


import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

from components.auth_guard import render_user_box, require_login
from components.filters import get_active_filters, render_global_filters
from components.tables import render_kpis
from services.firestore_queries import load_collection_df
from services.metrics import safe_count, status_breakdown

SHIPMENT_STATUSES = [
    "READY_TO_PRINT",
    "PRINTING",
    "PRINTED",
    "PRINT_DISPATCHED",
    "SHIPPED",
    "COMPLETED",
    "REVIEW",
    "PRINT_ERROR",
    "IN_PRODUCTION",
    "PACKAGING",
    "WAITING_FOR_PICKUP",
]

STATUS_COLORS = {
    "WAITING_LABEL": "#f8d7da",
    "READY_TO_PRINT": "#f8d7da",
    "LABEL_RECEIVED": "#fff3cd",
    "IN_PRODUCTION": "#fff3cd",
    "PRINTING": "#fff3cd",
    "PACKAGING": "#fff3cd",
    "WAITING_FOR_PICKUP": "#fff3cd",
    "PRINTED": "#ffe5b4",
    "PRINT_DISPATCHED": "#ffe5b4",
    "SHIPPED": "#cfe2ff",
    "COMPLETED": "#d1e7dd",
    "REVIEW": "#d1e7dd",
    "CANCELED": "#e2e3e5",
    "ERROR": "#dc3545",
    "PRINT_ERROR": "#dc3545",
}

STATUS_TEXT_COLORS = {
    "WAITING_LABEL": "#721c24",
    "READY_TO_PRINT": "#721c24",
    "LABEL_RECEIVED": "#856404",
    "IN_PRODUCTION": "#856404",
    "PRINTING": "#856404",
    "PACKAGING": "#856404",
    "WAITING_FOR_PICKUP": "#856404",
    "PRINTED": "#8a5a00",
    "PRINT_DISPATCHED": "#8a5a00",
    "SHIPPED": "#0c5460",
    "COMPLETED": "#155724",
    "REVIEW": "#155724",
    "CANCELED": "#41464b",
    "ERROR": "#ffffff",
    "PRINT_ERROR": "#ffffff",
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
        return (
            current_start - pd.Timedelta(days=1),
            current_end - pd.Timedelta(days=1),
        )

    if period_mode == "Semana":
        return (
            current_start - pd.Timedelta(days=7),
            current_end - pd.Timedelta(days=7),
        )

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



def _get_pipeline_datetime_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="datetime64[ns]")

    for col in ["updatedAtParsed", "updatedAt", "shippedAtParsed", "shippedAt"]:
        if col in df.columns:
            series = pd.to_datetime(df[col], errors="coerce", utc=True)
            if series.notna().any():
                return series.dt.tz_convert(None)

    return pd.Series(dtype="datetime64[ns]")



def _build_comparison_df_day(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_pipeline_datetime_series(current_work)
    previous_work["_dt"] = _get_pipeline_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_series = current_work.groupby(current_work["_dt"].dt.hour).size()
    previous_series = previous_work.groupby(previous_work["_dt"].dt.hour).size()

    hours = list(range(24))
    labels = [f"{hour:02d}:00" for hour in hours]

    return pd.DataFrame(
        {
            "label": labels,
            "current_value": [int(current_series.get(hour, 0)) for hour in hours],
            "previous_value": [int(previous_series.get(hour, 0)) for hour in hours],
        }
    )



def _build_comparison_df_week(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
) -> pd.DataFrame:
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_pipeline_datetime_series(current_work)
    previous_work["_dt"] = _get_pipeline_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_series = current_work.groupby(current_work["_dt"].dt.weekday).size()
    previous_series = previous_work.groupby(previous_work["_dt"].dt.weekday).size()

    start_weekday = current_start.weekday()
    end_weekday = current_end.weekday()
    weekdays = list(range(start_weekday, end_weekday + 1))
    labels = [WEEKDAY_LABELS_PT[d] for d in weekdays]

    return pd.DataFrame(
        {
            "label": labels,
            "current_value": [int(current_series.get(day, 0)) for day in weekdays],
            "previous_value": [int(previous_series.get(day, 0)) for day in weekdays],
        }
    )



def _build_comparison_df_month(current_df: pd.DataFrame, previous_df: pd.DataFrame, current_end: pd.Timestamp) -> pd.DataFrame:
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_pipeline_datetime_series(current_work)
    previous_work["_dt"] = _get_pipeline_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_series = current_work.groupby(current_work["_dt"].dt.day).size()
    previous_series = previous_work.groupby(previous_work["_dt"].dt.day).size()

    days = list(range(1, current_end.day + 1))
    labels = [str(day) for day in days]

    return pd.DataFrame(
        {
            "label": labels,
            "current_value": [int(current_series.get(day, 0)) for day in days],
            "previous_value": [int(previous_series.get(day, 0)) for day in days],
        }
    )



def _build_comparison_df_fallback(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    previous_start: pd.Timestamp,
    previous_end: pd.Timestamp,
) -> pd.DataFrame:
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_pipeline_datetime_series(current_work)
    previous_work["_dt"] = _get_pipeline_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_series = current_work.groupby(current_work["_dt"].dt.normalize()).size()
    previous_series = previous_work.groupby(previous_work["_dt"].dt.normalize()).size()

    current_days = pd.date_range(start=current_start.normalize(), end=current_end.normalize(), freq="1D")
    previous_days = pd.date_range(start=previous_start.normalize(), end=previous_end.normalize(), freq="1D")

    slot_count = max(len(current_days), len(previous_days))
    labels = []
    current_values = []
    previous_values = []

    for i in range(slot_count):
        if i < len(current_days):
            labels.append(current_days[i].strftime("%d/%m"))
            current_values.append(int(current_series.get(current_days[i], 0)))
        else:
            labels.append(str(i + 1))
            current_values.append(0)

        if i < len(previous_days):
            previous_values.append(int(previous_series.get(previous_days[i], 0)))
        else:
            previous_values.append(0)

    return pd.DataFrame(
        {
            "label": labels,
            "current_value": current_values,
            "previous_value": previous_values,
        }
    )



def _build_comparison_df(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    previous_start: pd.Timestamp,
    previous_end: pd.Timestamp,
    period_mode: str,
) -> pd.DataFrame:
    if period_mode == "Dia":
        return _build_comparison_df_day(current_df, previous_df)

    if period_mode == "Semana":
        return _build_comparison_df_week(current_df, previous_df, current_start, current_end)

    if period_mode == "Mês":
        return _build_comparison_df_month(current_df, previous_df, current_end)

    return _build_comparison_df_fallback(
        current_df=current_df,
        previous_df=previous_df,
        current_start=current_start,
        current_end=current_end,
        previous_start=previous_start,
        previous_end=previous_end,
    )



def _render_pipeline_comparison_chart(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    previous_start: pd.Timestamp,
    previous_end: pd.Timestamp,
    period_mode: str,
    title: str,
) -> None:
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



def _build_status_chart_df(shipment_df: pd.DataFrame) -> pd.DataFrame:
    df = status_breakdown(shipment_df)

    if df.empty:
        return df

    work = df.copy()
    work["status"] = work["status"].astype(str).str.upper()
    work["color"] = work["status"].map(STATUS_COLORS).fillna("#adb5bd")
    return work



def _render_status_bar_chart(df: pd.DataFrame, title: str) -> None:
    if df.empty or "status" not in df.columns or "count" not in df.columns:
        st.info("Sem dados para apresentar.")
        return

    fig = px.bar(
        df,
        x="status",
        y="count",
        title=title,
        color="status",
        color_discrete_map=STATUS_COLORS,
    )

    fig.update_layout(
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis_title=None,
        yaxis_title=None,
        height=350,
        showlegend=False,
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

st.title("Shipment Status")

sales_df = load_collection_df(
    "sales",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="updatedAt",
    date_range=date_range,
)

if not sales_df.empty and "status" in sales_df.columns:
    shipment_df = sales_df[sales_df["status"].isin(SHIPMENT_STATUSES)].copy()
else:
    shipment_df = sales_df.copy()

current_start, current_end = _extract_current_range(date_range)
previous_shipment_df = pd.DataFrame()
previous_start, previous_end = None, None

if current_start is not None and current_end is not None:
    previous_start, previous_end = _previous_range_for_mode(current_start, current_end, period_mode)
    previous_sales_df = load_collection_df(
        "sales",
        account_id=account_filter,
        allowed_account_ids=allowed_account_ids,
        date_field="updatedAt",
        date_range=_range_dict(previous_start, previous_end),
    )

    if not previous_sales_df.empty and "status" in previous_sales_df.columns:
        previous_shipment_df = previous_sales_df[
            previous_sales_df["status"].isin(SHIPMENT_STATUSES)
        ].copy()

if not shipment_df.empty:
    if "updatedAtParsed" in shipment_df.columns:
        shipment_df = shipment_df.sort_values("updatedAtParsed", ascending=False, na_position="last")
    elif "updatedAt" in shipment_df.columns:
        shipment_df = shipment_df.sort_values("updatedAt", ascending=False, na_position="last")

if not previous_shipment_df.empty:
    if "updatedAtParsed" in previous_shipment_df.columns:
        previous_shipment_df = previous_shipment_df.sort_values("updatedAtParsed", ascending=False, na_position="last")
    elif "updatedAt" in previous_shipment_df.columns:
        previous_shipment_df = previous_shipment_df.sort_values("updatedAt", ascending=False, na_position="last")

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

left, right = st.columns([1.3, 1])

with left:
    if (
        current_start is not None
        and current_end is not None
        and previous_start is not None
        and previous_end is not None
    ):
        _render_pipeline_comparison_chart(
            current_df=shipment_df,
            previous_df=previous_shipment_df,
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
            period_mode=period_mode,
            title="Movimento do pipeline",
        )
    else:
        st.info("Sem dados para apresentar.")

with right:
    status_chart_df = _build_status_chart_df(shipment_df)
    _render_status_bar_chart(status_chart_df, "Distribuição por estado")

st.subheader("Tabela de shipment status")
visible_cols = [
    c for c in [
        "updatedAt",
        "accountId",
        "articleTitle",
        "shippingCode",
        "status",
        "completedAt",
    ] if c in shipment_df.columns
]

table_df = shipment_df[visible_cols].copy() if visible_cols else shipment_df.copy()
grid_df = _sanitize_for_aggrid(table_df)

gb = GridOptionsBuilder.from_dataframe(grid_df)
gb.configure_default_column(resizable=True, sortable=True, filter=True)
gb.configure_grid_options(
    rowHeight=40,
    animateRows=False,
    suppressRowClickSelection=True,
    suppressCellFocus=False,
)

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


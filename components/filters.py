from components.layout import apply_page_layout

apply_page_layout()

import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from components.auth_guard import render_user_box, require_login
from components.filters import get_active_filters, render_global_filters
from components.tables import render_dataframe, render_kpis
from services.firestore_queries import (
    load_collection_df,
    load_label_by_id,
    load_sale_items_for_sale,
    update_sale_status,
)
from services.metrics import (
    format_currency,
    safe_count,
    safe_sum,
    status_breakdown,
    time_series,
)

STATUS_OPTIONS = [
    "WAITING_LABEL",
    "LABEL_RECEIVED",
    "READY_TO_PRINT",
    "PRINTING",
    "PRINTED",
    "PRINT_DISPATCHED",
    "SHIPPED",
    "COMPLETED",
    "REVIEW",
    "ERROR",
    "CANCELED",
    "PRINT_ERROR",
    "IN_PRODUCTION",
    "PACKAGING",
]

STATUS_COLORS = {
    "WAITING_LABEL": "#f8d7da",
    "READY_TO_PRINT": "#f8d7da",
    "IN_PRODUCTION": "#f8d7da",
    "PRINT_DISPATCHED": "#fff3cd",
    "PACKAGING": "#fff3cd",
    "SHIPPED": "#cfe2ff",
    "COMPLETED": "#d1e7dd",
    "CANCELED": "#e2e3e5",
}

STATUS_TEXT_COLORS = {
    "PRINT_DISPATCHED": "#856404",
    "PACKAGING": "#856404",
    "IN_PRODUCTION": "#856404",
    "CANCELED": "#41464b",
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


def _update_sale_statuses(original_df: pd.DataFrame, edited_df: pd.DataFrame) -> int:
    if original_df.empty or edited_df.empty:
        return 0

    if "saleId" not in original_df.columns or "saleId" not in edited_df.columns:
        return 0

    original = original_df[["saleId", "status"]].copy()
    current = edited_df[["saleId", "status"]].copy()

    original["saleId"] = original["saleId"].astype(str)
    current["saleId"] = current["saleId"].astype(str)

    merged = original.merge(
        current,
        on="saleId",
        suffixes=("_old", "_new"),
    )

    changed_rows = [
        {
            "saleId": row["saleId"],
            "status": row["status_new"],
        }
        for _, row in merged.iterrows()
        if str(row["status_old"]) != str(row["status_new"])
    ]

    if not changed_rows:
        return 0

    updated = 0

    for row in changed_rows:
        sale_id = row.get("saleId")
        new_status = row.get("status")

        if not sale_id or not new_status:
            continue

        update_sale_status(str(sale_id), str(new_status).strip())
        updated += 1

    return updated


def _build_label_table(label: dict) -> pd.DataFrame:
    if not label:
        return pd.DataFrame()

    row = {
        "shippingCode": label.get("shippingCode"),
        "articleTitle": label.get("articleTitle"),
        "status": label.get("status"),
        "matchedSaleId": label.get("matchedSaleId"),
        "matchedBy": label.get("matchedBy"),
        "localEnrichedPdfPath": label.get("localEnrichedPdfPath"),
    }

    return pd.DataFrame([row])


def _build_status_chart_df(sales_df: pd.DataFrame) -> pd.DataFrame:
    df = status_breakdown(sales_df)

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


def _extract_date_range(date_range):
    if not isinstance(date_range, dict):
        return None, None

    start = pd.to_datetime(date_range.get("start"), errors="coerce", utc=True)
    end = pd.to_datetime(date_range.get("end"), errors="coerce", utc=True)

    if pd.isna(start) or pd.isna(end):
        return None, None

    start = pd.Timestamp(start)
    end = pd.Timestamp(end)

    if start > end:
        start, end = end, start

    return start, end


def _get_period_mode() -> str:
    period_mode = st.session_state.get("period_mode", "Mês")
    return str(period_mode)


def _shift_period_back(start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    duration = end - start
    previous_end = start - pd.Timedelta(microseconds=1)
    previous_start = previous_end - duration
    return previous_start, previous_end


def _get_datetime_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="datetime64[ns, UTC]")

    if "soldAtParsed" in df.columns:
        series = pd.to_datetime(df["soldAtParsed"], errors="coerce", utc=True)
        if series.notna().any():
            return series

    if "soldAt" in df.columns:
        series = pd.to_datetime(df["soldAt"], errors="coerce", utc=True)
        if series.notna().any():
            return series

    return pd.Series(dtype="datetime64[ns, UTC]")


def _get_numeric_value_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")

    if "value" in df.columns:
        return pd.to_numeric(df["value"], errors="coerce").fillna(0.0)

    return pd.Series([0.0] * len(df), index=df.index, dtype="float64")


def _chart_bucket_for_period_mode(period_mode: str) -> str:
    if period_mode == "Dia":
        return "hour"

    return "day"


def _make_bucket_start(series: pd.Series, bucket: str) -> pd.Series:
    series = pd.to_datetime(series, errors="coerce", utc=True)

    if bucket == "hour":
        return pd.to_datetime(series.dt.strftime("%Y-%m-%d %H:00:00"), utc=True)

    return series.dt.normalize()


def _single_bucket_start(ts: pd.Timestamp, bucket: str) -> pd.Timestamp:
    ts = pd.Timestamp(ts)

    if bucket == "hour":
        return pd.Timestamp(
            year=ts.year,
            month=ts.month,
            day=ts.day,
            hour=ts.hour,
            tz=ts.tz,
        )

    return pd.Timestamp(
        year=ts.year,
        month=ts.month,
        day=ts.day,
        tz=ts.tz,
    )


def _make_period_index(start: pd.Timestamp, end: pd.Timestamp, bucket: str) -> pd.DatetimeIndex:
    range_start = _single_bucket_start(start, bucket)
    range_end = _single_bucket_start(end, bucket)

    if bucket == "hour":
        return pd.date_range(start=range_start, end=range_end, freq="1h")

    return pd.date_range(start=range_start, end=range_end, freq="1D")


def _make_labels(index: pd.DatetimeIndex, bucket: str) -> list[str]:
    if bucket == "hour":
        return [ts.strftime("%H:%M") for ts in index]

    return [ts.strftime("%d/%m") for ts in index]


def _build_aligned_period_series(
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
        current_start: pd.Timestamp,
        current_end: pd.Timestamp,
        previous_start: pd.Timestamp,
        previous_end: pd.Timestamp,
        period_mode: str,
) -> pd.DataFrame:
    bucket = _chart_bucket_for_period_mode(period_mode)

    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["value"] = _get_numeric_value_series(current_work)
    previous_work["value"] = _get_numeric_value_series(previous_work)


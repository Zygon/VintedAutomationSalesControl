from components.layout import apply_page_layout

apply_page_layout()

import json
import time

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
    "WAITING_FOR_PICKUP",
]

EDITABLE_STATUS_OPTIONS = [
    "WAITING_FOR_PICKUP",
    "IN_PRODUCTION",
    "PACKAGING",
    "SHIPPED",
    "CANCELED",
    "COMPLETED",
]

STATUS_COLORS = {
    "WAITING_LABEL": "#f8d7da",
    "READY_TO_PRINT": "#f8d7da",
    "IN_PRODUCTION": "#f8d7da",
    "PRINT_DISPATCHED": "#fff3cd",
    "PACKAGING": "#fff3cd",
    "WAITING_FOR_PICKUP": "#ffe5b4",
    "SHIPPED": "#cfe2ff",
    "COMPLETED": "#d1e7dd",
    "CANCELED": "#e2e3e5",
}

STATUS_TEXT_COLORS = {
    "PRINT_DISPATCHED": "#856404",
    "PACKAGING": "#856404",
    "IN_PRODUCTION": "#856404",
    "WAITING_FOR_PICKUP": "#8a5a00",
    "CANCELED": "#41464b",
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

DETAIL_COLUMN_ID = "__view_detail__"
DETAIL_TRIGGER_COLUMN_ID = "__detail_trigger__"
DETAIL_MODAL_STATE_KEY = "sales_detail_modal_sale_id"
DETAIL_MODAL_NONCE_KEY = "sales_detail_modal_nonce"


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


def _get_datetime_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="datetime64[ns]")

    if "soldAtParsed" in df.columns:
        series = pd.to_datetime(df["soldAtParsed"], errors="coerce", utc=True)
        if series.notna().any():
            return series.dt.tz_convert(None)

    if "soldAt" in df.columns:
        series = pd.to_datetime(df["soldAt"], errors="coerce", utc=True)
        if series.notna().any():
            return series.dt.tz_convert(None)

    return pd.Series(dtype="datetime64[ns]")


def _get_numeric_value_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")

    if "value" in df.columns:
        return pd.to_numeric(df["value"], errors="coerce").fillna(0)

    return pd.Series([0.0] * len(df), index=df.index, dtype="float64")


def _build_comparison_df_day(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    previous_start: pd.Timestamp,
    previous_end: pd.Timestamp,
) -> pd.DataFrame:
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

    return pd.DataFrame(
        {
            "label": labels,
            "current_value": [float(current_series.get(hour, 0)) for hour in hours],
            "previous_value": [float(previous_series.get(hour, 0)) for hour in hours],
        }
    )


def _build_comparison_df_week(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    previous_start: pd.Timestamp,
    previous_end: pd.Timestamp,
) -> pd.DataFrame:
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

    return pd.DataFrame(
        {
            "label": labels,
            "current_value": [float(current_series.get(day, 0)) for day in weekdays],
            "previous_value": [float(previous_series.get(day, 0)) for day in weekdays],
        }
    )


def _build_comparison_df_month(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    previous_start: pd.Timestamp,
    previous_end: pd.Timestamp,
) -> pd.DataFrame:
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

    return pd.DataFrame(
        {
            "label": labels,
            "current_value": [float(current_series.get(day, 0)) for day in days],
            "previous_value": [float(previous_series.get(day, 0)) for day in days],
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
        return _build_comparison_df_day(
            current_df=current_df,
            previous_df=previous_df,
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
        )

    if period_mode == "Semana":
        return _build_comparison_df_week(
            current_df=current_df,
            previous_df=previous_df,
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
        )

    if period_mode == "Mês":
        return _build_comparison_df_month(
            current_df=current_df,
            previous_df=previous_df,
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
        )

    return _build_comparison_df_fallback(
        current_df=current_df,
        previous_df=previous_df,
        current_start=current_start,
        current_end=current_end,
        previous_start=previous_start,
        previous_end=previous_end,
    )


def _render_sales_comparison_chart(
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


@st.dialog("Detalhe da venda", width="large")
def _render_sale_detail_modal(
    sale_id: str,
    sales_df: pd.DataFrame,
    account_filter,
    allowed_account_ids,
) -> None:
    selected_sale = sales_df[sales_df["saleId"].astype(str) == str(sale_id)]

    if selected_sale.empty:
        st.warning("Não foi possível localizar a venda selecionada.")
    else:
        sale_row = selected_sale.iloc[0].to_dict()

        summary_cols = st.columns(4)
        with summary_cols[0]:
            st.metric("Order ID", sale_row.get("orderId") or "-")
        with summary_cols[1]:
            st.metric("Status", sale_row.get("status") or "-")
        with summary_cols[2]:
            value = sale_row.get("value")
            st.metric("Valor", format_currency(float(value)) if value is not None else "-")
        with summary_cols[3]:
            st.metric("SKU", sale_row.get("sku") or "-")

        st.markdown("#### saleItems")
        sale_items_df = load_sale_items_for_sale(
            str(sale_id),
            account_id=account_filter,
            allowed_account_ids=allowed_account_ids,
        )

        if sale_items_df.empty:
            st.info("Sem saleItems para esta venda.")
        else:
            sale_items_visible_cols = [
                c for c in [
                    "articleTitle",
                    "sku",
                    "allocatedValue",
                    "currency",
                    "shippingCode",
                    "buyerUsername",
                ] if c in sale_items_df.columns
            ]
            render_dataframe(sale_items_df[sale_items_visible_cols] if sale_items_visible_cols else sale_items_df)

        st.markdown("#### Label associada")
        matched_label_id = sale_row.get("matchedLabelId")
        if not matched_label_id:
            st.info("Esta venda ainda não tem label associada.")
        else:
            label = load_label_by_id(
                str(matched_label_id),
                account_id=account_filter,
                allowed_account_ids=allowed_account_ids,
            )

            if not label:
                st.warning("Label associada não encontrada.")
            else:
                label_df = _build_label_table(label)
                render_dataframe(label_df)

    st.divider()
    if st.button("Fechar", key=f"close_sale_detail_modal_{sale_id}"):
        st.session_state[DETAIL_MODAL_STATE_KEY] = None
        st.session_state[DETAIL_MODAL_NONCE_KEY] = None
        st.rerun()


user = require_login()
render_user_box(user)
render_global_filters(user)

filters = get_active_filters()
selected_account_id = filters["accountId"]
allowed_account_ids = filters["allowedAccountIds"]
account_filter = None if selected_account_id in (None, "__ALL__") else selected_account_id
date_range = filters["dateRange"]
period_mode = st.session_state.get("period_mode", "Mês")

st.title("Vendas")

sales_df = load_collection_df(
    "sales",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="soldAt",
    date_range=date_range,
)

current_start, current_end = _extract_current_range(date_range)
previous_sales_df = pd.DataFrame()
previous_start, previous_end = None, None

if current_start is not None and current_end is not None:
    previous_start, previous_end = _previous_range_for_mode(current_start, current_end, period_mode)
    previous_sales_df = load_collection_df(
        "sales",
        account_id=account_filter,
        allowed_account_ids=allowed_account_ids,
        date_field="soldAt",
        date_range=_range_dict(previous_start, previous_end),
    )

if not sales_df.empty:
    if "soldAtParsed" in sales_df.columns:
        sales_df = sales_df.sort_values("soldAtParsed", ascending=False, na_position="last")
    elif "soldAt" in sales_df.columns:
        sales_df = sales_df.sort_values("soldAt", ascending=False, na_position="last")

if not previous_sales_df.empty:
    if "soldAtParsed" in previous_sales_df.columns:
        previous_sales_df = previous_sales_df.sort_values("soldAtParsed", ascending=False, na_position="last")
    elif "soldAt" in previous_sales_df.columns:
        previous_sales_df = previous_sales_df.sort_values("soldAt", ascending=False, na_position="last")

valid_sales_df = sales_df[
    sales_df["status"] != "CANCELED"
] if not sales_df.empty and "status" in sales_df.columns else sales_df

canceled_sales_df = sales_df[
    sales_df["status"] == "CANCELED"
] if not sales_df.empty and "status" in sales_df.columns else pd.DataFrame()

render_kpis([
    ("Registos", str(safe_count(sales_df))),
    ("Vendas válidas", str(safe_count(valid_sales_df))),
    ("Receita válida", format_currency(safe_sum(valid_sales_df, "value"))),
    ("Canceladas", str(safe_count(canceled_sales_df))),
])

left, right = st.columns([1.3, 1])

with left:
    if (
        current_start is not None
        and current_end is not None
        and previous_start is not None
        and previous_end is not None
    ):
        _render_sales_comparison_chart(
            current_df=sales_df,
            previous_df=previous_sales_df,
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
            period_mode=period_mode,
            title="Valor de vendas por período",
        )
    else:
        series_df = (
            time_series(sales_df, "soldAtParsed", "value")
            if not sales_df.empty
            else pd.DataFrame()
        )

        if series_df.empty:
            st.info("Sem dados para apresentar.")
        else:
            fig = px.line(
                series_df,
                x="period",
                y="value",
                title="Valor de vendas por período",
            )
            fig.update_layout(
                margin=dict(l=10, r=10, t=50, b=10),
                xaxis_title=None,
                yaxis_title=None,
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)

with right:
    status_chart_df = _build_status_chart_df(sales_df)
    _render_status_bar_chart(status_chart_df, "Estados das vendas")

st.subheader("Tabela de vendas")

visible_columns = [
    c for c in [
        "soldAt",
        "articleTitle",
        "sku",
        "value",
        "status",
        "orderId",
        "accountId",
        "buyerUsername",
    ] if c in sales_df.columns
]

support_columns = [
    c for c in ["saleId", "matchedLabelId"] if c in sales_df.columns
]

table_df = sales_df[support_columns + visible_columns].copy()
grid_df = _sanitize_for_aggrid(table_df)
grid_df.insert(0, DETAIL_TRIGGER_COLUMN_ID, "")
grid_df.insert(0, DETAIL_COLUMN_ID, "")

gb = GridOptionsBuilder.from_dataframe(grid_df)

gb.configure_default_column(
    resizable=True,
    sortable=True,
    filter=True,
)

detail_button_renderer = JsCode(
    f"""
    class DetailButtonRenderer {{
        init(params) {{
            this.params = params;
            this.eGui = document.createElement('button');
            this.eGui.innerHTML = '{"👁"}';
            this.eGui.setAttribute('title', 'Ver detalhe');
            this.eGui.style.width = '100%';
            this.eGui.style.height = '30px';
            this.eGui.style.border = 'none';
            this.eGui.style.background = 'transparent';
            this.eGui.style.cursor = 'pointer';
            this.eGui.style.fontSize = '18px';
            this.eGui.style.lineHeight = '1';
            this.eGui.addEventListener('click', () => {{
                const token = String(params.data.saleId || '') + '__' + String(Date.now());
                params.node.setDataValue('{DETAIL_TRIGGER_COLUMN_ID}', token);
            }});
        }}
        getGui() {{
            return this.eGui;
        }}
    }}
    """
)

gb.configure_column(
    DETAIL_COLUMN_ID,
    header_name="",
    editable=False,
    sortable=False,
    filter=False,
    width=55,
    pinned="left",
    lockPosition=True,
    suppressMenu=True,
    cellRenderer=detail_button_renderer,
    cellStyle={
        "textAlign": "center",
        "paddingLeft": "0px",
        "paddingRight": "0px",
    },
)

gb.configure_column(DETAIL_TRIGGER_COLUMN_ID, hide=True)
gb.configure_column("saleId", hide=True)
gb.configure_column("matchedLabelId", hide=True)

if "status" in grid_df.columns:
    gb.configure_column(
        "status",
        editable=True,
        cellEditor="agSelectCellEditor",
        cellEditorParams={"values": EDITABLE_STATUS_OPTIONS},
    )

if "value" in grid_df.columns:
    gb.configure_column("value", type=["numericColumn"])

gb.configure_grid_options(
    rowHeight=40,
    animateRows=False,
    suppressRowClickSelection=True,
    rowSelection="single",
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

grid_response = AgGrid(
    grid_df,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.VALUE_CHANGED,
    allow_unsafe_jscode=True,
    fit_columns_on_grid_load=False,
    use_container_width=True,
    height=550,
    theme="streamlit",
    reload_data=False,
)

edited_df = pd.DataFrame(grid_response["data"])
modal_sale_id = None
modal_nonce = None

if not edited_df.empty and DETAIL_TRIGGER_COLUMN_ID in edited_df.columns:
    trigger_rows = edited_df[
        edited_df[DETAIL_TRIGGER_COLUMN_ID].astype(str).str.contains("__", na=False)
    ]

    if not trigger_rows.empty:
        trigger_value = str(trigger_rows.iloc[0][DETAIL_TRIGGER_COLUMN_ID])
        sale_id_part, _, nonce_part = trigger_value.partition("__")
        if sale_id_part and nonce_part:
            modal_sale_id = sale_id_part
            modal_nonce = trigger_value

if not edited_df.empty:
    for helper_col in [DETAIL_COLUMN_ID, DETAIL_TRIGGER_COLUMN_ID]:
        if helper_col in edited_df.columns:
            edited_df = edited_df.drop(columns=[helper_col])

save_col, info_col = st.columns([1, 3])

with save_col:
    if st.button("Guardar alterações de status"):
        updated_count = _update_sale_statuses(table_df, edited_df)

        if updated_count > 0:
            st.success(f"{updated_count} venda(s) atualizada(s).")
            st.rerun()
        else:
            st.info("Não houve alterações de status para guardar.")

with info_col:
    st.caption(
        "Cores: vermelho = WAITING_LABEL / READY_TO_PRINT / IN_PRODUCTION | "
        "amarelo = PRINT_DISPATCHED / PACKAGING / WAITING_FOR_PICKUP | "
        "azul = SHIPPED | verde = COMPLETED | cinzento = CANCELED"
    )

if modal_nonce and modal_nonce != st.session_state.get(DETAIL_MODAL_NONCE_KEY):
    st.session_state[DETAIL_MODAL_STATE_KEY] = modal_sale_id
    st.session_state[DETAIL_MODAL_NONCE_KEY] = modal_nonce

current_modal_sale_id = st.session_state.get(DETAIL_MODAL_STATE_KEY)
if current_modal_sale_id:
    _render_sale_detail_modal(
        sale_id=str(current_modal_sale_id),
        sales_df=sales_df,
        account_filter=account_filter,
        allowed_account_ids=allowed_account_ids,
    )
else:
    st.caption("Clica no olho para abrir o detalhe da venda em modal.")

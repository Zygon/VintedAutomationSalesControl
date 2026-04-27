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
    "CANCELED": "#e2e5e5",
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
    if date_range is None:
        return None, None

    start_raw = None
    end_raw = None

    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_raw, end_raw = date_range[0], date_range[1]

    elif isinstance(date_range, dict):
        for start_key, end_key in [
            ("start", "end"),
            ("from", "to"),
            ("date_from", "date_to"),
            ("startDate", "endDate"),
            ("start_date", "end_date"),
        ]:
            if start_key in date_range and end_key in date_range:
                start_raw = date_range.get(start_key)
                end_raw = date_range.get(end_key)
                break

    if start_raw is None or end_raw is None:
        return None, None

    start = pd.to_datetime(start_raw, errors="coerce")
    end = pd.to_datetime(end_raw, errors="coerce")

    if pd.isna(start) or pd.isna(end):
        return None, None

    start = pd.Timestamp(start)
    end = pd.Timestamp(end)

    if start > end:
        start, end = end, start

    if (
        end.hour == 0
        and end.minute == 0
        and end.second == 0
        and end.microsecond == 0
    ):
        end = end + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    return start, end


def _shift_date_range_back(start: pd.Timestamp, end: pd.Timestamp):
    duration = end - start
    previous_end = start - pd.Timedelta(microseconds=1)
    previous_start = previous_end - duration
    return previous_start, previous_end


def _get_datetime_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="datetime64[ns]")

    if "soldAtParsed" in df.columns:
        series = pd.to_datetime(df["soldAtParsed"], errors="coerce")
        if series.notna().any():
            return series

    if "soldAt" in df.columns:
        series = pd.to_datetime(df["soldAt"], errors="coerce")
        if series.notna().any():
            return series

    return pd.Series(dtype="datetime64[ns]")


def _infer_bucket_config(start: pd.Timestamp, end: pd.Timestamp):
    total_seconds = max((end - start).total_seconds(), 1)

    if total_seconds <= 36 * 3600:
        return {
            "bucket": "hour",
            "label_format": "%H:%M",
        }

    if total_seconds <= 62 * 24 * 3600:
        return {
            "bucket": "day",
            "label_format": "%d/%m",
        }

    return {
        "bucket": "week",
        "label_format": "%d/%m",
    }


def _week_start(series: pd.Series) -> pd.Series:
    series = pd.to_datetime(series, errors="coerce")
    return series.dt.normalize() - pd.to_timedelta(series.dt.weekday, unit="D")


def _truncate_to_hour(series: pd.Series) -> pd.Series:
    series = pd.to_datetime(series, errors="coerce")
    return series.dt.strftime("%Y-%m-%d %H:00:00").pipe(pd.to_datetime, errors="coerce")


def _make_bucket_start(series: pd.Series, bucket: str) -> pd.Series:
    series = pd.to_datetime(series, errors="coerce")

    if bucket == "hour":
        return _truncate_to_hour(series)

    if bucket == "day":
        return series.dt.normalize()

    if bucket == "week":
        return _week_start(series)

    return series.dt.normalize()


def _single_bucket_start(ts: pd.Timestamp, bucket: str) -> pd.Timestamp:
    ts = pd.Timestamp(ts)

    if bucket == "hour":
        return pd.Timestamp(
            year=ts.year,
            month=ts.month,
            day=ts.day,
            hour=ts.hour,
        )

    if bucket == "day":
        return pd.Timestamp(
            year=ts.year,
            month=ts.month,
            day=ts.day,
        )

    if bucket == "week":
        normalized = pd.Timestamp(year=ts.year, month=ts.month, day=ts.day)
        return normalized - pd.Timedelta(days=normalized.weekday())

    return pd.Timestamp(year=ts.year, month=ts.month, day=ts.day)


def _make_period_index(start: pd.Timestamp, end: pd.Timestamp, bucket: str) -> pd.DatetimeIndex:
    range_start = _single_bucket_start(start, bucket)
    range_end = _single_bucket_start(end, bucket)

    if bucket == "hour":
        step = "1h"
    elif bucket == "day":
        step = "1D"
    elif bucket == "week":
        step = "7D"
    else:
        step = "1D"

    return pd.date_range(start=range_start, end=range_end, freq=step)


def _make_labels(index: pd.DatetimeIndex, bucket: str, label_format: str) -> list[str]:
    labels = []

    for ts in index:
        if bucket == "week":
            labels.append(f"Semana {ts.strftime(label_format)}")
        else:
            labels.append(ts.strftime(label_format))

    return labels


def _build_aligned_period_series(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    previous_start: pd.Timestamp,
    previous_end: pd.Timestamp,
) -> pd.DataFrame:
    config = _infer_bucket_config(current_start, current_end)
    bucket = config["bucket"]
    label_format = config["label_format"]

    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)

    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["value"] = pd.to_numeric(current_work.get("value", 0), errors="coerce").fillna(0)
    previous_work["value"] = pd.to_numeric(previous_work.get("value", 0), errors="coerce").fillna(0)

    current_index = _make_period_index(current_start, current_end, bucket)
    previous_index = _make_period_index(previous_start, previous_end, bucket)

    if len(current_index) == 0:
        current_index = pd.DatetimeIndex([_single_bucket_start(current_start, bucket)])

    if len(previous_index) == 0:
        previous_index = pd.DatetimeIndex([_single_bucket_start(previous_start, bucket)])

    current_work["_bucket_start"] = _make_bucket_start(current_work["_dt"], bucket)
    previous_work["_bucket_start"] = _make_bucket_start(previous_work["_dt"], bucket)

    current_series = (
        current_work.groupby("_bucket_start", dropna=True)["value"]
        .sum()
        .reindex(current_index, fill_value=0)
    )

    previous_series = (
        previous_work.groupby("_bucket_start", dropna=True)["value"]
        .sum()
        .reindex(previous_index, fill_value=0)
    )

    slot_count = max(len(current_series), len(previous_series))

    current_values = list(current_series.values)
    previous_values = list(previous_series.values)

    if len(current_values) < slot_count:
        current_values.extend([0] * (slot_count - len(current_values)))

    if len(previous_values) < slot_count:
        previous_values.extend([0] * (slot_count - len(previous_values)))

    if len(current_index) < slot_count:
        last = current_index[-1] if len(current_index) > 0 else _single_bucket_start(current_start, bucket)

        if bucket == "hour":
            step = pd.Timedelta(hours=1)
        elif bucket == "day":
            step = pd.Timedelta(days=1)
        else:
            step = pd.Timedelta(days=7)

        extra = [last + step * (i + 1) for i in range(slot_count - len(current_index))]
        current_index = current_index.append(pd.DatetimeIndex(extra))

    labels = _make_labels(current_index[:slot_count], bucket, label_format)

    return pd.DataFrame(
        {
            "label": labels,
            "current_value": current_values[:slot_count],
            "previous_value": previous_values[:slot_count],
        }
    )


def _render_sales_comparison_chart(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    previous_start: pd.Timestamp,
    previous_end: pd.Timestamp,
    title: str,
) -> None:
    aligned_df = _build_aligned_period_series(
        current_df=current_df,
        previous_df=previous_df,
        current_start=current_start,
        current_end=current_end,
        previous_start=previous_start,
        previous_end=previous_end,
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
date_range = filters.get("dateRange")

st.title("Vendas")

sales_df = load_collection_df(
    "sales",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="soldAt",
    date_range=date_range,
)

current_start, current_end = _extract_date_range(date_range)
previous_sales_df = pd.DataFrame()
previous_start, previous_end = None, None

if current_start is not None and current_end is not None:
    previous_start, previous_end = _shift_date_range_back(current_start, current_end)
    previous_sales_df = load_collection_df(
        "sales",
        account_id=account_filter,
        allowed_account_ids=allowed_account_ids,
        date_field="soldAt",
        date_range=(previous_start, previous_end),
    )

if not sales_df.empty:
    if "soldAtParsed" in sales_df.columns:
        sales_df = sales_df.sort_values("soldAtParsed", ascending=False, na_position="last")
    elif "soldAt" in sales_df.columns:
        sales_df = sales_df.sort_values("soldAt", ascending=False, na_position="last")

valid_sales_df = (
    sales_df[sales_df["status"] != "CANCELED"]
    if not sales_df.empty and "status" in sales_df.columns
    else sales_df
)

canceled_sales_df = (
    sales_df[sales_df["status"] == "CANCELED"]
    if not sales_df.empty and "status" in sales_df.columns
    else pd.DataFrame()
)

render_kpis(
    [
        ("Registos", str(safe_count(sales_df))),
        ("Vendas válidas", str(safe_count(valid_sales_df))),
        ("Receita válida", format_currency(safe_sum(valid_sales_df, "value"))),
        ("Canceladas", str(safe_count(canceled_sales_df))),
    ]
)

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

table_columns = [
    c
    for c in [
        "saleId",
        "soldAt",
        "articleTitle",
        "sku",
        "value",
        "status",
        "orderId",
        "accountId",
        "buyerUsername",
        "matchedLabelId",
    ]
    if c in sales_df.columns
]

table_df = sales_df[table_columns].copy()
grid_df = _sanitize_for_aggrid(table_df)

gb = GridOptionsBuilder.from_dataframe(grid_df)

gb.configure_default_column(
    resizable=True,
    sortable=True,
    filter=True,
)

gb.configure_column("saleId", hide=True)
gb.configure_column("matchedLabelId", hide=True)

if "status" in grid_df.columns:
    gb.configure_column(
        "status",
        editable=True,
        cellEditor="agSelectCellEditor",
        cellEditorParams={"values": STATUS_OPTIONS},
    )

if "value" in grid_df.columns:
    gb.configure_column("value", type=["numericColumn"])

gb.configure_selection(
    selection_mode="single",
    use_checkbox=False,
)

gb.configure_grid_options(
    rowHeight=40,
    animateRows=False,
)

status_colors_js = json.dumps(STATUS_COLORS)
status_text_colors_js = json.dumps(STATUS_TEXT_COLORS)

row_style_jscode = JsCode(
    f"""
    function(params) {{
        const status = String((params.data && params.data.status) || "").toUpperCase();
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
    update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED,
    allow_unsafe_jscode=True,
    fit_columns_on_grid_load=False,
    use_container_width=True,
    height=550,
    theme="streamlit",
    reload_data=False,
)

edited_df = pd.DataFrame(grid_response["data"])

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
        "amarelo = PRINT_DISPATCHED / PACKAGING | "
        "azul = SHIPPED | verde = COMPLETED | cinzento = CANCELED"
    )

selected_rows = grid_response.get("selected_rows", [])
selected_sale_id = None

if isinstance(selected_rows, pd.DataFrame) and not selected_rows.empty:
    selected_sale_id = str(selected_rows.iloc[0].get("saleId"))
elif isinstance(selected_rows, list) and len(selected_rows) > 0:
    selected_sale_id = str(selected_rows[0].get("saleId"))

st.subheader("Detalhe da venda selecionada")

if not selected_sale_id:
    st.info("Seleciona uma linha na tabela de vendas para ver os saleItems e a label associada.")
else:
    selected_sale = sales_df[sales_df["saleId"].astype(str) == selected_sale_id]

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
            selected_sale_id,
            account_id=account_filter,
            allowed_account_ids=allowed_account_ids,
        )

        if sale_items_df.empty:
            st.info("Sem saleItems para esta venda.")
        else:
            sale_items_visible_cols = [
                c
                for c in [
                    "articleTitle",
                    "sku",
                    "allocatedValue",
                    "currency",
                    "shippingCode",
                    "buyerUsername",
                ]
                if c in sale_items_df.columns
            ]
            render_dataframe(
                sale_items_df[sale_items_visible_cols]
                if sale_items_visible_cols
                else sale_items_df
            )

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
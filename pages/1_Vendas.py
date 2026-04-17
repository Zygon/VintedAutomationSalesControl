from components.layout import apply_page_layout

apply_page_layout()

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from components.auth_guard import render_user_box, require_login
from components.charts import render_bar_chart, render_line_chart
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
    safe_avg,
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
]


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


user = require_login()
render_user_box(user)
render_global_filters(user)

filters = get_active_filters()
selected_account_id = filters["accountId"]
allowed_account_ids = filters["allowedAccountIds"]
account_filter = None if selected_account_id in (None, "__ALL__") else selected_account_id
date_range = filters["dateRange"]

st.title("Vendas")

sales_df = load_collection_df(
    "sales",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
    date_field="soldAt",
    date_range=date_range,
)

if not sales_df.empty:
    if "soldAtParsed" in sales_df.columns:
        sales_df = sales_df.sort_values("soldAtParsed", ascending=False, na_position="last")
    elif "soldAt" in sales_df.columns:
        sales_df = sales_df.sort_values("soldAt", ascending=False, na_position="last")

valid_sales_df = sales_df[
    sales_df["status"] != "CANCELED"] if not sales_df.empty and "status" in sales_df.columns else sales_df
canceled_sales_df = sales_df[
    sales_df["status"] == "CANCELED"] if not sales_df.empty and "status" in sales_df.columns else pd.DataFrame()

render_kpis([
    ("Registos", str(safe_count(sales_df))),
    ("Vendas válidas", str(safe_count(valid_sales_df))),
    ("Receita válida", format_currency(safe_sum(valid_sales_df, "value"))),
    ("Canceladas", str(safe_count(canceled_sales_df))),
])

left, right = st.columns([1.3, 1])

with left:
    series_df = (
        time_series(sales_df, "soldAtParsed", "value")
        if not sales_df.empty
        else pd.DataFrame()
    )
    render_line_chart(series_df, "period", "value", "Valor de vendas por período")

with right:
    render_bar_chart(status_breakdown(sales_df), "status", "count", "Estados das vendas")

st.subheader("Tabela de vendas")

table_columns = [
    c for c in [
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
    ] if c in sales_df.columns
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

row_style_jscode = JsCode(
    """
    function(params) {
        const status = (params.data.status || "").toUpperCase();

        if (status === "COMPLETED") {
            return { backgroundColor: "#d1e7dd" };
        }

        if (status === "SHIPPED") {
            return { backgroundColor: "#cfe2ff" };
        }

        if (status === "PRINT_DISPATCHED") {
            return { backgroundColor: "#fff3cd", color: "#856404" };
        }

        if (status === "WAITING_LABEL" || status === "READY_TO_PRINT") {
            return { backgroundColor: "#f8d7da" };
        }

        if (status === "CANCELED") {
            return { backgroundColor: "#e2e3e5", color: "#41464b" };
        }

        return {};
    }
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
        "Cores: vermelho = WAITING_LABEL / READY_TO_PRINT | amarelo = PRINT_DISPATCHED | azul = SHIPPED | verde = COMPLETED"
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
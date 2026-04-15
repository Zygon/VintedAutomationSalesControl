from __future__ import annotations

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from components.auth_guard import render_user_box, require_login
from components.charts import render_bar_chart, render_line_chart
from components.filters import get_active_filters, render_global_filters
from components.tables import render_kpis
from services.firestore_queries import (
    get_db,
    load_collection_df,
    load_label_by_id,
    load_sale_items_for_sale,
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
    "PRINT_ERROR",
]


def _update_sale_statuses(original_df: pd.DataFrame, edited_df: pd.DataFrame) -> int:
    if original_df.empty or edited_df.empty:
        return 0

    if "saleId" not in original_df.columns or "saleId" not in edited_df.columns:
        return 0

    original = original_df[["saleId", "status"]].copy()
    current = edited_df[["saleId", "status"]].copy()

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

    db = get_db()
    updated = 0

    for row in changed_rows:
        sale_id = row.get("saleId")
        new_status = row.get("status")

        if not sale_id or not new_status:
            continue

        db.collection("sales").document(str(sale_id)).set(
            {"status": str(new_status).strip()},
            merge=True,
        )
        updated += 1

    return updated


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

render_kpis([
    ("Total de vendas", str(safe_count(sales_df))),
    ("Faturação", format_currency(safe_sum(sales_df, "value"))),
    ("Ticket médio", format_currency(safe_avg(sales_df, "value"))),
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
        "accountId",
        "soldAt",
        "buyerUsername",
        "articleTitle",
        "sku",
        "value",
        "status",
        "orderId",
        "matchedLabelId",
    ] if c in sales_df.columns
]

table_df = sales_df[table_columns].copy()

gb = GridOptionsBuilder.from_dataframe(table_df)

gb.configure_default_column(
    resizable=True,
    sortable=True,
    filter=True,
)

gb.configure_column("saleId", hide=True)
gb.configure_column("matchedLabelId", hide=True)

if "status" in table_df.columns:
    gb.configure_column(
        "status",
        editable=True,
        cellEditor="agSelectCellEditor",
        cellEditorParams={"values": STATUS_OPTIONS},
    )

if "value" in table_df.columns:
    gb.configure_column("value", type=["numericColumn"])

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

        if (status === "WAITING_LABEL" || status === "READY_TO_PRINT") {
            return { backgroundColor: "#f8d7da" };
        }

        return {};
    }
    """
)

gb.configure_grid_options(getRowStyle=row_style_jscode)

grid_options = gb.build()

grid_response = AgGrid(
    table_df,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.VALUE_CHANGED,
    allow_unsafe_jscode=True,
    fit_columns_on_grid_load=False,
    use_container_width=True,
    height=500,
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
        "Cores por status: vermelho = WAITING_LABEL / READY_TO_PRINT | azul = SHIPPED | verde = COMPLETED"
    )

st.subheader("Detalhe da venda")
sale_options: list[str] = []
if not sales_df.empty and "saleId" in sales_df.columns:
    sale_options = sales_df["saleId"].dropna().astype(str).tolist()

selected_sale_id = st.selectbox("Seleciona uma saleId", [""] + sale_options)

if selected_sale_id:
    sale_row = sales_df[sales_df["saleId"].astype(str) == selected_sale_id].iloc[0].to_dict()
    st.json(sale_row)

    sale_items_df = load_sale_items_for_sale(
        selected_sale_id,
        account_id=account_filter,
        allowed_account_ids=allowed_account_ids,
    )
    st.markdown("#### saleItems")
    if sale_items_df.empty:
        st.info("Sem saleItems para esta venda.")
    else:
        st.dataframe(sale_items_df, use_container_width=True, hide_index=True)

    matched_label_id = sale_row.get("matchedLabelId")
    if matched_label_id:
        st.markdown("#### label")
        label = load_label_by_id(
            str(matched_label_id),
            account_id=account_filter,
            allowed_account_ids=allowed_account_ids,
        )
        if label:
            st.json(label)
        else:
            st.warning("Label associada não encontrada.")
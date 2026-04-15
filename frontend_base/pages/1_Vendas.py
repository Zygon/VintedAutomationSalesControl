from __future__ import annotations

import pandas as pd
import streamlit as st

from components.auth_guard import render_user_box, require_login
from components.charts import render_bar_chart, render_line_chart
from components.filters import get_active_filters, render_global_filters
from components.tables import render_dataframe, render_kpis
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


def _style_status_rows(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def row_style(row: pd.Series):
        status = str(row.get("status", "")).strip().upper()

        if status in {"WAITING_LABEL", "READY_TO_PRINT"}:
            return ["background-color: #f8d7da"] * len(row)  # vermelho claro
        if status == "COMPLETED":
            return ["background-color: #d1e7dd"] * len(row)  # verde claro
        if status == "SHIPPED":
            return ["background-color: #cfe2ff"] * len(row)  # azul claro

        return [""] * len(row)

    return df.style.apply(row_style, axis=1)


def _update_sale_statuses(changed_rows: list[dict]):
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

# Ordenação por soldAt desc
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

left, right = st.columns([1.2, 1])

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

# Mantemos saleId internamente para update e detalhe, mas não mostramos
table_df = sales_df.copy()

visible_cols = [
    c for c in [
        "accountId",
        "soldAt",
        "buyerUsername",
        "articleTitle",
        "sku",
        "value",
        "status",
        "orderId",
    ] if c in table_df.columns
]

editor_df = table_df.copy()

# Data editor para editar status
editable_cols = [
    c for c in ["saleId", *visible_cols] if c in editor_df.columns
]
editor_df = editor_df[editable_cols].copy()

edited_df = st.data_editor(
    editor_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "saleId": st.column_config.TextColumn("saleId", disabled=True, width="small"),
        "status": st.column_config.SelectboxColumn(
            "status",
            options=STATUS_OPTIONS,
            required=True,
        ),
    },
    disabled=[c for c in editor_df.columns if c not in {"status"}],
    key="sales_editor",
)

col_save, col_info = st.columns([1, 3])

with col_save:
    if st.button("Guardar alterações de status"):
        if "saleId" not in edited_df.columns or "saleId" not in editor_df.columns:
            st.error("Não foi possível identificar as vendas para atualização.")
        else:
            original = editor_df[["saleId", "status"]].copy().reset_index(drop=True)
            current = edited_df[["saleId", "status"]].copy().reset_index(drop=True)

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

            updated_count = _update_sale_statuses(changed_rows)
            if updated_count:
                st.success(f"{updated_count} venda(s) atualizada(s).")
                st.rerun()
            else:
                st.info("Não houve alterações de status para guardar.")

with col_info:
    st.caption(
        "Cores: vermelho = WAITING_LABEL / READY_TO_PRINT | azul = SHIPPED | verde = COMPLETED"
    )

# Tabela colorida só para leitura visual
st.markdown("#### Vista colorida")
if visible_cols:
    styled_df = table_df[visible_cols].copy()
    st.dataframe(_style_status_rows(styled_df), use_container_width=True, hide_index=True)
else:
    render_dataframe(table_df)

st.subheader("Detalhe da venda")
sale_options: list[str] = []
if not sales_df.empty and "saleId" in sales_df.columns:
    sale_options = sales_df["saleId"].dropna().astype(str).tolist()

selected_sale_id = st.selectbox("Seleciona uma saleId", [""] + sale_options)

if selected_sale_id:
    sale_row = sales_df[sales_df["saleId"].astype(str) == selected_sale_id].iloc[0].to_dict()

    # aqui podes mostrar tudo, incluindo IDs, porque é detalhe técnico
    st.json(sale_row)

    sale_items_df = load_sale_items_for_sale(
        selected_sale_id,
        account_id=account_filter,
        allowed_account_ids=allowed_account_ids,
    )
    st.markdown("#### saleItems")
    render_dataframe(sale_items_df)

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
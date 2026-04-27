from components.layout import apply_page_layout

apply_page_layout()

import importlib
import json
from datetime import date, datetime, time as dt_time, timezone
from typing import Any, Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from components.auth_guard import render_user_box, require_login
from components.filters import get_active_filters, render_global_filters
from components.tables import render_kpis
from services.firestore_queries import load_collection_df
from services.metrics import format_currency, safe_count, safe_sum

CATEGORY_COLORS = {
    "SHIPPING": "#cfe2ff",
    "PACKAGING": "#fff3cd",
    "SUPPLIES": "#fff3cd",
    "MATERIAL": "#fff3cd",
    "MARKETING": "#fce5cd",
    "ADS": "#fce5cd",
    "FEES": "#f8d7da",
    "PLATFORM_FEES": "#f8d7da",
    "PAYMENT_FEES": "#f8d7da",
    "REFUND": "#f8d7da",
    "SOFTWARE": "#d9ead3",
    "TOOLS": "#d9ead3",
    "SUBSCRIPTION": "#d9ead3",
    "TAX": "#ead1dc",
    "OTHER": "#e2e3e5",
}

CATEGORY_TEXT_COLORS = {
    "SHIPPING": "#0c5460",
    "PACKAGING": "#856404",
    "SUPPLIES": "#856404",
    "MATERIAL": "#856404",
    "MARKETING": "#8a5a00",
    "ADS": "#8a5a00",
    "FEES": "#721c24",
    "PLATFORM_FEES": "#721c24",
    "PAYMENT_FEES": "#721c24",
    "REFUND": "#721c24",
    "SOFTWARE": "#155724",
    "TOOLS": "#155724",
    "SUBSCRIPTION": "#155724",
    "TAX": "#6a1b4d",
    "OTHER": "#41464b",
}

CATEGORY_OPTIONS = [
    "SHIPPING",
    "PACKAGING",
    "SUPPLIES",
    "MATERIAL",
    "MARKETING",
    "ADS",
    "FEES",
    "PLATFORM_FEES",
    "PAYMENT_FEES",
    "REFUND",
    "SOFTWARE",
    "TOOLS",
    "SUBSCRIPTION",
    "TAX",
    "OTHER",
]

DATE_COLUMN_CANDIDATES = [
    "expenseDateParsed",
    "expenseDate",
    "incurredAtParsed",
    "incurredAt",
    "createdAtParsed",
    "createdAt",
    "updatedAtParsed",
    "updatedAt",
    "dateParsed",
    "date",
]

AMOUNT_COLUMN_CANDIDATES = [
    "amount",
    "value",
    "total",
    "cost",
    "price",
    "expenseValue",
    "expenseAmount",
    "allocatedValue",
    "netAmount",
    "grossAmount",
    "net",
    "gross",
]

CATEGORY_COLUMN_CANDIDATES = [
    "category",
    "expenseCategory",
    "type",
    "expenseType",
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


def _to_naive_timestamp(value: Any):
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts).tz_convert(None)


def _extract_current_range(date_range: dict | None):
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


def _previous_range_for_mode(current_start: pd.Timestamp, current_end: pd.Timestamp, period_mode: str):
    if period_mode == "Dia":
        return current_start - pd.Timedelta(days=1), current_end - pd.Timedelta(days=1)

    if period_mode == "Semana":
        return current_start - pd.Timedelta(days=7), current_end - pd.Timedelta(days=7)

    if period_mode == "Mês":
        prev_month_anchor = pd.Timestamp(current_start - pd.offsets.MonthBegin(1))
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


def _pick_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _get_datetime_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="datetime64[ns]")

    col = _pick_first_existing_column(df, DATE_COLUMN_CANDIDATES)
    if not col:
        return pd.Series(dtype="datetime64[ns]")

    series = pd.to_datetime(df[col], errors="coerce", utc=True)
    if series.notna().any():
        return series.dt.tz_convert(None)

    return pd.Series(dtype="datetime64[ns]")




def _coerce_numeric_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype="float64")

    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace(r"[\s\u00A0]", "", regex=True)
        .str.replace(r"[€$£]", "", regex=True)
        .str.replace(r"[^0-9,.-]", "", regex=True)
    )

    both_mask = cleaned.str.contains(",", regex=False) & cleaned.str.contains(".", regex=False)
    cleaned.loc[both_mask] = cleaned.loc[both_mask].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

    comma_only_mask = cleaned.str.contains(",", regex=False) & ~cleaned.str.contains(".", regex=False)
    cleaned.loc[comma_only_mask] = cleaned.loc[comma_only_mask].str.replace(",", ".", regex=False)

    cleaned = cleaned.replace({"": None, "-": None, ".": None, ",": None, "--": None})
    return pd.to_numeric(cleaned, errors="coerce")


def _pick_amount_column(df: pd.DataFrame) -> str | None:
    for col in AMOUNT_COLUMN_CANDIDATES:
        if col in df.columns:
            series = _coerce_numeric_series(df[col])
            if series.notna().any():
                return col

    for col in df.columns:
        series = _coerce_numeric_series(df[col])
        if series.notna().any() and series.abs().sum() > 0:
            return col

    return None


def _get_numeric_value_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")

    amount_col = _pick_amount_column(df)
    if amount_col:
        return _coerce_numeric_series(df[amount_col]).fillna(0)

    return pd.Series([0.0] * len(df), index=df.index, dtype="float64")


def _pick_category_column(df: pd.DataFrame) -> str | None:
    return _pick_first_existing_column(df, CATEGORY_COLUMN_CANDIDATES)


def _filter_df_by_date_range(df: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.DataFrame:
    if df.empty or start is None or end is None:
        return df.copy()

    series = _get_datetime_series(df)
    if series.empty:
        return df.copy()

    work = df.copy()
    work["__filter_dt__"] = series
    work = work[work["__filter_dt__"].notna()]
    work = work[(work["__filter_dt__"] >= start) & (work["__filter_dt__"] <= end)]
    return work.drop(columns=["__filter_dt__"], errors="ignore")


def _build_comparison_df_day(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)
    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["_amount"] = _get_numeric_value_series(current_work)
    previous_work["_amount"] = _get_numeric_value_series(previous_work)

    current_series = current_work.groupby(current_work["_dt"].dt.hour)["_amount"].sum()
    previous_series = previous_work.groupby(previous_work["_dt"].dt.hour)["_amount"].sum()

    hours = list(range(24))
    return pd.DataFrame(
        {
            "label": [f"{hour:02d}:00" for hour in hours],
            "current_value": [float(current_series.get(hour, 0)) for hour in hours],
            "previous_value": [float(previous_series.get(hour, 0)) for hour in hours],
        }
    )


def _build_comparison_df_week(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
) -> pd.DataFrame:
    weekday_labels = {0: "Seg", 1: "Ter", 2: "Qua", 3: "Qui", 4: "Sex", 5: "Sáb", 6: "Dom"}

    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)
    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["_amount"] = _get_numeric_value_series(current_work)
    previous_work["_amount"] = _get_numeric_value_series(previous_work)

    current_series = current_work.groupby(current_work["_dt"].dt.weekday)["_amount"].sum()
    previous_series = previous_work.groupby(previous_work["_dt"].dt.weekday)["_amount"].sum()

    weekdays = list(range(current_start.weekday(), current_end.weekday() + 1))
    return pd.DataFrame(
        {
            "label": [weekday_labels[d] for d in weekdays],
            "current_value": [float(current_series.get(day, 0)) for day in weekdays],
            "previous_value": [float(previous_series.get(day, 0)) for day in weekdays],
        }
    )


def _build_comparison_df_month(current_df: pd.DataFrame, previous_df: pd.DataFrame, current_end: pd.Timestamp) -> pd.DataFrame:
    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)
    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["_amount"] = _get_numeric_value_series(current_work)
    previous_work["_amount"] = _get_numeric_value_series(previous_work)

    current_series = current_work.groupby(current_work["_dt"].dt.day)["_amount"].sum()
    previous_series = previous_work.groupby(previous_work["_dt"].dt.day)["_amount"].sum()

    days = list(range(1, current_end.day + 1))
    return pd.DataFrame(
        {
            "label": [str(day) for day in days],
            "current_value": [float(current_series.get(day, 0)) for day in days],
            "previous_value": [float(previous_series.get(day, 0)) for day in days],
        }
    )


def _build_comparison_df(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    period_mode: str,
) -> pd.DataFrame:
    if period_mode == "Dia":
        return _build_comparison_df_day(current_df, previous_df)
    if period_mode == "Semana":
        return _build_comparison_df_week(current_df, previous_df, current_start, current_end)
    if period_mode == "Mês":
        return _build_comparison_df_month(current_df, previous_df, current_end)

    current_work = current_df.copy()
    previous_work = previous_df.copy()

    current_work["_dt"] = _get_datetime_series(current_work)
    previous_work["_dt"] = _get_datetime_series(previous_work)
    current_work = current_work.dropna(subset=["_dt"])
    previous_work = previous_work.dropna(subset=["_dt"])

    current_work["_amount"] = _get_numeric_value_series(current_work)
    previous_work["_amount"] = _get_numeric_value_series(previous_work)

    current_work["bucket"] = current_work["_dt"].dt.normalize()
    previous_work["bucket"] = previous_work["_dt"].dt.normalize()

    current_series = current_work.groupby("bucket")["_amount"].sum()
    previous_series = previous_work.groupby("bucket")["_amount"].sum()

    duration = current_end - current_start
    previous_start = (current_start - duration).normalize()
    previous_end = (current_start - pd.Timedelta(microseconds=1)).normalize()

    current_days = pd.date_range(current_start.normalize(), current_end.normalize(), freq="1D")
    previous_days = pd.date_range(previous_start, previous_end, freq="1D")
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

    return pd.DataFrame({"label": labels, "current_value": current_values, "previous_value": previous_values})


def _render_expenses_comparison_chart(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    current_start: pd.Timestamp,
    current_end: pd.Timestamp,
    period_mode: str,
    title: str,
) -> None:
    aligned_df = _build_comparison_df(current_df, previous_df, current_start, current_end, period_mode)
    if aligned_df.empty:
        st.info("Sem dados para apresentar.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=aligned_df["label"], y=aligned_df["current_value"], mode="lines+markers", name="Período atual")
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


def _build_category_chart_df(df: pd.DataFrame) -> pd.DataFrame:
    category_col = _pick_category_column(df)
    if df.empty or not category_col:
        return pd.DataFrame(columns=["category", "count"])

    work = df.copy()
    work[category_col] = work[category_col].fillna("OTHER").astype(str).str.upper().str.strip()
    return (
        work.groupby(category_col, dropna=False)
        .size()
        .reset_index(name="count")
        .rename(columns={category_col: "category"})
        .sort_values(["count", "category"], ascending=[False, True])
    )


def _render_category_bar_chart(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        st.info("Sem dados para apresentar.")
        return

    fig = px.bar(
        df,
        x="category",
        y="count",
        title=title,
        color="category",
        color_discrete_map=CATEGORY_COLORS,
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis_title=None,
        yaxis_title=None,
        height=350,
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


def _resolve_insert_expense_callable() -> Callable[..., Any] | None:
    candidate_modules = [
        "services.firestore_queries",
        "services.postgres_queries",
        "services.expense_service",
    ]
    candidate_names = [
        "create_expense",
        "insert_expense",
        "add_expense",
        "save_expense",
        "upsert_expense",
        "create_expense_record",
    ]

    for module_name in candidate_modules:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue

        for fn_name in candidate_names:
            fn = getattr(module, fn_name, None)
            if callable(fn):
                return fn

    return None


def _call_insert_expense(payload: dict) -> tuple[bool, str]:
    insert_fn = _resolve_insert_expense_callable()
    if insert_fn is None:
        return False, (
            "Não encontrei nenhuma função de insert para despesas no backend. "
            "Cria uma destas funções e esta página passa a gravar sem mexer mais aqui: "
            "create_expense, insert_expense, add_expense, save_expense, upsert_expense ou create_expense_record."
        )

    try:
        result = insert_fn(payload)
        if isinstance(result, dict) and result.get("id"):
            return True, str(result.get("id"))
        return True, "ok"
    except TypeError:
        try:
            result = insert_fn(**payload)
            if isinstance(result, dict) and result.get("id"):
                return True, str(result.get("id"))
            return True, "ok"
        except Exception as exc:
            return False, str(exc)
    except Exception as exc:
        return False, str(exc)


@st.dialog("Nova despesa", width="large")
def _render_create_expense_modal(selected_account_id: str, allowed_account_ids: list[str]) -> None:
    st.write("Inserção manual de despesa")

    use_manual_account = selected_account_id in (None, "__ALL__")
    account_id = selected_account_id

    if use_manual_account:
        account_id = st.selectbox(
            "Conta",
            options=allowed_account_ids,
            index=0 if allowed_account_ids else None,
            placeholder="Escolhe a conta",
        )
    else:
        st.text_input("Conta", value=str(selected_account_id), disabled=True)

    col1, col2 = st.columns(2)
    with col1:
        expense_date = st.date_input("Data da despesa", value=date.today())
    with col2:
        expense_time = st.time_input("Hora", value=dt_time(hour=12, minute=0))

    col3, col4 = st.columns([2, 1])
    with col3:
        description = st.text_input("Descrição")
    with col4:
        category = st.selectbox("Categoria", options=CATEGORY_OPTIONS, index=CATEGORY_OPTIONS.index("OTHER"))

    col5, col6, col7 = st.columns([1, 1, 1])
    with col5:
        amount = st.number_input("Valor", min_value=0.0, step=0.01, format="%.2f")
    with col6:
        currency = st.selectbox("Moeda", options=["EUR", "USD", "GBP"], index=0)
    with col7:
        supplier = st.text_input("Fornecedor")

    notes = st.text_area("Notas", height=120)

    if st.button("Guardar despesa", type="primary"):
        if not account_id:
            st.error("Falta selecionar a conta.")
            return
        if not description.strip():
            st.error("A descrição é obrigatória.")
            return
        if amount <= 0:
            st.error("O valor tem de ser maior que zero.")
            return

        expense_dt = datetime.combine(expense_date, expense_time)
        expense_dt_utc = expense_dt.replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)

        payload = {
            "accountId": str(account_id),
            "description": description.strip(),
            "category": str(category).strip().upper(),
            "amount": float(amount),
            "currency": str(currency).strip().upper(),
            "supplier": supplier.strip() or None,
            "notes": notes.strip() or None,
            "source": "MANUAL",
            "createdFrom": "BACKOFFICE",
            "expenseDate": expense_dt_utc,
            "expenseDateIso": expense_dt_utc.isoformat(),
            "createdAt": now_utc,
            "updatedAt": now_utc,
        }

        ok, result = _call_insert_expense(payload)
        if ok:
            st.success("Despesa inserida com sucesso.")
            st.rerun()
        else:
            st.error(f"Falha ao inserir despesa: {result}")


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

all_expenses_df = load_collection_df(
    "expenses",
    account_id=account_filter,
    allowed_account_ids=allowed_account_ids,
)

current_start, current_end = _extract_current_range(date_range)
expenses_df = _filter_df_by_date_range(all_expenses_df, current_start, current_end)

previous_expenses_df = pd.DataFrame()
previous_start, previous_end = None, None
if current_start is not None and current_end is not None:
    previous_start, previous_end = _previous_range_for_mode(current_start, current_end, period_mode)
    previous_expenses_df = _filter_df_by_date_range(all_expenses_df, previous_start, previous_end)

sort_col = _pick_first_existing_column(expenses_df, DATE_COLUMN_CANDIDATES)
if not expenses_df.empty and sort_col:
    expenses_df = expenses_df.sort_values(sort_col, ascending=False, na_position="last")

if not previous_expenses_df.empty:
    prev_sort_col = _pick_first_existing_column(previous_expenses_df, DATE_COLUMN_CANDIDATES)
    if prev_sort_col:
        previous_expenses_df = previous_expenses_df.sort_values(prev_sort_col, ascending=False, na_position="last")

amount_col = _pick_amount_column(expenses_df)
current_total = float(_coerce_numeric_series(expenses_df[amount_col]).fillna(0).sum()) if amount_col else 0.0
current_count = int(safe_count(expenses_df))
average_value = (current_total / current_count) if current_count > 0 else 0.0

render_kpis(
    [
        ("Registos", str(current_count)),
        ("Despesa total", format_currency(current_total)),
        ("Despesa média", format_currency(average_value)),
    ]
)

action_col, _ = st.columns([1, 5])
with action_col:
    if st.button("Inserir despesa", type="primary", use_container_width=True):
        _render_create_expense_modal(selected_account_id, allowed_account_ids)

left, right = st.columns([1.3, 1])

with left:
    if current_start is not None and current_end is not None and previous_start is not None and previous_end is not None:
        _render_expenses_comparison_chart(
            current_df=expenses_df,
            previous_df=previous_expenses_df,
            current_start=current_start,
            current_end=current_end,
            period_mode=period_mode,
            title="Valor de despesas por período",
        )
    else:
        series = _get_datetime_series(expenses_df)
        if expenses_df.empty or series.empty or amount_col is None:
            st.info("Sem dados para apresentar.")
        else:
            series_df = pd.DataFrame({"period": series, "value": _coerce_numeric_series(expenses_df[amount_col]).fillna(0)})
            series_df = series_df.dropna(subset=["period"]).groupby("period", as_index=False)["value"].sum()
            if series_df.empty:
                st.info("Sem dados para apresentar.")
            else:
                fig = px.line(series_df, x="period", y="value", title="Valor de despesas por período")
                fig.update_layout(
                    margin=dict(l=10, r=10, t=50, b=10),
                    xaxis_title=None,
                    yaxis_title=None,
                    height=350,
                )
                st.plotly_chart(fig, use_container_width=True)

with right:
    category_chart_df = _build_category_chart_df(expenses_df)
    _render_category_bar_chart(category_chart_df, "Distribuição por categoria")

st.subheader("Tabela de despesas")

if expenses_df.empty:
    st.info("Sem dados para apresentar.")
else:
    table_df = expenses_df.copy()
    if "expenseId" in table_df.columns:
        table_df = table_df.drop(columns=["expenseId"])

    grid_df = _sanitize_for_aggrid(table_df)

    gb = GridOptionsBuilder.from_dataframe(grid_df)
    gb.configure_default_column(resizable=True, sortable=True, filter=True)

    if amount_col and amount_col in grid_df.columns:
        gb.configure_column(amount_col, type=["numericColumn"])

    gb.configure_grid_options(
        rowHeight=40,
        animateRows=False,
        suppressRowClickSelection=True,
        suppressCellFocus=False,
        rowSelection="single",
    )

    category_colors_js = json.dumps(CATEGORY_COLORS)
    category_text_colors_js = json.dumps(CATEGORY_TEXT_COLORS)

    row_style_jscode = JsCode(
        f"""
        function(params) {{
            const data = params.data || {{}};
            const rawCategory = String(
                data.category || data.expenseCategory || data.type || data.expenseType || 'OTHER'
            ).toUpperCase().trim();
            const bgColors = {category_colors_js};
            const textColors = {category_text_colors_js};

            if (bgColors[rawCategory]) {{
                const style = {{ backgroundColor: bgColors[rawCategory] }};
                if (textColors[rawCategory]) {{
                    style.color = textColors[rawCategory];
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
        update_mode=GridUpdateMode.NO_UPDATE,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False,
        use_container_width=True,
        height=550,
        theme="streamlit",
        reload_data=False,
    )

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import streamlit as st


def _compute_date_range(period_mode: str, custom_start: date, custom_end: date):
    today = datetime.now(timezone.utc).date()

    if period_mode == "Dia":
        start_date = today
        end_date = today
    elif period_mode == "Semana":
        start_date = today - timedelta(days=today.weekday())
        end_date = today
    elif period_mode == "Mês":
        start_date = today.replace(day=1)
        end_date = today
    else:
        start_date = custom_start
        end_date = custom_end

    start_dt = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(end_date, time.max, tzinfo=timezone.utc)

    return {
        "start": start_dt,
        "end": end_dt,
    }


def render_global_filters(user: dict):
    st.sidebar.markdown("## Filtros globais")

    cached_allowed_user_id = st.session_state.get("_allowed_accounts_user_id")
    cached_allowed_accounts = st.session_state.get("allowed_accounts")

    if cached_allowed_user_id == user["userId"] and cached_allowed_accounts is not None:
        allowed_accounts = cached_allowed_accounts
    else:
        from services.access_service import get_allowed_accounts

        allowed_accounts = get_allowed_accounts(user["userId"])
        st.session_state["allowed_accounts"] = allowed_accounts
        st.session_state["_allowed_accounts_user_id"] = user["userId"]

    st.session_state["allowed_account_ids"] = [a["accountId"] for a in allowed_accounts]

    account_options = [{"accountId": "__ALL__", "emailAddress": "Todas", "role": None}] + allowed_accounts

    current_selected = st.session_state.get("selected_account_id", "__ALL__")
    valid_ids = {item["accountId"] for item in account_options}
    if current_selected not in valid_ids:
        current_selected = "__ALL__"

    selected_account = st.sidebar.selectbox(
        "Conta",
        options=account_options,
        index=next(i for i, item in enumerate(account_options) if item["accountId"] == current_selected),
        format_func=lambda x: x["emailAddress"],
    )

    st.session_state["selected_account_id"] = selected_account["accountId"]
    st.session_state["selected_account_role"] = selected_account.get("role")

    period_mode = st.sidebar.selectbox(
        "Período",
        ["Dia", "Semana", "Mês", "Intervalo manual"],
        index=["Dia", "Semana", "Mês", "Intervalo manual"].index(
            st.session_state.get("period_mode", "Mês")
        ),
    )

    st.session_state["period_mode"] = period_mode

    today = date.today()
    default_start = today - timedelta(days=30)

    if period_mode == "Intervalo manual":
        col1, col2 = st.sidebar.columns(2)

        start_date = col1.date_input(
            "Início",
            value=st.session_state.get("custom_start", default_start),
        )

        end_date = col2.date_input(
            "Fim",
            value=st.session_state.get("custom_end", today),
        )

        st.session_state["custom_start"] = start_date
        st.session_state["custom_end"] = end_date
    else:
        st.session_state["custom_start"] = default_start
        st.session_state["custom_end"] = today

    date_range = _compute_date_range(
        period_mode,
        st.session_state["custom_start"],
        st.session_state["custom_end"],
    )

    st.session_state["date_range"] = date_range


def get_active_filters():
    account_id = st.session_state.get("selected_account_id", "__ALL__")
    date_range = st.session_state.get("date_range")
    allowed_account_ids = st.session_state.get("allowed_account_ids", [])

    return {
        "accountId": account_id,
        "dateRange": date_range,
        "allowedAccountIds": allowed_account_ids,
    }
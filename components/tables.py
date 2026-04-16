from __future__ import annotations

import pandas as pd
import streamlit as st


def render_dataframe(df: pd.DataFrame, height: int = 420, hide_index: bool = True):
    if df.empty:
        st.info("Sem dados para mostrar.")
        return
    st.dataframe(df, use_container_width=True, height=height, hide_index=hide_index)


def render_kpis(kpis: list[tuple[str, str]]):
    if not kpis:
        return

    cols = st.columns(len(kpis))
    for col, (label, value) in zip(cols, kpis):
        col.metric(label, value)

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def render_line_chart(df: pd.DataFrame, x: str, y: str, title: str):
    if df.empty:
        st.info("Sem dados para o gráfico.")
        return
    fig = px.line(df, x=x, y=y, markers=True, title=title)
    st.plotly_chart(fig, use_container_width=True)


def render_bar_chart(df: pd.DataFrame, x: str, y: str, title: str):
    if df.empty:
        st.info("Sem dados para o gráfico.")
        return
    fig = px.bar(df, x=x, y=y, title=title)
    st.plotly_chart(fig, use_container_width=True)


def render_pie_chart(df: pd.DataFrame, names: str, values: str, title: str):
    if df.empty:
        st.info("Sem dados para o gráfico.")
        return
    fig = px.pie(df, names=names, values=values, title=title)
    st.plotly_chart(fig, use_container_width=True)

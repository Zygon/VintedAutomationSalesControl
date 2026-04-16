import streamlit as st

st.set_page_config(
    page_title="Vinted Ops Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container {
        max-width: 100% !important;
        padding-top: 1rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Vinted Ops Dashboard")
st.caption("Seleciona uma página no menu lateral.")
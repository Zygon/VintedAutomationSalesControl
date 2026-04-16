from __future__ import annotations

import streamlit as st


def apply_page_layout():


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
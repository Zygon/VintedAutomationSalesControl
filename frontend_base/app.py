from pathlib import Path

import streamlit as st

st.set_page_config(
    page_title="Vinted Ops Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

from dotenv import load_dotenv
from components.auth_guard import render_user_box, require_login
from components.filters import render_global_filters

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = PROJECT_ROOT / ".env"

if DOTENV_PATH.exists():
    load_dotenv(dotenv_path=DOTENV_PATH)

user = require_login()
render_user_box(user)
render_global_filters(user)

st.title("Vinted Ops Dashboard")
st.caption("Frontend local/online com login Google, controlo multi-account e operações.")
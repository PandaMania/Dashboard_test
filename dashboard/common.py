import os
import streamlit as st



def get_secret(key: str, default=None):
    v = os.getenv(key)
    if v:
        return v
    try:
        v2 = st.secrets.get(key, None)
        if v2:
            return str(v2)
    except Exception:
        pass
    return default

def require_login():
    expected = get_secret("APP_PASSWORD")
    if not expected:
        return
    if "authed" not in st.session_state:
        st.session_state.authed = False

    if not st.session_state.authed:
        with st.sidebar:
            st.subheader("Login")
            pw = st.text_input("Password", type="password")
            if st.button("Login"):
                st.session_state.authed = (pw == expected)
        if not st.session_state.authed:
            st.warning("Please log in.")
            st.stop()

def apply_dark_theme():
    DARK_BG = "#0b1020"
    GRID = "#1e2747"
    TEXT = "#e6ecff"
    st.markdown(
        f"""
        <style>
          .stApp {{
            background-color: {DARK_BG};
            color: {TEXT};
          }}
          section[data-testid="stSidebar"] > div {{
            background-color: {DARK_BG};
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )

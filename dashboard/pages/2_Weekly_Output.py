import os
from datetime import date
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px

CACHE_TTL_SECONDS = 6 * 60 * 60

# Optional: load .env locally
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------------------------
# Theme (reuse your constants)
# ---------------------------
DARK_BG = "#0b1020"
GRID = "#1e2747"
TEXT = "#e6ecff"
NEON_COLORS = ["#00F0FF", "#FF2CDF", "#7CFF00", "#FFD300", "#FF6B00", "#9D4EDD"]

st.title("📅 Weekly Output")

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

# ---------------------------
# Secrets / login (same pattern)
# ---------------------------
def _get_secret(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key)
    if v is not None and v != "":
        return v
    try:
        v2 = st.secrets.get(key, None)
        if v2 is not None and str(v2) != "":
            return str(v2)
    except Exception:
        pass
    return default

def require_login():
    expected = _get_secret("APP_PASSWORD")
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

require_login()

# ---------------------------
# DB helpers
# ---------------------------
@st.cache_resource
def get_engine():
    db_url = _get_secret("DB_URL")
    if db_url:
        return create_engine(db_url, pool_pre_ping=True)

    host = _get_secret("DB_HOST")
    name = _get_secret("DB_NAME")
    user = _get_secret("DB_USER")
    pwd  = _get_secret("DB_PASSWORD")

    if not all([host, name, user, pwd]):
        st.error("Missing DB config. Set DB_URL or DB_HOST/DB_NAME/DB_USER/DB_PASSWORD.")
        st.stop()

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:5432/{name}"
    return create_engine(url, pool_pre_ping=True)

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def qdf(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

# ---------------------------
# Sidebar controls
# ---------------------------
with st.sidebar:
    st.header("Weekly Volume")

    weeks_back = st.number_input("Lookback (weeks)", min_value=4, max_value=260, value=26, step=1)
    include_null_category = st.checkbox("Include NULL hc_category", value=False)

    st.divider()
    if st.button("Refresh (clear cache)"):
        st.cache_data.clear()
        st.rerun()

# ---------------------------
# Query
# ---------------------------
# Week ending Sunday (NY):
# - close_time_ny = close_time AT TIME ZONE 'America/New_York'
# - date_trunc('week', ...) uses Monday as start
# - so week_end_sun = start_of_week(Mon) + 6 days
sql = """
WITH base AS (
  SELECT
    (close_time AT TIME ZONE 'America/New_York') AS close_time_ny,
    hc_category,
    volume
  FROM summarised_tickers
  WHERE close_time IS NOT NULL
    AND volume IS NOT NULL
    AND (close_time AT TIME ZONE 'America/New_York')
          >= ( (NOW() AT TIME ZONE 'America/New_York') - (:weeks_back * INTERVAL '7 days') )
),
agg AS (
  SELECT
    ((date_trunc('week', close_time_ny)::date + INTERVAL '6 days')::date) AS week_end_sun_ny,
    hc_category,
    SUM(volume) AS total_handle
  FROM base
  GROUP BY 1, 2
)
SELECT *
FROM agg
ORDER BY week_end_sun_ny, hc_category
"""

df_week = qdf(sql, params={"weeks_back": int(weeks_back)})

if df_week.empty:
    st.info("No rows returned for that lookback.")
    st.stop()

# Optional: drop NULL hc_category if you want
if not include_null_category:
    df_week = df_week[df_week["hc_category"].notna()].copy()

df_week["week_end_sun_ny"] = pd.to_datetime(df_week["week_end_sun_ny"])

# ---------------------------
# Chart
# ---------------------------
st.subheader("Total Handle by Week (ending Sunday, NY)")

# Stacked area usually reads well for "total by category"
fig = px.area(
    df_week,
    x="week_end_sun_ny",
    y="total_handle",
    color="hc_category",
    title="Weekly Total Handle (NY week ending Sunday) by hc_category",
)

fig.update_layout(
    plot_bgcolor=DARK_BG,
    paper_bgcolor=DARK_BG,
    font=dict(color=TEXT, size=14),
    legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center"),
    xaxis=dict(title="Week ending (NY Sunday)", showgrid=True, gridcolor=GRID, zeroline=False),
    yaxis=dict(title="Total Handle", showgrid=True, gridcolor=GRID, zeroline=False),
    margin=dict(l=40, r=40, t=80, b=40),
)
st.plotly_chart(fig, use_container_width=True)

with st.expander("Show weekly table"):
    st.dataframe(df_week.sort_values(["week_end_sun_ny", "hc_category"]), use_container_width=True, height=520)

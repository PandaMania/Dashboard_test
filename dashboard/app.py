import os
from datetime import date
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px

CACHE_TTL_SECONDS = 6 * 60 * 60
# NEW: load .env locally (safe in prod: it just does nothing if no .env exists)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------------------------
# Page + style constants
# ---------------------------
st.set_page_config(page_title="Data Quality Dashboard", layout="wide")

NEON_COLORS = [
    "#00F0FF",  # cyan
    "#FF2CDF",  # magenta
    "#7CFF00",  # acid green
    "#FFD300",  # yellow
    "#FF6B00",  # orange
    "#9D4EDD",  # violet
]
DARK_BG = "#0b1020"
GRID = "#1e2747"
TEXT = "#e6ecff"

# A tiny bit of CSS for the full-page dark background
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
# NEW: simple login gate
# ---------------------------
def _get_secret(key: str, default: str | None = None) -> str | None:
    # 1) Env var is always safe
    v = os.getenv(key)
    if v is not None and v != "":
        return v

    # 2) Only then try Streamlit secrets, but never use `in` or iterate
    try:
        v2 = st.secrets.get(key, None)
        if v2 is not None and str(v2) != "":
            return str(v2)
    except Exception:
        # No secrets.toml present (or other secrets error) -> ignore
        pass

    return default


def require_login():
    expected = _get_secret("APP_PASSWORD")
    # If you don't set APP_PASSWORD, we don't block access (useful for local dev).
    # If you want to FORCE login always, remove this early return.
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
    # Option A: DB_URL (preferred)
    db_url = _get_secret("DB_URL")
    if db_url:
        return create_engine(db_url, pool_pre_ping=True)

    # Option B: components
    host = _get_secret("DB_HOST")
    name = _get_secret("DB_NAME")
    user = _get_secret("DB_USER")
    pwd  = _get_secret("DB_PASSWORD")

    if not all([host, name, user, pwd]):
        st.error(
            "Missing DB config. Set either DB_URL, or DB_HOST/DB_NAME/DB_USER/DB_PASSWORD "
            "(via environment variables or Streamlit secrets)."
        )
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
st.title("📊 Data Quality Dashboard")

with st.sidebar:
    st.header("Controls")

    default_prefixes = [
        "KXNBAGAME",
        "KXNFLGAME",
        "KXNCAAFGAME",
        "KXNHLGAME",
        "KXNCAAMBGAME",
    ]
    prefixes = st.multiselect("Ticker prefixes", default_prefixes, default=default_prefixes)

    st.divider()
    st.subheader("Trades volume ratio band")
    low = st.number_input("LOW", value=0.98, step=0.01, format="%.2f")
    high = st.number_input("HIGH", value=1.02, step=0.01, format="%.2f")

    st.divider()
    st.subheader("Trades filter")
    trades_after = st.date_input("processed_at >= ", value=date(2025, 12, 1))

    st.divider()
    if st.button("Refresh (clear cache)"):
        st.cache_data.clear()
        st.rerun()

# ---------------------------
# Load data
# ---------------------------
if not prefixes:
    st.warning("Select at least one prefix.")
    st.stop()

# NEW: parameterized LIKE ANY (ARRAY[:p1, :p2, ...])
like_params = {f"p{i}": f"{p}%" for i, p in enumerate(prefixes)}
like_array = ", ".join([f":p{i}" for i in range(len(prefixes))])

sql_markets = f"""
SELECT *
FROM markets2026
WHERE ticker LIKE ANY (ARRAY[{like_array}])
"""

sql_trades = """
SELECT *
FROM trade_backfill_processed
WHERE processed_at >= :after_ts
"""

with st.spinner("Loading markets2026…"):
    df = qdf(sql_markets, params=like_params)

with st.spinner("Loading trade_backfill_processed…"):
    df_trades = qdf(sql_trades, params={"after_ts": str(trades_after)})

# ---------------------------
# Markets calcs (weeks + completeness)
# ---------------------------
df = df.copy()

if "ticker" not in df.columns:
    st.error("markets2026 missing required column: ticker")
    st.stop()

df["close_time"] = pd.to_datetime(df.get("close_time"), utc=True, errors="coerce")
df = df.dropna(subset=["close_time"]).copy()

df["close_time_ny"] = df["close_time"].dt.tz_convert("America/New_York")
df["week_end_sun_ny"] = df["close_time_ny"].dt.to_period("W-SUN").dt.end_time.dt.date

now_ny = pd.Timestamp.now(tz="America/New_York")
current_week_end = now_ny.to_period("W-SUN").end_time.date()

df_historical = df[df["week_end_sun_ny"] <= current_week_end].copy()
df_historical["ticker_option"] = df_historical["ticker"].astype(str).str.extract(r"^(KX[A-Z]+GAME)")

if df_historical.empty:
    st.warning("No historical rows found for the selected prefixes.")
    st.stop()

total_rows = len(df_historical)
coverage = (
    df_historical.notna()
    .mean()
    .mul(100)
    .round(2)
    .rename("pct_not_null")
    .reset_index()
    .rename(columns={"index": "column"})
    .sort_values("pct_not_null")
)

coverage["null_pct"] = (100 - coverage["pct_not_null"]).round(2)
coverage["rows_not_null"] = (coverage["pct_not_null"] * total_rows / 100).round(0).astype(int)
coverage["rows_null"] = total_rows - coverage["rows_not_null"]
coverage["status"] = pd.cut(
    coverage["pct_not_null"],
    bins=[-1, 5, 50, 90, 100],
    labels=["🚨 Dead", "⚠️ Sparse", "🟡 Partial", "🟢 Good"],
)

broken_cols = coverage.loc[coverage["pct_not_null"] < 100, "column"].tolist()

# ---------------------------
# Trades calcs (volume ratio)
# ---------------------------
df_vol = df_trades.copy()
df_vol["processed_at"] = pd.to_datetime(df_vol.get("processed_at"), utc=True, errors="coerce")

# Guard against missing columns
needed = {"trades_sum_volume", "market_volume", "ticker", "processed_at"}
missing_needed = [c for c in needed if c not in df_vol.columns]
if missing_needed:
    st.error(f"Trades table missing required columns: {missing_needed}")
    st.stop()

df_vol["volume_ratio"] = df_vol["trades_sum_volume"] / df_vol["market_volume"]
df_vol = df_vol[
    (df_vol["market_volume"] > 0)
    & (df_vol["trades_sum_volume"] >= 0)
    & (df_vol["volume_ratio"].notna())
    & (df_vol["processed_at"].notna())
].copy()

df_vol["is_outside_band"] = ~df_vol["volume_ratio"].between(low, high)
df_outside = df_vol[df_vol["is_outside_band"]].copy()
df_outside["ticker_group"] = df_outside["ticker"].astype(str).str.split("-", n=1).str[0]
df_outside["processed_at_ny"] = df_outside["processed_at"].dt.tz_convert("America/New_York")

# ---------------------------
# Layout: top KPIs
# ---------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Markets rows", f"{len(df):,}")
c2.metric("Historical rows", f"{len(df_historical):,}")
c3.metric("Broken columns", f"{len(broken_cols):,}")
c4.metric("Outside-band trades", f"{len(df_outside):,}")

st.caption(f"Now (NY): {now_ny:%Y-%m-%d %H:%M} | Current week ends (NY Sunday): {current_week_end}")

# ---------------------------
# Section: Weekly games offered
# ---------------------------
st.subheader("Weekly Games Offered")

weekly = (
    df_historical.dropna(subset=["ticker_option", "week_end_sun_ny"])
    .groupby(["week_end_sun_ny", "ticker_option"], as_index=False)
    .agg(games=("ticker", "nunique"))
    .sort_values(["week_end_sun_ny", "ticker_option"])
)

fig_weekly = px.line(
    weekly,
    x="week_end_sun_ny",
    y="games",
    color="ticker_option",
    markers=True,
    color_discrete_sequence=NEON_COLORS,
    title=f"Weekly Games Offered (History → current week ending {current_week_end}, NY)",
)
fig_weekly.update_traces(line=dict(width=3), marker=dict(size=9))
fig_weekly.update_layout(
    plot_bgcolor=DARK_BG,
    paper_bgcolor=DARK_BG,
    font=dict(color=TEXT, size=14),
    title=dict(x=0.5, xanchor="center", font=dict(size=20)),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    xaxis=dict(title="Week ending (NY Sunday)", showgrid=True, gridcolor=GRID, zeroline=False),
    yaxis=dict(title="Games offered", showgrid=True, gridcolor=GRID, zeroline=False),
    margin=dict(l=40, r=40, t=80, b=40),
)
st.plotly_chart(fig_weekly, use_container_width=True)

# ---------------------------
# Section: Completeness
# ---------------------------
st.subheader("Column Completeness")

fig_cov = px.bar(
    coverage.sort_values("pct_not_null"),
    x="pct_not_null",
    y="column",
    orientation="h",
    title="Column completeness (% not null)",
    color="status",
    color_discrete_map={
        "🟢 Good": "#7CFF00",
        "🟡 Partial": "#FFD300",
        "⚠️ Sparse": "#FF6B00",
        "🚨 Dead": "#FF2CDF",
    },
)
fig_cov.update_layout(
    plot_bgcolor=DARK_BG,
    paper_bgcolor=DARK_BG,
    font=dict(color=TEXT),
    xaxis=dict(title="% of rows with data", showgrid=True, gridcolor=GRID, zeroline=False),
    yaxis=dict(title="", showgrid=False),
    legend=dict(orientation="h", y=1.05),
    margin=dict(l=140, r=40, t=80, b=40),
)
st.plotly_chart(fig_cov, use_container_width=True)

with st.expander("Show completeness table"):
    st.dataframe(coverage, use_container_width=True, height=420)

# ---------------------------
# Section: Missingness heatmap (select one column)
# ---------------------------
st.subheader("Missingness Heatmap")

if broken_cols:
    col = st.selectbox("Pick a column to inspect", broken_cols, index=0)

    heat = (
        df_historical.dropna(subset=["ticker_option", "week_end_sun_ny"])
        .groupby(["week_end_sun_ny", "ticker_option"], as_index=False)
        .agg(
            total_games=("ticker", "nunique"),
            missing_rows=(col, lambda s: s.isna().sum()),
        )
    )

    heat = heat[heat["total_games"] > 0].copy()
    heat["pct_missing"] = (heat["missing_rows"] / heat["total_games"] * 100).round(1)
    pivot = heat.pivot(index="ticker_option", columns="week_end_sun_ny", values="pct_missing")
    pivot = pivot.dropna(axis=1, how="all").sort_index()

    if pivot.empty:
        st.info("No heatmap data for this column and selection.")
    else:
        fig_hm = px.imshow(
            pivot,
            aspect="auto",
            zmin=0,
            zmax=100,
            color_continuous_scale=[
                (0.00, "#0b1020"),
                (0.20, "#1e2747"),
                (0.40, "#FFD300"),
                (0.70, "#FF6B00"),
                (1.00, "#FF2CDF"),
            ],
            title=f"% Missing: {col}",
        )
        fig_hm.update_traces(xgap=0, ygap=0)
        fig_hm.update_xaxes(showgrid=False, zeroline=False, showline=False)
        fig_hm.update_yaxes(showgrid=False, zeroline=False, showline=False)
        fig_hm.update_layout(
            plot_bgcolor=DARK_BG,
            paper_bgcolor=DARK_BG,
            font=dict(color=TEXT),
            coloraxis_colorbar=dict(title="% missing"),
            margin=dict(l=140, r=40, t=80, b=40),
        )
        st.plotly_chart(fig_hm, use_container_width=True)
else:
    st.success("No broken columns. Everything is 100% populated 🎉")

# ---------------------------
# Section: Trades volume ratio
# ---------------------------
st.subheader("Trades Volume Ratio Checks")

plot_df = df_vol.copy()
plot_df["volume_ratio_clip"] = plot_df["volume_ratio"].clip(0, 3)

fig_hist = px.histogram(
    plot_df,
    x="volume_ratio_clip",
    nbins=60,
    title="Trades volume ÷ Market volume (clipped to 0–3 for visibility)",
    labels={"volume_ratio_clip": "trades_sum_volume / market_volume"},
)
fig_hist.update_traces(marker_line_width=0)
fig_hist.update_layout(
    plot_bgcolor=DARK_BG,
    paper_bgcolor=DARK_BG,
    font=dict(color=TEXT),
    xaxis=dict(gridcolor=GRID, zeroline=False),
    yaxis=dict(gridcolor=GRID, zeroline=False),
)
st.plotly_chart(fig_hist, use_container_width=True)

if df_outside.empty:
    st.info(f"No markets outside band {low:.2f}–{high:.2f} for the selected date window.")
else:
    fig_scatter = px.scatter(
        df_outside.sort_values("processed_at_ny"),
        x="processed_at_ny",
        y="volume_ratio",
        color="ticker_group",
        color_discrete_sequence=NEON_COLORS,
        opacity=0.8,
        title=f"Outside-band markets (excluded band {low:.2f}–{high:.2f})",
        hover_data={
            "ticker": True,
            "ticker_group": True,
            "market_volume": True,
            "trades_sum_volume": True,
            "volume_ratio": ':.4f',
        },
    )
    fig_scatter.add_hline(y=low, opacity=0.25)
    fig_scatter.add_hline(y=high, opacity=0.25)
    fig_scatter.update_layout(
        plot_bgcolor=DARK_BG,
        paper_bgcolor=DARK_BG,
        font=dict(color=TEXT),
        xaxis=dict(title="processed_at (NY)", gridcolor=GRID, zeroline=False),
        yaxis=dict(title="trades_sum_volume / market_volume", gridcolor=GRID, zeroline=False),
        legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center"),
        margin=dict(l=60, r=40, t=90, b=60),
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    with st.expander("Show outside-band rows"):
        st.dataframe(
            df_outside.sort_values("processed_at_ny", ascending=False),
            use_container_width=True,
            height=420,
        )

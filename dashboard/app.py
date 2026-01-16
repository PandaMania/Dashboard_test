import streamlit as st

st.set_page_config(page_title="HoldCrunch Dashboards", layout="wide")

st.title("📌 HoldCrunch Dashboards")
st.write("Use the sidebar to pick a page.")
st.markdown(
    """
- **Data Quality**: column completeness, missingness heatmap, volume ratio checks
- **Weekly Output**: weekly rollups and exports
"""
)

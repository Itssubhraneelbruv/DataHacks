from __future__ import annotations

import streamlit as st

from utils import (
    METRIC_LABELS,
    choropleth_map,
    latest_snapshot,
    load_state_timeseries,
    metric_help,
    ranked_states,
    render_metric_cards,
)


st.set_page_config(page_title="Heat Trace", layout="wide")

st.title("Heat Trace")
st.caption(
    "A hackathon-ready data app for exploring how U.S. energy activity, solar adoption, "
    "and environmental indicators move together across states."
)

df = load_state_timeseries()
latest = latest_snapshot(df)
latest_year = int(latest["year"].max())

st.subheader(f"National overview, {latest_year}")
render_metric_cards(latest)

metric = st.selectbox(
    "Map metric",
    options=list(METRIC_LABELS.keys()),
    format_func=lambda key: METRIC_LABELS[key],
    help="Choose the state-level metric to compare on the map and ranking table.",
)
st.caption(metric_help(metric))

st.plotly_chart(choropleth_map(latest, metric), use_container_width=True)

st.subheader(f"Top and bottom states by {METRIC_LABELS[metric].lower()}")
ranked = ranked_states(latest, metric, n=5)
display_col = METRIC_LABELS[metric]
formatter = {display_col: "{:.1%}"} if metric == "solar_growth_rate" else {display_col: "{:,.3f}"}
if metric not in {"clean_ratio", "emissions_intensity", "solar_growth_rate"}:
    formatter = {display_col: "{:,.0f}"}

st.dataframe(
    ranked.style.format(formatter),
    use_container_width=True,
    hide_index=True,
)

with st.expander("About this prototype"):
    st.write(
        "Heat Trace currently uses generated state-level mock data stored as parquet. "
        "The app is organized so future EIA, ZenPower, and EPA ingestion can write the same "
        "processed schema into `data/processed/` without changing the Streamlit pages."
    )

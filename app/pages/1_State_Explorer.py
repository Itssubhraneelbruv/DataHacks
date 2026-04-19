from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.append(str(APP_DIR))

from utils import CHART_METRICS, METRIC_LABELS, load_state_timeseries, state_summary, time_series_chart


st.set_page_config(page_title="Heat Trace | State Explorer", layout="wide")

st.title("State Explorer")
st.caption("Explore annual state energy consumption and year-over-year change from the clean dataset.")

df = load_state_timeseries()
states = df["state"].drop_duplicates().sort_values().tolist()

selector_col, _ = st.columns([1, 2])
with selector_col:
    selected_state = st.selectbox(
        "State",
        states,
        index=states.index("California") if "California" in states else 0,
    )
state_df = df[df["state"] == selected_state].sort_values("date")

latest = state_df.iloc[-1]
first = state_df.iloc[0]
peak = state_df.loc[state_df["energy_btu"].idxmax()]
cols = st.columns(4)
cols[0].metric("Latest energy", f"{latest['energy_btu']:,.0f}")
cols[1].metric("Latest energy (kWh)", f"{latest['energy_kwh']:,.0f}")
cols[2].metric("Peak year", f"{int(peak['year'])}")
cols[3].metric("Latest YoY change", f"{latest['year_over_year_change']:.1%}")

st.subheader(selected_state)
st.write(state_summary(state_df))

tab_labels = [METRIC_LABELS[metric] for metric in CHART_METRICS]
tabs = st.tabs(tab_labels)
for tab, metric in zip(tabs, CHART_METRICS):
    with tab:
        st.plotly_chart(time_series_chart(state_df, metric), use_container_width=True)

with st.expander("State data"):
    st.dataframe(
        state_df[["year", "energy_btu", "energy_kwh", "year_over_year_change"]].style.format(
            {
                "energy_btu": "{:,.0f}",
                "energy_kwh": "{:,.0f}",
                "year_over_year_change": "{:.1%}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

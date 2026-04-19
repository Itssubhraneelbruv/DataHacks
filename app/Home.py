from __future__ import annotations

import time

import streamlit as st

from utils import (
    METRIC_LABELS,
    choropleth_map,
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
min_year = int(df["year"].min())
max_year = int(df["year"].max())
default_year = min(2023, max_year)


def sync_year_from_slider() -> None:
    st.session_state["selected_year"] = st.session_state["year_slider"]


def sync_year_from_input() -> None:
    st.session_state["selected_year"] = st.session_state["year_input"]


def advance_year(min_year: int, max_year: int) -> None:
    current_year = int(st.session_state["selected_year"])
    next_year = current_year + 1 if current_year < max_year else min_year
    st.session_state["selected_year"] = next_year
    st.session_state["year_slider"] = next_year
    st.session_state["year_input"] = next_year


if "selected_year" not in st.session_state:
    st.session_state["selected_year"] = default_year
if "is_playing" not in st.session_state:
    st.session_state["is_playing"] = False
if "playback_speed" not in st.session_state:
    st.session_state["playback_speed"] = 0.5
if "last_advance_at" not in st.session_state:
    st.session_state["last_advance_at"] = time.monotonic()
if "selected_metric" not in st.session_state:
    st.session_state["selected_metric"] = list(METRIC_LABELS.keys())[0]

st.session_state["year_slider"] = st.session_state["selected_year"]
st.session_state["year_input"] = st.session_state["selected_year"]

refresh_interval = float(st.session_state["playback_speed"]) if st.session_state["is_playing"] else None


@st.fragment(run_every=refresh_interval)
def render_playback_dashboard() -> None:
    if st.session_state["is_playing"]:
        now = time.monotonic()
        interval = float(st.session_state["playback_speed"])
        if now - float(st.session_state["last_advance_at"]) >= interval:
            advance_year(min_year, max_year)
            st.session_state["last_advance_at"] = now

    controls_col, map_col = st.columns([1, 4], gap="large")

    with controls_col:
        st.subheader("Map controls")
        play_col, pause_col = st.columns(2)
        with play_col:
            if st.button("Play", use_container_width=True):
                st.session_state["is_playing"] = True
                st.session_state["last_advance_at"] = time.monotonic()
                st.rerun()
        with pause_col:
            if st.button("Pause", use_container_width=True):
                st.session_state["is_playing"] = False
                st.rerun()

        st.select_slider(
            "Playback speed (seconds)",
            options=[0.25, 0.50, 0.75, 1.00],
            key="playback_speed",
            format_func=lambda value: f"{value:.2f}",
            help="Choose how long the app waits before advancing to the next year.",
        )
        st.slider(
            "Year",
            min_value=min_year,
            max_value=max_year,
            key="year_slider",
            on_change=sync_year_from_slider,
        )
        st.number_input(
            "Type a year",
            min_value=min_year,
            max_value=max_year,
            key="year_input",
            step=1,
            on_change=sync_year_from_input,
        )

        metric = st.selectbox(
            "Map metric",
            options=list(METRIC_LABELS.keys()),
            key="selected_metric",
            format_func=lambda key: METRIC_LABELS[key],
            help="Choose the energy metric to compare on the map and ranking table.",
        )
        st.caption(metric_help(metric))

    selected_year = int(st.session_state["selected_year"])
    selected_snapshot = df[df["year"] == selected_year].copy()

    st.subheader(f"National overview, {selected_year}")
    render_metric_cards(selected_snapshot)

    with map_col:
        st.plotly_chart(
            choropleth_map(selected_snapshot, metric),
            use_container_width=True,
            config={"displayModeBar": False, "scrollZoom": False, "doubleClick": False},
            key=f"map-{metric}",
        )

    st.subheader(f"Top and bottom states by {METRIC_LABELS[metric].lower()}")
    ranked = ranked_states(selected_snapshot, metric, n=5)
    display_col = METRIC_LABELS[metric]
    formatter = {display_col: "{:.1%}"} if metric == "year_over_year_change" else {display_col: "{:,.0f}"}

    st.dataframe(
        ranked.style.format(formatter),
        use_container_width=True,
        hide_index=True,
    )


render_playback_dashboard()

with st.expander("About this prototype"):
    st.write(
        "Heat Trace now uses a cleaned state-year energy dataset stored in "
        "`data/processed/clean_energy_data.csv`. The app maps annual BTU consumption "
        "by state, derives a matching kWh view, and computes a year-over-year change metric from that source."
    )

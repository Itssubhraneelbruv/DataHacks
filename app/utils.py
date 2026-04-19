from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "processed" / "clean_energy_data.csv"
BTU_PER_KWH = 3412.142

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


METRIC_LABELS = {
    "energy_btu": "Energy consumption (BTU)",
    "energy_kwh": "Energy consumption (kWh)",
    "year_over_year_change": "Year-over-year change",
}

CHART_METRICS = ["energy_btu", "energy_kwh", "year_over_year_change"]

STATE_NAME_BY_ABBR = {
    "AK": "Alaska",
    "AL": "Alabama",
    "AR": "Arkansas",
    "AZ": "Arizona",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NV": "Nevada",
    "NY": "New York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "US": "United States",
    "UT": "Utah",
    "VA": "Virginia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "West Virginia",
    "WY": "Wyoming",
}


def ensure_energy_data() -> Path:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Expected energy dataset at {DATA_PATH}")
    return DATA_PATH


def load_state_timeseries() -> pd.DataFrame:
    ensure_energy_data()
    df = pd.read_csv(DATA_PATH)
    df = df.rename(columns={"state": "state_abbr"})
    df = df[df["state_abbr"] != "US"].copy()
    df["state"] = df["state_abbr"].map(STATE_NAME_BY_ABBR).fillna(df["state_abbr"])
    df["date"] = pd.to_datetime(df["year"].astype(int).astype(str) + "-01-01")
    df["energy_btu"] = pd.to_numeric(df["energy_btu"], errors="coerce")
    df = df.dropna(subset=["energy_btu", "year"]).copy()
    df["date"] = pd.to_datetime(df["date"])
    return compute_metrics(df)


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values(["state", "date"])
    df["energy_kwh"] = df["energy_btu"] / BTU_PER_KWH
    df["year_over_year_change"] = df.groupby("state")["energy_btu"].pct_change().fillna(0)
    return df


def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    latest_year = int(df["year"].max())
    return df[df["year"] == latest_year].copy()


def format_metric(value: float, metric: str) -> str:
    if metric == "year_over_year_change":
        return f"{value:.1%}"
    return f"{value:,.0f}"


def metric_help(metric: str) -> str:
    descriptions = {
        "energy_btu": "Annual state energy consumption from the clean dataset, measured in BTUs.",
        "energy_kwh": "Annual state energy consumption converted from BTUs into kilowatt-hours.",
        "year_over_year_change": "Percent change in annual state energy consumption versus the prior year.",
    }
    return descriptions.get(metric, "")


def render_metric_cards(latest: pd.DataFrame) -> None:
    import streamlit as st

    total_energy = latest["energy_btu"].sum()
    avg_energy = latest["energy_btu"].mean()
    top_state = latest.nlargest(1, "energy_btu").iloc[0]
    median_yoy = latest["year_over_year_change"].median()

    cols = st.columns(4)
    cols[0].metric("Total energy", f"{total_energy:,.0f}")
    cols[1].metric("Average by state", f"{avg_energy:,.0f}")
    cols[2].metric("Highest state", top_state["state"])
    cols[3].metric("Median YoY change", f"{median_yoy:.1%}")


def choropleth_map(latest: pd.DataFrame, metric: str) -> go.Figure:
    color_scale = "RdYlGn" if metric == "year_over_year_change" else "Viridis"
    fig = px.choropleth(
        latest,
        locations="state_abbr",
        locationmode="USA-states",
        scope="usa",
        color=metric,
        hover_name="state",
        hover_data={
            "state_abbr": False,
            metric: ":.1%" if metric == "year_over_year_change" else ":,.0f",
        },
        color_continuous_scale=color_scale,
        labels={metric: METRIC_LABELS.get(metric, metric)},
    )
    fig.update_traces(marker_line_color="rgba(255, 255, 255, 0.8)", marker_line_width=0.8)
    fig.update_layout(
        dragmode=False,
        margin=dict(l=0, r=0, t=10, b=0),
        height=500,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#31333f"),
        coloraxis_colorbar=dict(
            title=dict(text=METRIC_LABELS.get(metric, metric), font=dict(color="#ffffff")),
            tickfont=dict(color="#ffffff"),
        ),
    )
    fig.update_geos(
        bgcolor="rgba(0,0,0,0)",
        showland=False,
        showocean=False,
        showlakes=False,
        showcoastlines=False,
        showcountries=False,
        showsubunits=False,
        showframe=False,
    )
    return fig


def ranked_states(latest: pd.DataFrame, metric: str, n: int = 10) -> pd.DataFrame:
    top = latest.nlargest(n, metric).assign(rank_group="Top")
    bottom = latest.nsmallest(n, metric).assign(rank_group="Bottom")
    ranked = pd.concat([top, bottom], ignore_index=True)
    return ranked[["rank_group", "state", "state_abbr", metric]].rename(
        columns={
            "rank_group": "Group",
            "state": "State",
            "state_abbr": "Abbr",
            metric: METRIC_LABELS.get(metric, metric),
        }
    )


def time_series_chart(state_df: pd.DataFrame, metric: str) -> go.Figure:
    fig = px.line(
        state_df,
        x="date",
        y=metric,
        markers=True,
        labels={"date": "Year", metric: METRIC_LABELS.get(metric, metric)},
        title=METRIC_LABELS.get(metric, metric),
    )
    fig.update_traces(line=dict(width=3))
    fig.update_layout(height=310, margin=dict(l=10, r=10, t=45, b=10), hovermode="x unified")
    return fig


def state_summary(state_df: pd.DataFrame) -> str:
    state_df = state_df.sort_values("year")
    state = state_df["state"].iloc[0]
    first = state_df.iloc[0]
    latest = state_df.iloc[-1]
    energy_change = pct_change(first["energy_btu"], latest["energy_btu"])
    avg_growth = state_df["year_over_year_change"].iloc[1:].mean() if len(state_df) > 1 else 0.0
    peak_year = int(state_df.loc[state_df["energy_btu"].idxmax(), "year"])
    latest_rank = int(
        state_df[state_df["year"] == latest["year"]]["energy_btu"].rank(method="dense", ascending=False).iloc[0]
    )

    return (
        f"{state} used {energy_change:.0%} more energy in {int(latest['year'])} than in {int(first['year'])}. "
        f"Its average year-over-year change across the series is {avg_growth:.1%}, and its peak energy use "
        f"occurred in {peak_year} at about {state_df['energy_kwh'].max():,.0f} kWh."
    )


def pct_change(old: float, new: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / old

def build_insights(df: pd.DataFrame) -> pd.DataFrame:
    latest = latest_snapshot(df)
    national_median = latest["energy_btu"].median()
    records = []

    for state, state_df in df.groupby("state", sort=True):
        state_df = state_df.sort_values("year")
        latest_row = state_df.iloc[-1]
        recent_window = state_df.tail(min(5, len(state_df)))
        energy_change = pct_change(recent_window.iloc[0]["energy_btu"], recent_window.iloc[-1]["energy_btu"])

        if energy_change > 0.15:
            records.append(
                {
                    "State": state,
                    "Abbr": latest_row["state_abbr"],
                    "Insight": "Fast recent energy growth",
                    "Energy change, recent": energy_change,
                    "Latest energy": latest_row["energy_btu"],
                }
            )
        if latest_row["energy_btu"] > national_median * 2:
            records.append(
                {
                    "State": state,
                    "Abbr": latest_row["state_abbr"],
                    "Insight": "Very high current energy use",
                    "Energy change, recent": energy_change,
                    "Latest energy": latest_row["energy_btu"],
                }
            )

    return pd.DataFrame(records)


def insight_count_chart(insights: pd.DataFrame) -> go.Figure:
    if insights.empty:
        fig = go.Figure()
        fig.update_layout(title="No insight flags found", height=320)
        return fig

    counts = insights["Insight"].value_counts().reset_index()
    counts.columns = ["Insight", "States"]
    fig = px.bar(counts, x="States", y="Insight", orientation="h", text="States", color="Insight")
    fig.update_layout(height=330, margin=dict(l=10, r=10, t=20, b=10), showlegend=False)
    fig.update_yaxes(categoryorder="total ascending")
    return fig


def dataframe_with_formats(df: pd.DataFrame, percent_columns: Iterable[str] = ()) -> object:
    styled = df.style
    for col in percent_columns:
        if col in df.columns:
            styled = styled.format({col: "{:.1%}"})
    numeric_formats = {
        "Latest energy": "{:,.0f}",
    }
    existing = {key: value for key, value in numeric_formats.items() if key in df.columns}
    if existing:
        styled = styled.format(existing)
    return styled

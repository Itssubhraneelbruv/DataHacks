from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "processed" / "heat_trace_state_timeseries.parquet"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


METRIC_LABELS = {
    "energy_consumption": "Energy consumption",
    "energy_production": "Energy production",
    "solar_capacity_added": "Solar capacity added",
    "co2_emissions": "CO2 emissions",
    "air_quality_index": "Air quality index",
    "clean_ratio": "Clean ratio",
    "emissions_intensity": "Emissions intensity",
    "solar_growth_rate": "Solar growth rate",
}

CHART_METRICS = [
    "energy_consumption",
    "energy_production",
    "solar_capacity_added",
    "co2_emissions",
    "air_quality_index",
]


def ensure_mock_data() -> Path:
    """Create the processed parquet file if the repo was freshly cloned."""
    if DATA_PATH.exists():
        return DATA_PATH

    from scripts.build_mock_data import save_mock_dataset

    return save_mock_dataset(DATA_PATH)


def load_state_timeseries() -> pd.DataFrame:
    ensure_mock_data()
    df = pd.read_parquet(DATA_PATH)
    df["date"] = pd.to_datetime(df["date"])
    return compute_metrics(df)


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute derived metrics so real ingestion can feed the same app contract."""
    df = df.copy().sort_values(["state", "date"])

    df["clean_ratio"] = safe_divide(df["solar_capacity_added"], df["energy_production"])
    df["emissions_intensity"] = safe_divide(df["co2_emissions"], df["energy_consumption"])
    df["solar_growth_rate"] = (
        df.groupby("state")["solar_capacity_added"].pct_change().replace([np.inf, -np.inf], np.nan).fillna(0)
    )
    co2_change = df.groupby("state")["co2_emissions"].pct_change().replace([np.inf, -np.inf], np.nan).fillna(0)
    df["impact_gap_flag"] = (df["solar_growth_rate"] > 0.05) & (co2_change >= -0.005)

    return df


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    result = numerator / denominator.replace(0, np.nan)
    return result.replace([np.inf, -np.inf], np.nan).fillna(0)


def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    latest_year = int(df["year"].max())
    return df[df["year"] == latest_year].copy()


def format_metric(value: float, metric: str) -> str:
    if metric in {"clean_ratio", "emissions_intensity"}:
        return f"{value:.3f}"
    if metric == "solar_growth_rate":
        return f"{value:.1%}"
    if metric == "air_quality_index":
        return f"{value:.0f}"
    return f"{value:,.0f}"


def metric_help(metric: str) -> str:
    descriptions = {
        "energy_consumption": "Mock annual state energy demand index.",
        "energy_production": "Mock annual state energy supply index.",
        "solar_capacity_added": "Mock annual solar capacity additions.",
        "co2_emissions": "Mock annual CO2 emissions index.",
        "air_quality_index": "Mock annual average AQI; lower is better.",
        "clean_ratio": "Solar capacity added divided by energy production.",
        "emissions_intensity": "CO2 emissions divided by energy consumption.",
        "solar_growth_rate": "Year-over-year change in solar capacity added.",
    }
    return descriptions.get(metric, "")


def render_metric_cards(latest: pd.DataFrame) -> None:
    import streamlit as st

    total_consumption = latest["energy_consumption"].sum()
    total_production = latest["energy_production"].sum()
    total_solar = latest["solar_capacity_added"].sum()
    avg_aqi = latest["air_quality_index"].mean()

    cols = st.columns(4)
    cols[0].metric("Energy consumed", f"{total_consumption:,.0f}")
    cols[1].metric("Energy produced", f"{total_production:,.0f}")
    cols[2].metric("Solar added", f"{total_solar:,.0f}")
    cols[3].metric("Avg AQI", f"{avg_aqi:.0f}")


def choropleth_map(latest: pd.DataFrame, metric: str) -> go.Figure:
    color_scale = "RdYlGn_r" if metric in {"co2_emissions", "air_quality_index", "emissions_intensity"} else "Viridis"
    map_background = "#30352f"
    fig = px.choropleth(
        latest,
        locations="state_abbr",
        locationmode="USA-states",
        scope="usa",
        color=metric,
        hover_name="state",
        hover_data={
            "state_abbr": False,
            metric: ":,.3f" if metric in {"clean_ratio", "emissions_intensity", "solar_growth_rate"} else ":,.0f",
        },
        color_continuous_scale=color_scale,
        labels={metric: METRIC_LABELS.get(metric, metric)},
    )
    fig.update_layout(
        dragmode=False,
        margin=dict(l=0, r=0, t=10, b=0),
        height=500,
        paper_bgcolor=map_background,
        plot_bgcolor=map_background,
        font=dict(color="#f7f3e8"),
        coloraxis_colorbar=dict(
            title=dict(text=METRIC_LABELS.get(metric, metric), font=dict(color="#f7f3e8")),
            tickfont=dict(color="#f7f3e8"),
        ),
    )
    fig.update_geos(
        bgcolor=map_background,
        showland=True,
        landcolor="#ded2bd",
        showocean=True,
        oceancolor="#454b3e",
        showlakes=True,
        lakecolor="#58604f",
        showcoastlines=True,
        coastlinecolor="rgba(247, 243, 232, 0.45)",
        subunitcolor="rgba(247, 243, 232, 0.55)",
        countrycolor="rgba(247, 243, 232, 0.45)",
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

    solar_change = pct_change(first["solar_capacity_added"], latest["solar_capacity_added"])
    emissions_change = pct_change(first["co2_emissions"], latest["co2_emissions"])
    aqi_change = latest["air_quality_index"] - first["air_quality_index"]
    clean_ratio = latest["clean_ratio"]

    emissions_phrase = "fell" if emissions_change < -0.02 else "rose" if emissions_change > 0.02 else "stayed roughly flat"
    aqi_phrase = "improved" if aqi_change < -2 else "worsened" if aqi_change > 2 else "held steady"

    flag_sentence = (
        "The state is currently flagged for an impact gap because solar is growing while emissions are not clearly improving."
        if bool(latest["impact_gap_flag"])
        else "The latest year does not show an impact-gap flag."
    )

    return (
        f"{state} added {solar_change:.0%} more solar capacity than in {int(first['year'])}. "
        f"Over the same period, CO2 emissions {emissions_phrase} by {abs(emissions_change):.0%}, "
        f"and air quality {aqi_phrase}. The latest clean ratio is {clean_ratio:.3f}. {flag_sentence}"
    )


def pct_change(old: float, new: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / old


def build_insights(df: pd.DataFrame) -> pd.DataFrame:
    latest = latest_snapshot(df)
    records = []
    clean_high = latest["clean_ratio"].quantile(0.75)
    clean_low = latest["clean_ratio"].quantile(0.25)
    pollution_high = latest["air_quality_index"].quantile(0.75)

    for state, state_df in df.groupby("state", sort=True):
        state_df = state_df.sort_values("year")
        latest_row = state_df.iloc[-1]
        window = state_df.tail(5)
        solar_change = pct_change(window.iloc[0]["solar_capacity_added"], window.iloc[-1]["solar_capacity_added"])
        emissions_change = pct_change(window.iloc[0]["co2_emissions"], window.iloc[-1]["co2_emissions"])
        aqi_change = window.iloc[-1]["air_quality_index"] - window.iloc[0]["air_quality_index"]

        state_flags = []
        if solar_change > 0.25 and emissions_change >= -0.02:
            state_flags.append("Solar growth increased but emissions did not decrease")
        if latest_row["air_quality_index"] >= pollution_high and latest_row["clean_ratio"] <= clean_low:
            state_flags.append("High pollution and low solar adoption")
        if latest_row["clean_ratio"] >= clean_high and emissions_change < -0.04 and aqi_change < -3:
            state_flags.append("Strong solar adoption and improving environmental indicators")
        if bool(latest_row["impact_gap_flag"]) and "Solar growth increased but emissions did not decrease" not in state_flags:
            state_flags.append("Latest-year impact gap")

        for flag in state_flags:
            records.append(
                {
                    "State": state,
                    "Abbr": latest_row["state_abbr"],
                    "Insight": flag,
                    "Solar growth, 5yr": solar_change,
                    "CO2 change, 5yr": emissions_change,
                    "AQI change, 5yr": aqi_change,
                    "Clean ratio": latest_row["clean_ratio"],
                    "Latest AQI": latest_row["air_quality_index"],
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
        "AQI change, 5yr": "{:+.1f}",
        "Clean ratio": "{:.3f}",
        "Latest AQI": "{:.0f}",
    }
    existing = {key: value for key, value in numeric_formats.items() if key in df.columns}
    if existing:
        styled = styled.format(existing)
    return styled

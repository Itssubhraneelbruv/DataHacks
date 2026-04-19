from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.append(str(APP_DIR))


st.set_page_config(page_title="Heat Trace | Insights", layout="wide")

st.title("Insights")
st.caption("Solar permit patterns reveal two very different adoption stories.")

permit_count_chart_path = APP_DIR / "assets" / "top_cities_by_permit_count.png"
avg_system_chart_path = APP_DIR / "assets" / "top_cities_by_avg_system_size_per_permit.png"
adoption_time_chart_path = APP_DIR / "assets" / "solar_permit_adoption_over_time.png"

st.subheader("Where individual solar buying is happening at scale")
st.image(permit_count_chart_path, use_container_width=True)
st.write(
    "Permit volume shows where lots of individual buyers are entering the solar market. "
    "Oakland leads by a wide margin with 3,455 permits, while Hanford, Indio, and San Diego "
    "also show strong adoption. The interesting part is that permit count is not just a "
    "sun-exposure story: places with very different climates and city sizes can still show "
    "high solar activity when household demand, incentives, costs, and local permitting line up."
)

st.subheader("Where system size tells a different story")
st.image(avg_system_chart_path, use_container_width=True)
st.write(
    "A typical home solar system is around 8-12 kW of capacity, but every city on this chart averages "
    "300-741 kW per permit. That means these are not normal homeowner installs. They are more "
    "likely farms, ranches, commercial properties, or small utility-scale projects, which means "
    "this slice of the records data is capturing a completely different buyer profile than the "
    "one we assumed at first."
)
st.write(
    "Point Reyes is the clearest example. It is a rural coastal community in Marin County with "
    "farms and nature preserves, and its 741 kW average system size is likely agricultural or "
    "large-property solar. The same pattern shows up across the list: Paicines, Lebec, Terra Bella, "
    "Aromas, and San Juan Bautista are small rural places rather than urban centers."
)

st.subheader("What this means")
st.write(
    "This changes the interpretation of solar adoption. The permit count chart shows urban homeowners "
    "buying small systems by the thousands, while the system-size chart shows rural landowners buying "
    "massive systems one at a time. Both count as solar adoption, but economically, environmentally, "
    "and behaviorally, they are almost completely different markets."
)

st.divider()

st.subheader("Solar adoption shifted from flatline to breakout")
st.image(adoption_time_chart_path, use_container_width=True)
st.write(
    "From 2018 through 2020, monthly permit counts were modest — typically 20-100 per month, "
    "with a brief COVID dip to under 20 in spring 2020. By 2021 and 2022, the market had grown "
    "to roughly 100-200 permits per month. The pace accelerated sharply in early 2023: monthly "
    "permits jumped to nearly 750 in April 2023, which may line up with Inflation Reduction Act "
    "tax credits making solar more affordable for homeowners. Through 2024, adoption climbed "
    "steadily from around 250 to over 650 per month. By 2025, the floor had risen to roughly "
    "700 permits per month and reached over 1,600 by year-end."
)
st.write(
    "March 2026 stands out as the major anomaly, peaking at 4,592 permits. That sudden jump could "
    "reflect a policy deadline, a rush to file before incentives changed, a large data batch upload, "
    "or a genuine surge in demand. As of mid-April 2026, the month has recorded 1,151 permits — "
    "still well above the historical baseline, though April is not yet complete. Even so, the level "
    "is far above pre-2023 norms, suggesting that the floor for solar adoption has permanently moved higher."
)

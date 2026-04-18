# Heat Trace

Heat Trace is a Streamlit prototype for exploring relationships between U.S. energy activity, solar adoption, and environmental indicators across states.

The first pass uses generated state-level mock data. The processed data schema is intentionally simple so real EIA, ZenPower, and EPA ingestion can replace the mock data later.

## Folder structure

```text
.
├── app/
│   ├── Home.py
│   ├── pages/
│   │   ├── 1_State_Explorer.py
│   │   └── 2_Insights.py
│   └── utils.py
├── data/
│   ├── raw/
│   └── processed/
├── scripts/
│   └── build_mock_data.py
├── requirements.txt
└── README.md
```

## Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Generate mock data

```bash
python scripts/build_mock_data.py
```

This writes:

```text
data/processed/heat_trace_state_timeseries.parquet
```

The Streamlit app will also generate this file automatically if it is missing.

## Run the app

```bash
streamlit run app/Home.py
```

Open the local URL shown by Streamlit, usually `http://localhost:8501`.

## Data model

The processed parquet dataset is state-level annual time series data with these columns:

- `state`
- `state_abbr`
- `date`
- `year`
- `energy_consumption`
- `energy_production`
- `solar_capacity_added`
- `co2_emissions`
- `air_quality_index`
- `clean_ratio`
- `emissions_intensity`
- `solar_growth_rate`
- `impact_gap_flag`

Derived metrics:

- `clean_ratio = solar_capacity_added / energy_production`
- `emissions_intensity = co2_emissions / energy_consumption`
- `solar_growth_rate = percent change in solar_capacity_added over time`
- `impact_gap_flag = True when solar grows but emissions do not clearly improve`

## Where real ingestion plugs in later

Keep raw source downloads in `data/raw/` and write normalized, state-level outputs to `data/processed/`.

Suggested next ingestion modules:

- EIA: state energy consumption and production.
- ZenPower: solar adoption or distributed energy additions.
- EPA: emissions and air-quality indicators.

As long as those pipelines produce the same processed parquet schema, the Streamlit pages can keep using `app/utils.py` without major changes.

## App pages

- Home: national metric cards, U.S. choropleth map, and top/bottom state rankings.
- State Explorer: state selector, time-series charts, and a generated state summary.
- Insights: rule-based flags for impact gaps, high-pollution/low-solar states, and strong solar adoption with improving indicators.

import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
from datetime import datetime
import numpy as np

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='Riderships Dashboard',
    page_icon=':metro:',
)

# -------------------------------------------------------------------------------
# Utility functions

@st.cache_data
def get_ridership_predictions():
    """
    Load MTA subway ridership predictions from a CSV file.
    Returns a DataFrame with parsed and cleaned data.
    """
    DATA_FILENAME = Path(__file__).parent/'data/riderships_predictions.csv'
    raw_predictions = pd.read_csv(DATA_FILENAME)

    raw_predictions["station_complex_id"] = raw_predictions[
        "station_complex_id"
    ].astype(str)
    raw_predictions["created_date"] = pd.to_datetime(raw_predictions["created_date"])
    raw_predictions["date"] = raw_predictions["created_date"].dt.date

    return raw_predictions

@st.cache_data
def get_latest_data():
    """
    Load and process the latest MTA subway ridership data from a CSV file.
    Scales the 'number_of_riderships' for use in a map chart.
    """
    min_val, max_val = 1, 1000

    DATA_FILENAME = Path(__file__).parent/'data/mta-subway-transformed.csv'
    raw_data = pd.read_csv(DATA_FILENAME)

    last_date = raw_data["created_date"].max()
    latest_data = raw_data[raw_data["created_date"] == last_date]

    # Normalize the 'number_of_riderships' column
    min_original = latest_data["number_of_riderships"].min()
    max_original = latest_data["number_of_riderships"].max()
    latest_data["number_of_riderships_scaled"] = (
        (latest_data["number_of_riderships"] - min_original)
        / (max_original - min_original)
    ) * (max_val - min_val) + min_val

    return latest_data

# Load data
ridership_predictions_df = get_ridership_predictions()
latest_data_df = get_latest_data()

# -------------------------------------------------------------------------------
# Page Content

# Page header
st.markdown(
    """
    # :metro: Manhattan Subway Stations Ridership Dashboard

    Visualize subway ridership data by station, sourced from the [MTA](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-July-2020/wujg-7c2s/about_data).
    The original hourly data has been aggregated to daily values. This dataset covers January 2023 through September 2024,
    with predictions extending to October 2024 for the top 20 stations with the highest number of riderships.
    """
)

# Map visualization
st.header("Subway Ridership by Georeference", divider="gray")
with st.spinner("Generating New York map..."):
    st.map(
        latest_data_df,
        latitude="latitude",
        longitude="longitude",
        size="number_of_riderships_scaled",
    )

# Date filter
min_date = datetime.strptime("2023-01-01", "%Y-%m-%d").date()
max_date = ridership_predictions_df["date"].max()

from_year, to_year = st.slider(
    "Select the date range for ridership predictions:",
    min_value=min_date,
    max_value=max_date,
    value=[min_date, max_date],
)

# Station filter
stations = ridership_predictions_df["station_complex_id"].unique()
selected_stations = st.multiselect(
    "Select station IDs to view:", stations, default=["611"]
)

if not selected_stations:
    st.warning("Please select at least one station to display data.")

# Filter predictions
filtered_predictions = ridership_predictions_df[
    (ridership_predictions_df["station_complex_id"].isin(selected_stations))
    & (ridership_predictions_df["date"] <= to_year)
    & (ridership_predictions_df["date"] >= from_year)
]

# Display predictions chart
if not filtered_predictions.empty:
    station_name = filtered_predictions["station_complex"].unique()[0]
    st.header(f"Ridership Predictions for {station_name}", divider="gray")

    # Line chart for predictions
    base_chart = (
        alt.Chart(filtered_predictions)
        .mark_line()
        .encode(
            x=alt.X("date:T", axis=alt.Axis(title="Date")),
            y=alt.Y(
                "number_of_riderships:Q", axis=alt.Axis(title="Ridership Predictions")
            ),
            color="station_complex_id:N",
        )
    )

    prediction_lines = (
        alt.Chart(filtered_predictions)
        .mark_line(color="blue")
        .encode(x="date:T", y="predicted_mean:Q")
    )

    lower_bound = (
        alt.Chart(filtered_predictions)
        .mark_line(strokeDash=[5, 5], color="red")
        .encode(x="date:T", y="lower_bound:Q")
    )

    upper_bound = (
        alt.Chart(filtered_predictions)
        .mark_line(strokeDash=[5, 5], color="green")
        .encode(x="date:T", y="upper_bound:Q")
    )

    final_chart = base_chart + prediction_lines + lower_bound + upper_bound
    st.altair_chart(final_chart, use_container_width=True)
else:
    st.info("No data available for the selected filters.")

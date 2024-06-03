# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
from native_sdk_api.observability import get_aggregated_connector_stats


def home_page():
    data_frame = get_aggregated_connector_stats()
    if not data_frame.empty:
        st.vega_lite_chart(
            data_frame,
            {
                "mark": {
                    "type": "bar",
                    "width": {
                        "band": 0.8 if len(data_frame.index) == 1 else 0.95,
                    },
                },
                "encoding": {
                    "x": {"field": "RUN_DATE", "type": "temporal", "timeUnit": "dayhours"},
                    "y": {"field": "UPDATED_ROWS", "type": "quantitative", "aggregate": "mean"},
                },
            },
            use_container_width=True)
    else:
        st.info("No ingested rows in the chosen period of time. Start the ingestion in order to display the chart.")

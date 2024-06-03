# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
from daily_use.home_page import home_page
from daily_use.data_sync_page import data_sync_page
from daily_use.settings_page import settings_page


def daily_use_page():
    home_tab, data_sync_tab, settings_tab = st.tabs(["Home", "Data Sync", "Settings"])

    with home_tab:
        home_page()

    with data_sync_tab:
        data_sync_page()

    with settings_tab:
        settings_page()

# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
from native_sdk_api.connector_status import (
    load_connector_statuses,
    is_connector_in_error,
    is_connector_configuring
)
from daily_use.daily_use_layout import daily_use_page
from wizard.wizard_layout import wizard_page


load_connector_statuses()

if is_connector_in_error():
    st.error("Unexpected error has occurred, please reinstall the application")
elif is_connector_configuring():
    wizard_page()
else:
    daily_use_page()

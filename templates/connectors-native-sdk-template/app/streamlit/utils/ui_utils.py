# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
from native_sdk_api.connector_response import ConnectorResponse


def show_vertical_space(empty_lines: int):
    for _ in range(empty_lines):
        st.write("")


def show_error(error: str):
    st.error(error)


def show_error_response(response: ConnectorResponse, called_procedure: str):
    st.error(
        f"An error response with code {response.get_response_code()} has been returned by "
        f"{called_procedure}: {response.get_message()}"
    )

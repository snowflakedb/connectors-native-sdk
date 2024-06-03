# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
import wizard.wizard_step as ws
from utils.sf_utils import validate_identifier, validate_fully_qualified_object_name
from utils.ui_utils import show_vertical_space, show_error, show_error_response
from native_sdk_api.connector_status import is_connection_configured
from native_sdk_api.connection_config import (
    set_connection_configuration,
    get_connection_configuration
)


def connection_config_page():
    load_current_config()

    st.header("Connect to GitHub API")
    st.caption(
        "To setup the GitHub API connection you need to provide the following objects, created in "
        "your Snowflake account"
    )
    st.divider()

    st.subheader("External access integration")
    input_col, _ = st.columns([2, 1])
    with input_col:
        st.text_input("", key="ext_acc_int", label_visibility="collapsed")
    st.caption(
        "Name of the external access integration, which will be used to connect to the GitHub API. "
        "The following privileges must be granted to the app, in order to use the integration:"
    )
    st.caption("- `USAGE` on the database in which the integration is located")
    st.caption("- `USAGE` on the schema in which the integration is located")
    st.caption("- `READ` on the secret used in the integration")
    st.caption("- `USAGE` on the integration")
    show_vertical_space(3)

    st.subheader("Secret")
    input_col, _ = st.columns([2, 1])
    with input_col:
        st.text_input("", key="secret", label_visibility="collapsed")
    st.caption(
        "Fully qualified name of the secret containing the GitHub API token, used to create the "
        "external access integration provided above"
    )
    st.divider()

    _, btn_col = st.columns([3.45, 0.55])
    with btn_col:
        st.button(
            "Connect",
            on_click=finish_config,
            type="primary"
        )


def load_current_config():
    if is_connection_configured():
        current_config = get_connection_configuration()

        if not st.session_state.get("ext_acc_int"):
            st.session_state["ext_acc_int"] = current_config.get("external_access_integration", "")
        if not st.session_state.get("secret"):
            st.session_state["secret"] = current_config.get("secret", "")


def finish_config():
    if not validate_identifier(st.session_state["ext_acc_int"]):
        show_error("Invalid object name provided for the external access integration")
        return
    if not validate_fully_qualified_object_name(st.session_state["secret"]):
        show_error("Invalid object name provided for the secret")
        return

    try:
        response = set_connection_configuration(
            external_access_integration=st.session_state["ext_acc_int"],
            secret=st.session_state["secret"]
        )

        if response.is_ok():
            ws.change_step(ws.FINALIZE_CONFIG)
        else:
            show_error_response(response=response, called_procedure="SET_CONNECTION_CONFIGURATION")
    except:
        show_error("Unexpected error occurred, correct the provided data and try again")

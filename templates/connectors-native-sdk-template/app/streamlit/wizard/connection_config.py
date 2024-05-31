# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
import wizard.wizard_step as ws
from utils.ui_utils import show_vertical_space, show_error, show_error_response
from native_sdk_api.connector_status import is_connection_configured
from native_sdk_api.connection_config import (
    set_connection_configuration,
    get_connection_configuration
)


def connection_config_page():
    load_current_config()

    st.header("Connect to External API")
    st.caption(
        "To setup the connection to the source system you might need to provide some additional configuration. "
        "If you need external access integration or some secret they have to be created in the account and then specified here."
    )
    st.caption("For more information please check the [documentation](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/connection_configuration) and the [reference](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/connection_configuration_reference)")
    st.divider()

    st.subheader("Custom connection configuration parameter")
    input_col, _ = st.columns([2, 1])
    with input_col:
        st.text_input("", key="custom_connection_property", label_visibility="collapsed")
    st.caption(
        "Here you can specify some additional connection parameters like secrets, external access integrations etc. "
        "This configuration step will save them and test the connection to the source system. "
        "However, this connection check and any additional logic have to be implemented first. "
        "To implement those classes look for the following comments in the Java code of the application"
    )
    st.caption("- IMPLEMENT ME test connection")
    st.caption("- IMPLEMENT ME connection configuration validate")
    st.caption("- IMPLEMENT ME connection callback")
    # TODO: Additional configuration properties can be added to the UI like this:
    # st.subheader("Additional connection parameter")
    # input_col, _ = st.columns([2, 1])
    # with input_col:
    #     st.text_input("", key="additional_connection_property", label_visibility="collapsed")
    # st.caption(
    #     "Some description of the additional property"
    # )
    show_vertical_space(3)

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

        if not st.session_state.get("custom_connection_property"):
            st.session_state["custom_connection_property"] = current_config.get("custom_connection_property", "")


def finish_config():
    try:
        # TODO: If some additional properties were specified they need to be passed to the set_connection_configuration function.
        # The properties can also be validated, for example, check whether they are not blank strings etc.
        response = set_connection_configuration(
            custom_connection_property=st.session_state["custom_connection_property"],
        )

        if response.is_ok():
            ws.change_step(ws.FINALIZE_CONFIG)
        else:
            show_error_response(response=response, called_procedure="SET_CONNECTION_CONFIGURATION")
    except:
        show_error("Unexpected error occurred, correct the provided data and try again")

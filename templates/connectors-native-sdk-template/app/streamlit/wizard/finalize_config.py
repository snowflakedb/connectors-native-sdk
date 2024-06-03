# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
import wizard.wizard_step as ws
from utils.ui_utils import show_vertical_space, show_error
from native_sdk_api.finalize_config import finalize_connector_configuration


def finalize_config_page():
    st.header("Finalize connector configuration")
    st.caption(
        "Finalisation step allows the user to provide some additional properties needed by the connector. "
        "Those properties are not saved by default in the database "
        "and this behavior needs to be implemented in the internal callback if required. "
        "Additionally, this step performs more sophisticated validations on the source system if needed. "
        "Another responsibility of this step is to prepare sink database for the ingested date, "
        "any other needed database entities and start task reactor instances and scheduler. "
    )
    st.caption("For more information please check the [documentation](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/finalize_configuration) and the [reference](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/finalize_configuration_reference)")
    st.divider()

    st.subheader("Custom property")
    input_col, _ = st.columns([2, 1])
    with input_col:
        st.text_input(
            "",
            key="custom_property",
            label_visibility="collapsed",
            value="some_value"
        )
    st.caption(
        "Custom property for finalization, if you want to use it somehow, it needs to be implemented."
        "For custom implementations search for the following:"
    )
    st.caption("- IMPLEMENT ME finalize internal")
    st.caption("- IMPLEMENT ME validate source")
    # TODO: Here you can add additional fields in finalize connector configuration.
    # For example:
    # st.subheader("Some additional property")
    # input_col, _ = st.columns([2, 1])
    # with input_col:
    #     st.text_input("", key="some_additional_property", label_visibility="collapsed")
    # st.caption("Description of some new additional property")
    show_vertical_space(3)
    st.divider()

    if st.session_state.get("show_main_error"):
        st.error(st.session_state.get("error_msg"))
    _, finalize_btn_col = st.columns([2.95, 1.05])
    with finalize_btn_col:
        st.button(
            "Finalize configuration",
            on_click=finalize_configuration,
            type="primary"
        )


def finalize_configuration():
    try:
        st.session_state["show_main_error"] = False
        # TODO: If some additional properties were introduced, they need to be passed to the finalize_connector_configuration function.
        response = finalize_connector_configuration(st.session_state.get("custom_property"))
        if response.is_ok():
            ws.change_step(ws.FINALIZE_CONFIG)
        else:
            st.session_state["error_msg"] = f"An error response with code {response.get_response_code()} " \
                                            f"has been returned by FINALIZE_CONNECTOR_CONFIGURATION: " \
                                            f"{response.get_message()}"
            st.session_state["show_main_error"] = True
    except:
        show_error("Unexpected error occurred, correct the provided data and try again")

# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
import wizard.wizard_step as ws
from utils.ui_utils import show_error, show_error_response
from native_sdk_api.prerequisites import (
    fetch_prerequisites,
    update_prerequisite,
    mark_all_prerequisites_as_done,
    complete_prerequisite_step,
    Prerequisite
)


def prerequisites_page():
    prerequisites = fetch_prerequisites()

    st.header("Prerequisites")
    st.caption(
        "Before starting the actual connector configuration process, make sure that you meet all "
        "the prerequisites listed below. You don't need to complete this step before starting the "
        "configuration, but it will help in making you sure that you are ready to start the "
        "configuration process."
    )
    st.caption("For more information on the prerequisites check the [documentation](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/prerequisites) and the [reference](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/prerequisites_reference)")
    st.divider()

    for prerequisite in prerequisites:
        single_prerequisite_layout(prerequisite)

    _, mark_btn_col, conf_btn_call = st.columns([2.6, 1.25, 1.15])
    with mark_btn_col:
        st.button(
            "Mark all as done",
            on_click=call_mark_all_prerequisites_as_done,
            key="mark_all_as_done_btn",

        )
    with conf_btn_call:
        st.button(
            "Start configuration",
            on_click=call_complete_prerequisites_step,
            key="complete_prerequisites_step_btn",
            type="primary"
        )


def single_prerequisite_layout(prerequisite: Prerequisite):
    desc_col, check_col = st.columns([9, 2])
    with desc_col:
        st.subheader(prerequisite.get_title())
        st.caption(f"{prerequisite.get_description()}")
        if prerequisite.get_documentation_url():
            st.markdown(f"[Learn more]({prerequisite.get_documentation_url()})")
    with check_col:
        checkbox_key = f"prerequisite_checkbox_{prerequisite.get_prerequisite_id()}"
        st.text("Completed:")
        st.checkbox(
            "",
            value=prerequisite.get_is_completed(),
            key=checkbox_key,
            label_visibility="hidden",
            on_change=call_update_prerequisite,
            args=(
                  checkbox_key,
                  prerequisite.get_prerequisite_id(),
                  prerequisite.get_is_completed()
            )
        )
    st.divider()


def call_update_prerequisite(checkbox_key: str, prerequisite_id: str, is_completed: bool):
    checkbox_state = st.session_state.get(checkbox_key)
    if not checkbox_state == is_completed:
        response = update_prerequisite(prerequisite_id, checkbox_state)
        if not response.is_ok():
            show_error_response(response=response, called_procedure="UPDATE_PREREQUISITE")


def call_mark_all_prerequisites_as_done():
    response = mark_all_prerequisites_as_done()
    if not response.is_ok():
        show_error_response(response=response, called_procedure="MARK_ALL_PREREQUISITES_AS_DONE")


def call_complete_prerequisites_step():
    try:
        response = complete_prerequisite_step()

        if response.is_ok():
            ws.change_step(ws.CONNECTOR_CONFIG)
        else:
            show_error_response(response=response, called_procedure="COMPLETE_PREREQUISITES_STEP")
    except:
        show_error("Unexpected error occurred, correct the provided data and try again")

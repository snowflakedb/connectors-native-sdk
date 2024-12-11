# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
import wizard.wizard_step as ws
from utils.ui_utils import show_vertical_space, show_error
from native_sdk_api.finalize_config import finalize_connector_configuration


def finalize_config_page():
    st.header("Finalize connector configuration")
    st.caption(
        "In order to start the connector, you need to finalize the connector configuration by "
        "validating the source. The validation is about requesting the GitHub API endpoint "
        "**https://api.github.com/repos/OWNER/REPO** to check, if connector will be able to fetch "
        "repository data."
    )
    st.divider()

    st.subheader("Organization name")
    input_col, _ = st.columns([2, 1])
    with input_col:
        org_name_st = st.text_input(
            "",
            key="org_name_tb",
            label_visibility="collapsed",
            value="snowflakedb"
        )
    if st.session_state.get("is_org_tb_empty"):
        st.error("Organization name text box can not be empty. Please fill it.")
    st.caption("Name of the Organization from which the Repository will be fetched")
    show_vertical_space(3)

    st.subheader("Repository name")
    input_col, _ = st.columns([2, 1])
    with input_col:
        repo_name_tb = st.text_input(
            "",
            key="repo_name_tb",
            label_visibility="collapsed",
            value="connectors-native-sdk"
        )
    if st.session_state.get("is_repo_tb_empty"):
        st.error("Repository name text box can not be empty. Please fill it.")
    st.caption("Name of the fetched Repository")
    show_vertical_space(3)
    st.divider()

    if st.session_state.get("show_main_error"):
        st.error(st.session_state.get("error_msg"))
    _, finalize_btn_col = st.columns([2.95, 1.05])
    with finalize_btn_col:
        st.button(
            "Finalize configuration",
            on_click=finalize_configuration,
            args=(org_name_st, repo_name_tb),
            type="primary"
        )


def are_text_boxes_empty(org_name_tb: str, repo_name_tb: str):
    result = False
    if not org_name_tb:
        st.session_state["is_org_tb_empty"] = True
        result = True
    else:
        st.session_state["is_org_tb_empty"] = False
    if not repo_name_tb:
        st.session_state["is_repo_tb_empty"] = True
        result = True
    else:
        st.session_state["is_repo_tb_empty"] = False
    return result


def finalize_configuration(org_name: str, repo_name: str):
    if are_text_boxes_empty(org_name, repo_name):
        return

    try:
        st.session_state["show_main_error"] = False
        response = finalize_connector_configuration(org_name, repo_name)
        if response.is_ok():
            st.snow()
            ws.change_step(ws.FINALIZE_CONFIG)
        else:
            st.session_state["error_msg"] = f"An error response with code {response.get_response_code()} " \
                                            f"has been returned by FINALIZE_CONNECTOR_CONFIGURATION: " \
                                            f"{response.get_message()}"
            st.session_state["show_main_error"] = True
    except:
        show_error("Unexpected error occurred, correct the provided data and try again")

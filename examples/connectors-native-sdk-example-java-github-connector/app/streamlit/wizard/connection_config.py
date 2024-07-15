# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
import wizard.wizard_step as ws
from utils.ui_utils import show_vertical_space, show_error, show_error_response
from native_sdk_api.connection_config import set_connection_configuration
from utils.permission_sdk_utils import (
    request_github_eai_ref,
    get_github_eai_ref,
    get_github_secret_ref
)


def connection_config_page():
    st.header("Connect to GitHub API")

    st.caption("Before setting up the GitHub API connection you have to:")
    st.caption(" 1. Create a new GitHub app in your account")
    st.caption(" 2. Install the app in your GitHub account")
    st.caption(" 3. Copy the client ID and client secret from your GitHub app")
    st.caption("")
    st.caption("Refer to the example quickstart for more information about GitHub apps setup")

    st.divider()

    st.subheader("External access integration")
    eai_ref = get_github_eai_ref()
    input_col, btn_col = st.columns([2, 1])
    with input_col:
        st.text_input(
            "",
            value=(eai_ref[0] if eai_ref else ""),
            key="ext_acc_int",
            disabled=True,
            label_visibility="collapsed"
        )
    with btn_col:
        st.button(
            "Request access",
            on_click=request_github_eai_ref
        )
    st.caption(
        "Reference to an external access integration, which will be used to connect to the GitHub "
        "API. The reference will be automatically created during the OAuth authorization process"
    )
    show_vertical_space(3)

    st.subheader("Secret")
    secret_ref = get_github_secret_ref()
    input_col, _ = st.columns([2, 1])
    with input_col:
        st.text_input(
            "",
            value=(secret_ref[0] if secret_ref else ""),
            key="secret",
            disabled=True,
            label_visibility="collapsed"
        )
    st.caption(
        "Reference to the secret containing the GitHub API token, it will be created automatically "
        "while creating the external access integration requested above"
    )
    st.divider()

    _, btn_col = st.columns([3.45, 0.55])
    with btn_col:
        st.button(
            "Connect",
            on_click=finish_config,
            type="primary"
        )


def finish_config():
    if not get_github_eai_ref():
        show_error("External access integration reference was not set in the application")
        return
    if not get_github_secret_ref():
        show_error("Secret reference was not set in the application")
        return

    try:
        response = set_connection_configuration()

        if response.is_ok():
            ws.change_step(ws.FINALIZE_CONFIG)
        else:
            show_error_response(response=response, called_procedure="SET_CONNECTION_CONFIGURATION")
    except:
        show_error("Unexpected error occurred, correct the provided data and try again")

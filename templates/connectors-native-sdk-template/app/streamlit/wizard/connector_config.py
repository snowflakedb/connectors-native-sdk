# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
import wizard.wizard_step as ws
from utils.sf_utils import validate_identifier
from utils.ui_utils import show_vertical_space, show_error, show_error_response
from native_sdk_api.connector_status import is_connector_configured
from native_sdk_api.connector_config import configure_connector, get_connector_configuration
from utils.permission_sdk_utils import (
    get_missing_privileges,
    get_warehouse_ref,
    get_held_account_privileges,
    request_required_privileges,
    request_warehouse_ref
)


def connector_config_page():
    load_current_config()

    st.header("Configure connector")
    st.caption(
        "To complete the configuration and run the connector, the following objects will be "
        "created in your Snowflake environment."
    )
    st.caption("More can be found in the [documentation](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/connector_configuration) and the [reference](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/connector_configuration_reference)")
    st.divider()

    st.subheader("Application privileges")
    input_col, btn_col = st.columns([2, 1])
    with input_col:
        st.text_input(
            "",
            value=", ".join(get_held_account_privileges()),
            key="app_privileges",
            disabled=True,
            label_visibility="collapsed",
        )
    with btn_col:
        st.button(
            "Grant privileges",
            disabled=(not bool(get_missing_privileges())),
            on_click=request_required_privileges
        )
    st.caption("Specified privileges must be granted to the application before it can be started. This can be modified in the `manifest.yml` file and all the missing account level privileges will be requested automatically")
    show_vertical_space(3)

    st.subheader("Warehouse reference")
    wh_ref = get_warehouse_ref()
    input_col, btn_col = st.columns([2, 1])
    with input_col:
        st.text_input(
            "",
            value=(wh_ref[0] if wh_ref else ""),
            key="warehouse",
            disabled=True,
            label_visibility="collapsed"
        )
    with btn_col:
        st.button(
            "Choose warehouse",
            on_click=request_warehouse_ref
        )
    st.caption(
        "A reference to an existing warehouse must be provided via the popup or in the "
        "Security tab of your Native App. The USAGE privilege on the warehouse must also "
        "be granted. This warehouse will be used by all Snowflake tasks created within the connector."
    )
    show_vertical_space(3)

    st.subheader("Destination database")
    input_col, _ = st.columns([2, 1])
    with input_col:
        st.text_input("", key="dest_db", label_visibility="collapsed")
    st.caption(
        "Name of the new database, which will be created to store ingested data. The database "
        "must not already exist in your Snowflake account"
    )
    show_vertical_space(3)

    st.subheader("Destination schema")
    input_col, _ = st.columns([2, 1])
    with input_col:
        st.text_input("", key="dest_schema", label_visibility="collapsed")
    st.caption("Name of the new schema, which will be created to store ingested data")
    # TODO: Here you can add additional fields in connector configuration. Supported values are the following: warehouse, operational_warehouse, data_owner_role, agent_role, agent_username, cortex_warehouse, cortex_user_role
    # For example:
    # st.subheader("Operational warehouse")
    # input_col, _ = st.columns([2, 1])
    # with input_col:
    #     st.text_input("", key="operational_warehouse", label_visibility="collapsed")
    # st.caption("Name of the operational warehouse to be used")
    st.divider()

    _, btn_col = st.columns([3.4, 0.6])
    with btn_col:
        st.button(
            "Configure",
            on_click=finish_config,
            type="primary"
        )


def load_current_config():
    if is_connector_configured():
        current_config = get_connector_configuration()

        if not st.session_state.get("dest_db"):
            st.session_state["dest_db"] = current_config.get("destination_database", "")
        if not st.session_state.get("dest_schema"):
            st.session_state["dest_schema"] = current_config.get("destination_schema", "")


def finish_config():
    if get_missing_privileges():
        show_error("Required privileges were not granted to the application")
        return
    if not get_warehouse_ref():
        show_error("Warehouse reference was not set in the application")
        return
    if not validate_identifier(st.session_state["dest_db"]):
        show_error("Invalid identifier provided for the destination database")
        return
    if not validate_identifier(st.session_state["dest_schema"]):
        show_error("Invalid identifier provided for the destination schema")
        return

    try:
        # TODO: If some additional properties were added they need to be passed to the configure_connector function
        response = configure_connector(
            dest_db=st.session_state["dest_db"],
            # operational_warehouse = st.session_state["operational_warehouse"],
            dest_schema=st.session_state["dest_schema"]
        )

        if response.is_ok():
            ws.change_step(ws.CONNECTION_CONFIG)
        else:
            show_error_response(response=response, called_procedure="CONFIGURE_CONNECTOR")
    except:
        show_error("Unexpected error occurred, correct the provided data and try again")

# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
from native_sdk_api.connector_config import get_connector_configuration
from native_sdk_api.connection_config import get_connection_configuration
from utils.permission_sdk_utils import (
    get_held_account_privileges,
    get_warehouse_ref
)


def settings_page():
    connector, connection = st.tabs(["Connector configuration", "Connection configuration"])
    with connector:
        connector_config_page()
    with connection:
        connection_config_page()


def connector_config_page():
    current_config = get_connector_configuration()

    warehouse_reference = get_warehouse_ref()[0]
    granted_privileges = ", ".join(get_held_account_privileges())
    destination_database = current_config.get("destination_database", "")
    destination_schema = current_config.get("destination_schema", "")

    st.header("Connector configuration")
    st.caption("Here you can see the general connector configuration saved during the connector configuration step of "
               "the Wizard.")
    st.divider()

    st.text_input(
        "Granted privileges:",
        value=granted_privileges,
        disabled=True
    )
    st.text_input(
        "Warehouse reference:",
        value=warehouse_reference,
        disabled=True
    )
    st.text_input(
        "Destination database:",
        value=destination_database,
        disabled=True
    )
    st.text_input(
        "Destination schema:",
        value=destination_schema,
        disabled=True
    )
    st.divider()


def connection_config_page():
    current_config = get_connection_configuration()

    secret = current_config.get("secret", "")
    external_access_integration = current_config.get("external_access_integration", "")

    st.header("Connector configuration")
    st.caption("Here you can see the connector connection configuration saved during the connection configuration step "
               "of the Wizard.")
    st.divider()

    st.text_input(
        "Secret:",
        value=secret,
        disabled=True
    )
    st.text_input(
        "External access integration:",
        value=external_access_integration,
        disabled=True
    )
    st.divider()

# Copyright (c) 2024 Snowflake Inc.

import json
import streamlit as st
from enum import Enum
from snowflake.snowpark.context import get_active_session


def load_connector_statuses():
    statuses = json.loads(
        get_active_session()
        .sql("CALL PUBLIC.GET_CONNECTOR_STATUS()")
        .collect()[0][0]
    )

    st.session_state["connector_status"] = ConnectorStatus[statuses.get("status")]
    st.session_state["configuration_status"] = ConfigurationStatus[
        statuses.get("configurationStatus")
    ]


def get_connector_status():
    return st.session_state["connector_status"]


def get_configuration_status():
    return st.session_state["configuration_status"]


def is_connector_in_error():
    return get_connector_status() == ConnectorStatus.ERROR


def is_connector_configuring():
    return get_connector_status() == ConnectorStatus.CONFIGURING


def is_connector_configured():
    return get_configuration_status() in [
        ConfigurationStatus.CONFIGURED,
        ConfigurationStatus.CONNECTED
    ]


def is_connection_configured():
    return get_configuration_status() in [
        ConfigurationStatus.CONNECTED
    ]


class ConnectorStatus(Enum):
    CONFIGURING = "CONFIGURING"
    STARTING = "STARTING"
    STARTED = "STARTED"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class ConfigurationStatus(Enum):
    INSTALLED = "INSTALLED"
    PREREQUISITES_DONE = "PREREQUISITES_DONE"
    CONFIGURED = "CONFIGURED"
    CONNECTED = "CONNECTED"
    FINALIZED = "FINALIZED"

# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
from utils.ui_utils import show_error
from native_sdk_api.connector_status import get_configuration_status, ConfigurationStatus


class WizardStep:
    def __init__(self, sidebar_label: str, allowed_statuses: list):
        self._sidebar_label: str = sidebar_label
        self._allowed_statuses: list = allowed_statuses

    def get_sidebar_label(self):
        return self._sidebar_label

    def is_current_status_allowed(self):
        return st.session_state["configuration_status"] in self._allowed_statuses


PREREQUISITES = WizardStep(
    sidebar_label="1\.  Prerequisites",
    allowed_statuses=[
        ConfigurationStatus.INSTALLED,
        ConfigurationStatus.PREREQUISITES_DONE,
        ConfigurationStatus.CONFIGURED,
        ConfigurationStatus.CONNECTED
    ]
)

CONNECTOR_CONFIG = WizardStep(
    sidebar_label="2\.  Connector configuration",
    allowed_statuses=[
        ConfigurationStatus.INSTALLED,
        ConfigurationStatus.PREREQUISITES_DONE,
        ConfigurationStatus.CONFIGURED,
        ConfigurationStatus.CONNECTED
    ]
)

CONNECTION_CONFIG = WizardStep(
    sidebar_label="3\.  Connection configuration",
    allowed_statuses=[ConfigurationStatus.CONFIGURED, ConfigurationStatus.CONNECTED]
)

FINALIZE_CONFIG = WizardStep(
    sidebar_label="4\.  Finalize configuration",
    allowed_statuses=[ConfigurationStatus.CONNECTED]
)

__CONFIG_STATUS_TO_WIZARD_STEP = {
    ConfigurationStatus.INSTALLED: PREREQUISITES,
    ConfigurationStatus.PREREQUISITES_DONE: CONNECTOR_CONFIG,
    ConfigurationStatus.CONFIGURED: CONNECTION_CONFIG,
    ConfigurationStatus.CONNECTED: FINALIZE_CONFIG
}


def determine_wizard_step():
    status = get_configuration_status()
    determined_step = __CONFIG_STATUS_TO_WIZARD_STEP.get(status)

    if determined_step is not None:
        return determined_step
    else:
        show_error(f"Configuration status {status} should not trigger the configuration wizard")


def get_current_step():
    if "wizard_step" not in st.session_state:
        st.session_state["wizard_step"] = determine_wizard_step()
    return st.session_state["wizard_step"]


def change_step(new_step: WizardStep, rerun: bool = False):
    st.session_state["wizard_step"] = new_step
    if rerun:
        st.experimental_rerun()

# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
import wizard.wizard_step as ws
from utils.ui_utils import show_error
from wizard.connection_config import connection_config_page
from wizard.connector_config import connector_config_page
from wizard.finalize_config import finalize_config_page
from wizard.prerequisites import prerequisites_page


def wizard_page():
    step = ws.get_current_step()
    wizard_sidebar(step)

    if step == ws.PREREQUISITES:
        prerequisites_page()
    elif step == ws.CONNECTOR_CONFIG:
        connector_config_page()
    elif step == ws.CONNECTION_CONFIG:
        connection_config_page()
    elif step == ws.FINALIZE_CONFIG:
        finalize_config_page()
    else:
        show_error("Unknown step for the configuration wizard")


def wizard_sidebar(step: ws.WizardStep):
    with st.sidebar:
        st.header("Configuration steps")

        prepare_step_button(step, ws.PREREQUISITES)
        prepare_step_button(step, ws.CONNECTOR_CONFIG)
        prepare_step_button(step, ws.CONNECTION_CONFIG)
        prepare_step_button(step, ws.FINALIZE_CONFIG)


def prepare_step_button(current_step: ws.WizardStep, target_step: ws.WizardStep):
    if current_step == target_step:
        button_label = f":blue[{target_step.get_sidebar_label()}]"
    else:
        button_label = target_step.get_sidebar_label()

    is_disabled = not target_step.is_current_status_allowed()
    if st.button(button_label, disabled=is_disabled, use_container_width=True):
        ws.change_step(target_step, True)

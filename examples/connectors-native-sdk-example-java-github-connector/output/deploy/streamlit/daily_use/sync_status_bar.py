# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
import pandas as pd
import json
from utils.sql_utils import (call_procedure, select_all_from)
from native_sdk_api.connector_config import get_connector_configuration


def sync_status_bar():
    with st.container(border=True):
        global_schedule_config = json.loads(get_connector_configuration().get("global_schedule"))
        frame = pd.DataFrame(select_all_from("PUBLIC.SYNC_STATUS"))
        status = frame.iloc[0]['STATUS']
        last_synced_at = frame.iloc[0]['LAST_SYNCED_AT']

        header_col, button_col = st.columns([3, 2])
        with header_col:
            st.text_input(
                "Global schedule:",
                value=f"{global_schedule_config['scheduleType']}: {global_schedule_config['scheduleDefinition']}",
                disabled=True
            )

        with button_col:
            __display_start_pause_button(status)
            __display_sync_status(status, last_synced_at)


def __display_sync_status(status: str, timestamp):
    if status == 'SYNCING_DATA':
        st.button(
            f"**:blue[Syncing data]**",
            disabled=True,
            use_container_width=True
        )
    elif status == 'LAST_SYNCED':
        st.button(
            f"**:green[Last sync: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}]**",
            disabled=True,
            use_container_width=True
        )
    elif status == 'NOT_SYNCING':
        st.button(
            f"**:grey[Not syncing]**",
            disabled=True,
            use_container_width=True
        )
    elif status == 'PAUSED':
        st.button(
            f"**:grey[Syncing paused]**",
            disabled=True,
            use_container_width=True
        )
    else:
        st.button(
            f"**:red[Unknown sync status]**",
            disabled=True,
            use_container_width=True
        )


def __display_start_pause_button(status):
    if status in ['SYNCING_DATA', 'LAST_SYNCED', 'NOT_SYNCING']:
        st.button(
            ":black_medium_square: Pause connector",
            type="secondary",
            on_click=__pause_connector,
            use_container_width=True
        )
    elif status in ['PAUSED']:
        st.button(
            ":arrow_forward: Resume connector",
            type="primary",
            on_click=__resume_connector,
            use_container_width=True
        )
    else:
        st.button("Unknown sync status", disabled=True)


def __pause_connector():
    call_procedure('PUBLIC.PAUSE_CONNECTOR')


def __resume_connector():
    call_procedure('PUBLIC.RESUME_CONNECTOR')

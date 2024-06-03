# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
from daily_use.sync_status_bar import sync_status_bar
from native_sdk_api.resource_management import (
    create_resource,
    fetch_resources
)


def queue_resource():
    repository_name = st.session_state.get("repo_name")
    organisation_name = st.session_state.get("org_name")

    if not repository_name or not organisation_name:
        st.error("Organisation name and repository name cannot be empty.")
        return

    result = create_resource(organisation_name, repository_name)
    if result.is_ok():
        st.success("Resource created")
    else:
        st.error(result.get_message())


def data_sync_page():
    sync_status_bar()
    st.subheader("Enabled repositories")
    df = fetch_resources().to_pandas()

    with st.form("add_new_resource_form", clear_on_submit=True):
        st.caption("Enable new repository")
        st.text_input(
            "Organisation name",
            key="org_name",
        )
        st.text_input(
            "Repository name",
            key="repo_name",
        )
        _ = st.form_submit_button(
            "Queue ingestion",
            on_click=queue_resource
        )

    st.table(df)

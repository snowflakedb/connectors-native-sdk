# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
from daily_use.sync_status_bar import sync_status_bar
from native_sdk_api.resource_management import (
    create_resource,
    fetch_resources
)


def queue_resource():
    # TODO: add additional properties here and pass them to create_resource function
    resource_name = st.session_state.get("resource_name")

    if not resource_name:
        st.error("Resource name cannot be empty")
        return

    result = create_resource(resource_name)
    if result.is_ok():
        st.success("Resource created")
    else:
        st.error(result.get_message())


def data_sync_page():
    sync_status_bar()
    st.subheader("Enabled resources")
    df = fetch_resources().to_pandas()

    with st.form("add_new_resource_form", clear_on_submit=True):
        st.caption("Enable new resource. Fields required to enable a parameter might differ between the various source systems, so the field list will have to be customized here.")
        st.caption("For more information on resource definitions check the [documentation](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/resource_definition_and_ingestion_processes) and the [reference](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/resource_definition_and_ingestion_processes_reference)")
        # TODO: specify all the properties needed to define a resource in the source system. A subset of those properties should allow for a identification of a single resource, be it a table, endpoint, repository or some other data storage abstraction
        st.text_input(
            "Resource name",
            key="resource_name",
        )
        _ = st.form_submit_button(
            "Queue ingestion",
            on_click=queue_resource
        )

    st.table(df)

# Copyright (c) 2024 Snowflake Inc.

import streamlit as st
from daily_use.sync_status_bar import sync_status_bar
from native_sdk_api.resource_management import (
    create_resource,
    fetch_resources,
    enable_resource,
    disable_resource,
    Resource
)


def data_sync_page():
    sync_status_bar()

    st.subheader("Create resource")
    add_new_resource_layout()

    st.subheader("Ingested resources")
    enable_resource_layout()


def add_new_resource_layout():
    with st.form("add_new_resource_form", clear_on_submit=True):
        st.caption("Enable new resource. Fields required to enable a parameter might differ between the various source systems, so the field list will have to be customized here.")
        st.caption("For more information on resource definitions check the [documentation](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/resource_definition_and_ingestion_processes) and the [reference](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/resource_definition_and_ingestion_processes_reference)")
        # TODO: specify all the properties needed to define a resource in the source system. A subset of those properties should allow for an identification of a single resource, be it a table, endpoint, repository or some other data storage abstraction
        st.text_input(
            "Resource name",
            key="resource_name",
        )
        _ = st.form_submit_button(
            "Queue ingestion",
            on_click=queue_resource
        )


def enable_resource_layout():
    resources = fetch_resources()
    with st.container(border=True):
        if all(False for _ in resources):
            st.info("No resource has been added yet.")
        for resource in resources:
            with st.container(border=True):
                resource_data_col, enable_col = st.columns([9, 2.5])
                with resource_data_col:
                    with st.expander(f"Resource: **[{resource.get_resource_name()}]**"):
                        st.text_input(
                            "Resource name:",
                            key=f"{resource.get_resource_name()}_name_txt_input",
                            value=resource.get_resource_name(),
                            disabled=True
                        )
                        st.text_input(
                            "Resource ingestion definition ID:",
                            key=f"{resource.get_ingestion_definition_id()}_id_txt_input",
                            value=resource.get_ingestion_definition_id(),
                            disabled=True
                        )
                        st.text_input(
                            "Resource ID:",
                            key=f"{resource.get_ingestion_definition_id()}_enable_txt_input",
                            value=str(resource.get_resource_id()),
                            disabled=True
                        )
                with enable_col:
                    st.toggle(
                        "Enabled" if resource.is_enabled() else "Disabled",
                        key=resource.get_ingestion_definition_id(),
                        value=resource.is_enabled(),
                        on_change=enable_or_disable_resource,
                        args=([resource])
                    )


def enable_or_disable_resource(resource: Resource):
    if resource.is_enabled():
        disable_resource(resource)
    else:
        enable_resource(resource)


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

# Copyright (c) 2024 Snowflake Inc.
import json

import pandas as pd
import streamlit as st
import snowflake.permissions as permissions
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

if permissions.get_missing_account_privileges(["CREATE DATABASE", "EXECUTE TASK"]):
    permissions.request_account_privileges(["CREATE DATABASE", "EXECUTE TASK"])


def get_connection_configuration(session):
    connection = session.sql(
        "select value from state.app_configuration where key = 'config' limit 1"
    ).collect()
    if connection:
        return json.loads(connection[0].VALUE)
    return None


def configure_resource():
    session = get_active_session()
    result = session.sql(
        f"call public.enable_resource('{st.session_state.org_name}/{st.session_state.repo_name}')"
    ).collect()
    st.success(result[0].ENABLE_RESOURCE)


def configure_connector():
    session = get_active_session()
    config = {
        "destination_database": st.session_state.destination_database,
        "secret_name": st.session_state.secret_name,
        "external_access_integration_name": st.session_state.external_access_integration_name,
    }
    try:
        session.sql(f"call public.provision_connector(PARSE_JSON('{json.dumps(config)}'))").collect()
        st.session_state.connector_configured = True
    except Exception as err:
        st.error(err)
        st.session_state.connector_configured = False

    if not permissions.get_reference_associations("warehouse_reference"):
        permissions.request_reference("warehouse_reference")


def main():
    session: Session = get_active_session()

    st.header("GitHub Connector")

    configuration_tab, state_tab, data_preview, raw_data = st.tabs(
        ["Configuration", "State", "Data Preview", "Raw Data Preview"]
    )
    connection_configuration = get_connection_configuration(session) or {}

    st.session_state.connector_configured = bool(connection_configuration)

    if st.session_state.connector_configured:
        st.session_state.destination_database = connection_configuration["destination_database"]
        st.session_state.secret_name = connection_configuration["secret_name"]
        st.session_state.external_access_integration_name = connection_configuration[
            "external_access_integration_name"
        ]

    with configuration_tab:
        col1, col2 = st.columns([3, 2])

        with col1:
            st.subheader("Enabled resources")
            df = session.sql(
                "select key as repository, value:enabled as enabled from state.resource_configuration"
            ).to_pandas()

            with st.form("configuration_form", clear_on_submit=True):
                connector_not_configured = not st.session_state.connector_configured
                st.caption("Enable new repository")
                st.text_input(
                    "Organisation name",
                    key="org_name",
                    disabled=connector_not_configured,
                )
                st.text_input("Repo name", key="repo_name", disabled=connector_not_configured)
                _ = st.form_submit_button(
                    "Start ingestion",
                    on_click=configure_resource,
                    disabled=connector_not_configured,
                )

            st.table(df)

        with col2:
            with st.form("connector_form"):
                if st.session_state.connector_configured:
                    st.success("Connector provisioned.")

                st.text_input(
                    "Destination database",
                    disabled=st.session_state.connector_configured,
                    key="destination_database",
                    placeholder="GITHUB",
                )

                st.text_input(
                    "Secret name",
                    disabled=st.session_state.connector_configured,
                    key="secret_name",
                    placeholder="DB.SCHEMA.GITHUB_TOKEN",
                )

                st.text_input(
                    "Security integration name",
                    disabled=st.session_state.connector_configured,
                    key="external_access_integration_name",
                    placeholder="GITHUB_INTEGRATION",
                )

                _ = st.form_submit_button(
                    "Configure",
                    disabled=st.session_state.connector_configured,
                    on_click=configure_connector,
                )

    with state_tab:
        st.subheader("Connector state")
        state_df: pd.DataFrame = session.sql(
            """select
            timestamp,
            key as repository,
            value:state::string as state,
            value:ingestion:rows_count as rows_count,
            value:reason::string as reason
            from state.app_state
            qualify row_number() over (partition by repository order by timestamp desc) = 1
            """
        ).to_pandas()

        st.table(state_df)

    configured_tables = (
        session.table("state.resource_configuration").select("key").distinct().to_pandas()["KEY"]
    )

    with raw_data:
        repository = st.selectbox("Select repository", configured_tables, key="select_raw_repo")
        if repository:
            fetch_data_preview(repository, session)

    with data_preview:
        repository = st.selectbox("Select repository", configured_tables, key="select_flat_repo")
        if repository:
            fetch_data_preview(repository, session, flattened=True)


def fetch_data_preview(repository: str, session: Session, flattened: bool = False):
    table_name = repository.replace("/", "_").replace("-", "_")
    if flattened:
        table_name += "_FLATTENED"
    try:
        df = session.table(f"{st.session_state.destination_database}.PUBLIC.{table_name}").limit(10)
        st.table(df)
    except:  # noqa
        st.caption("No data yet.")
        pass


if __name__ == "__main__":
    main()

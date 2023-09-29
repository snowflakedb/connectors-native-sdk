import json

import streamlit as st
import pandas as pd
import time
import snowflake.permissions as permissions
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col


if permissions.get_missing_account_privileges(
    ["CREATE DATABASE", "EXECUTE TASK", "EXECUTE MANAGED TASK"]
):
    permissions.request_account_privileges(
        ["CREATE DATABASE", "EXECUTE TASK", "EXECUTE MANAGED TASK"]
    )


def read_tables(db_session):
    destination_database = st.session_state.destination_database
    return (
        db_session.table(f"{destination_database}.information_schema.tables")
        .filter(col("TABLE_SCHEMA") == "PUBLIC")
        .select(col("TABLE_NAME"), col("ROW_COUNT"))
        .to_pandas()
    )


def main():
    st.title("Example Push Based Connector :snowflake:")

    session: Session = get_active_session()
    connection_configuration = get_connection_configuration(session) or {}
    st.session_state.connector_configured = bool(connection_configuration)

    if st.session_state.connector_configured:
        st.session_state.destination_database = connection_configuration[
            "destination_database"
        ]

    with st.form("connector_form"):
        if st.session_state.connector_configured:
            st.success("Connector provisioned.")

        st.text_input(
            "Destination database",
            disabled=st.session_state.connector_configured,
            key="destination_database",
            placeholder="EXAMPLE_PUSH_BASED_CONNECTOR_DATA",
        )

        _ = st.form_submit_button(
            "Configure",
            disabled=st.session_state.connector_configured,
            on_click=configure_connector,
        )

    if st.session_state.connector_configured:
        tables: pd.DataFrame = read_tables(session)
        st.bar_chart(
            data=tables, x="TABLE_NAME", y="ROW_COUNT", use_container_width=True
        )

        refresh = st.checkbox("Refresh automatically")

        if refresh:
            st.write("Refresh active")
            time.sleep(1)
            st.experimental_rerun()


def get_connection_configuration(session):
    connection = session.sql(
        "select value from state.app_configuration where key = 'config' limit 1"
    ).collect()
    if connection:
        return json.loads(connection[0].VALUE)
    return None


def configure_connector():
    session = get_active_session()
    destination_database = st.session_state.destination_database

    try:
        session.sql(
            f"CALL PUBLIC.INIT_DESTINATION_DATABASE('{destination_database}')"
        ).collect()
        st.session_state.connector_configured = True
    except Exception as err:
        st.error(err)
        st.session_state.connector_configured = False


if __name__ == "__main__":
    main()

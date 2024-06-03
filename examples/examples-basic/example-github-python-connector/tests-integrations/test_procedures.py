# Copyright (c) 2024 Snowflake Inc.
import json


def test_provision_connector_procedure(snowpark_session, environment):
    config = {
        "warehouse": environment.warehouse,
        "destination_database": environment.dest_db,
        "secret_name": environment.secret_name,
        "external_access_integration_name": "GITHUB_INTEGRATION",
    }
    snowpark_session.sql(f"use database {environment.app_instance}").collect()
    result = snowpark_session.sql(f"call provision_connector(parse_json('{json.dumps(config)}'))").collect()
    assert result[0][0] == "Connector provisioned"


def test_enable_resource(snowpark_session, environment):
    snowpark_session.sql(f"use database {environment.app_instance}").collect()
    result = snowpark_session.sql("call enable_resource('Snowflake-Labs/snowcli')").collect()
    assert result[0][0] == "Snowflake-Labs/snowcli enabled"

    rows = snowpark_session.sql(f"DESC TABLE {environment.dest_db}.PUBLIC.SNOWFLAKE_LABS_SNOWCLI").collect()
    assert len(rows) == 1
    assert rows[0].name == "RAW"
    assert rows[0].type == "VARIANT"

    rows = snowpark_session.sql(
        f"DESC VIEW {environment.dest_db}.PUBLIC.SNOWFLAKE_LABS_SNOWCLI_FLATTENED"
    ).collect()
    assert len(rows) == 13


def test_ingest_data(snowpark_session, environment):
    snowpark_session.sql(f"use database {environment.app_instance}").collect()
    result = snowpark_session.sql("call ingest_data('Snowflake-Labs/snowcli')").collect()
    raw_json = json.loads(result[0][0])
    assert "extras" in raw_json
    assert raw_json["rows_count"] > 1

    assert snowpark_session.table(f"{environment.dest_db}.public.snowflake_labs_snowcli").count() > 0

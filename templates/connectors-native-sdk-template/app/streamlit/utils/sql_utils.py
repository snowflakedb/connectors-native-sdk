# Copyright (c) 2024 Snowflake Inc.

import json
from native_sdk_api.connector_response import ConnectorResponse
from snowflake.snowpark.context import get_active_session


def select_all_from(table_name):
    return get_active_session().sql(f"SELECT * FROM {table_name}").collect()


def call_procedure(procedure_name: str, arguments: list = ()) -> ConnectorResponse:
    response = (
        get_active_session()
        .sql(f"CALL {procedure_name}({__to_procedure_input(arguments)})")
        .collect()[0][0]
    )
    return ConnectorResponse(response)


def varchar_argument(argument: str):
    return f"'{argument}'"


def variant_argument(arguments: dict):
    return "PARSE_JSON('" + json.dumps(arguments) + "')"


def variant_list_argument(arguments: list):
    return "PARSE_JSON('" + json.dumps(arguments) + "')"


def __to_procedure_input(arguments: list):
    return ", ".join(arguments)

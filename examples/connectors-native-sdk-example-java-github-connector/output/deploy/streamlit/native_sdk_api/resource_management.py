# Copyright (c) 2024 Snowflake Inc.

import string, random

from utils.sql_utils import call_procedure, varchar_argument, variant_argument, variant_list_argument
from snowflake.snowpark.context import get_active_session

session = get_active_session()


def create_resource(organisation_name, repository_name):
    ingestion_config = [{
        "id": "ingestionConfig",
        "ingestionStrategy": "INCREMENTAL",
        "scheduleType": "INTERVAL",
        "scheduleDefinition": "60m"
    }]
    resource_id = {
        "organisation_name": organisation_name,
        "repository_name": repository_name
    }
    id = f"{organisation_name}.{repository_name}"

    return call_procedure("PUBLIC.CREATE_RESOURCE",
                          [
                              varchar_argument(id),
                              variant_argument(resource_id),
                              variant_list_argument(ingestion_config),
                              varchar_argument(f"{id}_{random_suffix()}"),
                              "true"
                          ])


def random_suffix():
    suffix = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(9))
    return suffix


def fetch_resources_raw():
    return session.sql(
        """
       SELECT
         id,
         resource_id:resource_name::string AS resource_name,
         ENABLED AS is_enabled,
         INGESTION_CONFIGURATION
       FROM STATE.RESOURCE_INGESTION_DEFINITION
       """
    )


def fetch_resources():
    result = session.table("STATE.RESOURCE_INGESTION_DEFINITION").collect()
    output = []
    for r in result:
        output.append(Resource(r["ID"], r["NAME"], r["ENABLED"], r["RESOURCE_ID"]))
    return output


class Resource:
    def __init__(self, ingestion_definition_id: str, resource_name: str, is_enabled: bool, resource_id: str):
        self._ingestion_definition_id = ingestion_definition_id
        self._resource_name = resource_name
        self._is_enabled = is_enabled
        self._resource_id = resource_id

    def get_ingestion_definition_id(self):
        return self._ingestion_definition_id

    def get_resource_name(self):
        return self._resource_name

    def is_enabled(self):
        return self._is_enabled

    def get_resource_id(self):
        return self._resource_id


def enable_resource(resource: Resource):
    call_procedure("PUBLIC.ENABLE_RESOURCE", [varchar_argument(resource.get_ingestion_definition_id())])


def disable_resource(resource: Resource):
    call_procedure("PUBLIC.DISABLE_RESOURCE", [varchar_argument(resource.get_ingestion_definition_id())])

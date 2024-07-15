# Copyright (c) 2024 Snowflake Inc.

import string, random

from utils.sql_utils import call_procedure, varchar_argument, variant_argument, variant_list_argument
from snowflake.snowpark.context import get_active_session

session = get_active_session()

def create_resource(resource_name):
    ingestion_config = [{
        "id": "ingestionConfig",
        "ingestionStrategy": "INCREMENTAL",
        # TODO: HINT: scheduleType and scheduleDefinition are currently not supported out of the box, due to globalSchedule being used. However, a custom implementation of the scheduler can use those fields. They need to be provided becuase they are mandatory in the resourceDefinition.
        "scheduleType": "INTERVAL",
        "scheduleDefinition": "60m"
    }]
    # TODO: HINT: resource_id should allow identification of a table, endpoint etc. in the source system. It should be unique.
    resource_id = {
        "resource_name": resource_name,
    }
    id = f"{resource_name}_{random_suffix()}"

    # TODO: if you specified some additional resource parameters then you need to put them inside resource metadata:
    # resource_metadata = {
    #     "some_additional_parameter": some_additional_parameter
    # }

    return call_procedure("PUBLIC.CREATE_RESOURCE",
                          [
                              varchar_argument(resource_name),
                              variant_argument(resource_id),
                              variant_list_argument(ingestion_config),
                              varchar_argument(id),
                              "true"
                              # variant_argument(resource_metadata)
                          ])


def fetch_resources_raw():
    # TODO: To modify the information shown about each resource in the table this query needs to be modified
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


def random_suffix():
    suffix = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(9))
    return suffix


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

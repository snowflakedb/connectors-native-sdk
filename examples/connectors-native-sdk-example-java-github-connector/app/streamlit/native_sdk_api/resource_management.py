# Copyright (c) 2024 Snowflake Inc.

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
                              varchar_argument(id),
                              "true"
                          ])


def fetch_resources():
    return session.sql(
        """
       SELECT
         resource_id:organisation_name::string AS organisation,
         resource_id:repository_name::string AS repository,
         IS_ENABLED
       FROM PUBLIC.INGESTION_DEFINITIONS
       """
    )

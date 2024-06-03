# Copyright (c) 2024 Snowflake Inc.

from snowflake.snowpark.context import get_active_session
from utils.sql_utils import call_procedure, varchar_argument


session = get_active_session()


def fetch_prerequisites():
    result = session.table("PUBLIC.PREREQUISITES").collect()
    output = []
    # TODO: prerequisite can contain some more fields, for more info check the documentation https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/prerequisites_reference
    for r in result:
        output.append(Prerequisite(r["ID"], r["TITLE"], r["DESCRIPTION"], r["DOCUMENTATION_URL"], r["IS_COMPLETED"]))
    return output


def mark_all_prerequisites_as_done():
    return call_procedure("PUBLIC.MARK_ALL_PREREQUISITES_AS_DONE")


def update_prerequisite(prerequisite_id: str, is_completed: str):
    return call_procedure(
        "PUBLIC.UPDATE_PREREQUISITE",
        [
            varchar_argument(prerequisite_id),
            str(is_completed)
        ]
    )


def complete_prerequisite_step():
    return call_procedure("PUBLIC.COMPLETE_PREREQUISITES_STEP")


class Prerequisite:
    # TODO: In case some additional fields were extracted from the prerequisites table, then they need to be mapped here.
    def __init__(self, prerequisite_id: str, title: str, description: str, documentation_url: str, is_completed: bool):
        self._prerequisite_id = prerequisite_id
        self._title = title
        self._description = description
        self._documentation_url = documentation_url
        self._is_completed = is_completed

    def get_prerequisite_id(self):
        return self._prerequisite_id

    def get_title(self):
        return self._title

    def get_description(self):
        return self._description

    def get_documentation_url(self):
        return self._documentation_url

    def get_is_completed(self):
        return self._is_completed

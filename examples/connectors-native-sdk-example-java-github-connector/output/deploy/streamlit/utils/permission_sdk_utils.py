# Copyright (c) 2024 Snowflake Inc.

import json
import snowflake.permissions as permissions
from snowflake.snowpark.context import get_active_session

REQUIRED_PRIVILEGES = ["CREATE DATABASE", "EXECUTE TASK"]
WAREHOUSE_REF = "WAREHOUSE_REFERENCE"
GITHUB_EAI_REF = "EAI_REFERENCE"
GITHUB_SECRET_REF = "SECRET_REFERENCE"


def get_ref(name):
    return get_active_session().sql(f"SELECT SYSTEM$GET_REFERENCED_OBJECT_ID_HASH('{name}')").collect()
    #return json.loads(refs[0])


def get_held_account_privileges():
    return permissions.get_held_account_privileges(REQUIRED_PRIVILEGES)


def get_missing_privileges():
    return permissions.get_missing_account_privileges(REQUIRED_PRIVILEGES)


def request_required_privileges():
    permissions.request_account_privileges(get_missing_privileges())


def get_warehouse_ref():
    return permissions.get_reference_associations(WAREHOUSE_REF)


def request_warehouse_ref():
    permissions.request_reference(WAREHOUSE_REF)


def get_github_eai_ref():
    return permissions.get_reference_associations(GITHUB_EAI_REF)


def request_github_eai_ref():
    permissions.request_reference(GITHUB_EAI_REF)


def get_github_secret_ref():
    return permissions.get_reference_associations(GITHUB_SECRET_REF)

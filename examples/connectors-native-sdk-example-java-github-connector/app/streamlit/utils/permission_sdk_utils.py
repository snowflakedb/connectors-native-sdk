# Copyright (c) 2024 Snowflake Inc.

import snowflake.permissions as permissions


REQUIRED_PRIVILEGES = ["CREATE DATABASE", "EXECUTE TASK"]
WAREHOUSE_REF = "WAREHOUSE_REFERENCE"


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

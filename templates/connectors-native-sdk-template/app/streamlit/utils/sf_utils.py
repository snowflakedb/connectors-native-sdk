# Copyright (c) 2024 Snowflake Inc.

import re


__NON_QUOTED_RAW_PATTERN = "[a-zA-Z_][\\w$]*"
__QUOTED_RAW_PATTERN = "\"([^\"]|\"\")+\""
__IDENTIFIER_RAW_PATTERN = f"({__NON_QUOTED_RAW_PATTERN})|({__QUOTED_RAW_PATTERN})"
__IDENTIFIER_PATTERN = re.compile(__IDENTIFIER_RAW_PATTERN)
__OBJECT_NAME_PATTERN = re.compile(
    f"^(({__IDENTIFIER_RAW_PATTERN}\\.){0,2}|({__IDENTIFIER_RAW_PATTERN}\\.\\.))"
    f"{__IDENTIFIER_RAW_PATTERN}$"
)
__FULLY_QUALIFIED_OBJECT_NAME_PATTERN = re.compile(
    f"^{__IDENTIFIER_RAW_PATTERN}\\.{__IDENTIFIER_RAW_PATTERN}{0,1}"
    f"\\.{__IDENTIFIER_RAW_PATTERN}$"
)


def validate_identifier(identifier: str):
    return __IDENTIFIER_PATTERN.match(identifier) is not None


def validate_object_name(name: str):
    return __OBJECT_NAME_PATTERN.match(name) is not None


def validate_fully_qualified_object_name(name: str):
    return __FULLY_QUALIFIED_OBJECT_NAME_PATTERN.match(name) is not None


def escape_identifier(identifier: str):
    return identifier.replace("'", "\\'").replace("\"", "\\\"")

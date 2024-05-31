# Copyright (c) 2024 Snowflake Inc.

import json


class ConnectorResponse:
    def __init__(self, response_as_json: str = "{}"):
        response = json.loads(response_as_json)

        self._response_code: str = response.get("response_code")
        self._message: str = response.get("message")

    def is_ok(self):
        return self._response_code == "OK"

    def get_response_code(self):
        return self._response_code

    def get_message(self):
        return self._message

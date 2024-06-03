-- Copyright (c) 2024 Snowflake Inc.

CREATE OR REPLACE PROCEDURE PUBLIC.CREATE_RESOURCE(
       name VARCHAR,
       resource_id VARIANT,
       ingestion_configurations VARIANT,
       id VARCHAR DEFAULT NULL,
       enabled BOOLEAN DEFAULT FALSE,
       resource_metadata VARIANT DEFAULT NULL)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.application.ingestion.CreateResourceHandler.createResource';

GRANT USAGE ON PROCEDURE PUBLIC.CREATE_RESOURCE(VARCHAR, VARIANT, VARIANT, VARCHAR, BOOLEAN, VARIANT) TO APPLICATION ROLE ADMIN;

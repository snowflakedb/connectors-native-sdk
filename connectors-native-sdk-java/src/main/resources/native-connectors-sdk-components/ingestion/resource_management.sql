-- Copyright (c) 2024 Snowflake Inc.

-- CREATE
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
    HANDLER = 'com.snowflake.connectors.application.ingestion.create.CreateResourceHandler.createResource';

GRANT USAGE ON PROCEDURE PUBLIC.CREATE_RESOURCE(VARCHAR, VARIANT, VARIANT, VARCHAR, BOOLEAN, VARIANT) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.CREATE_RESOURCE_VALIDATE(resource VARIANT)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.PRE_CREATE_RESOURCE(resource VARIANT)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.POST_CREATE_RESOURCE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

-- DISABLE
CREATE OR REPLACE PROCEDURE PUBLIC.DISABLE_RESOURCE(
       resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.application.ingestion.disable.DisableResourceHandler.disableResource';

GRANT USAGE ON PROCEDURE PUBLIC.DISABLE_RESOURCE(VARCHAR) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.PRE_DISABLE_RESOURCE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.POST_DISABLE_RESOURCE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

-- ENABLE
CREATE OR REPLACE PROCEDURE PUBLIC.ENABLE_RESOURCE(
       resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.application.ingestion.enable.EnableResourceHandler.enableResource';

GRANT USAGE ON PROCEDURE PUBLIC.ENABLE_RESOURCE(VARCHAR) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.ENABLE_RESOURCE_VALIDATE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.PRE_ENABLE_RESOURCE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.POST_ENABLE_RESOURCE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

-- UPDATE
CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_RESOURCE(
        resource_ingestion_definition_id VARCHAR,
        ingestion_configurations VARIANT
    )
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.application.ingestion.update.UpdateResourceHandler.updateResource';
GRANT USAGE ON PROCEDURE PUBLIC.UPDATE_RESOURCE(VARCHAR, VARIANT) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_RESOURCE_VALIDATE(
        resource_ingestion_definition_id VARCHAR,
        ingestion_configurations VARIANT
    )
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.PRE_UPDATE_RESOURCE(
        resource_ingestion_definition_id VARCHAR,
        ingestion_configurations VARIANT
    )
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.POST_UPDATE_RESOURCE(
        resource_ingestion_definition_id VARCHAR,
        ingestion_configurations VARIANT
    )
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

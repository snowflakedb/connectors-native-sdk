-- Copyright (c) 2024 Snowflake Inc.

CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_WAREHOUSE(warehouse_name STRING)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('@snowpark@')
    IMPORTS = ('@java_jar@')
    HANDLER = 'com.snowflake.connectors.application.configuration.warehouse.UpdateWarehouseHandler.updateWarehouse';
GRANT USAGE ON PROCEDURE PUBLIC.UPDATE_WAREHOUSE(STRING) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_WAREHOUSE_INTERNAL(warehouse_name STRING)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

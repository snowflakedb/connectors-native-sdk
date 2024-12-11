-- Copyright (c) 2024 Snowflake Inc.

CREATE TABLE IF NOT EXISTS STATE.APP_CONFIG (
    KEY STRING NOT NULL,
    VALUE VARIANT NOT NULL,
    UPDATED_AT TIMESTAMP_NTZ NOT NULL
);

CREATE OR REPLACE VIEW PUBLIC.CONNECTOR_CONFIGURATION AS
    SELECT
        config.key as "CONFIG_GROUP",
        flat_map.key as "CONFIG_KEY",
        flat_map.value::VARCHAR as "VALUE",
        updated_at as "UPDATED_AT"
    FROM STATE.APP_CONFIG config,
         LATERAL FLATTEN(input => config.value) flat_map
    WHERE IS_OBJECT(config.value) = TRUE
    UNION ALL
    SELECT config.key  as "CONFIG_GROUP",
           config.key as "CONFIG_KEY",
           config.value as "VALUE",
           config.updated_at as "UPDATED_AT"
    FROM STATE.APP_CONFIG config
    WHERE IS_OBJECT(config.value) = FALSE;
GRANT SELECT ON VIEW PUBLIC.CONNECTOR_CONFIGURATION TO APPLICATION ROLE ADMIN;
GRANT SELECT ON VIEW PUBLIC.CONNECTOR_CONFIGURATION TO APPLICATION ROLE VIEWER;

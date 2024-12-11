-- Copyright (c) 2024 Snowflake Inc.

CREATE TABLE IF NOT EXISTS STATE.INGESTION_PROCESS(
    id STRING NOT NULL,
    resource_ingestion_definition_id STRING NOT NULL,
    ingestion_configuration_id STRING NOT NULL,
    type STRING NOT NULL,
    status STRING NOT NULL,
    created_at TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),
    updated_at TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),
    finished_at TIMESTAMP_NTZ,
    metadata VARIANT
);

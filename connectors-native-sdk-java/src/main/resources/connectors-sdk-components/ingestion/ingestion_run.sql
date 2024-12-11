-- Copyright (c) 2024 Snowflake Inc.

CREATE TABLE IF NOT EXISTS STATE.INGESTION_RUN(
    id STRING NOT NULL,
    resource_ingestion_definition_id STRING,
    ingestion_configuration_id STRING,
    ingestion_process_id STRING,
    started_at TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),
    updated_at TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),
    completed_at TIMESTAMP_NTZ,
    status STRING NOT NULL,
    ingested_rows INTEGER NOT NULL DEFAULT 0,
    metadata VARIANT
    );

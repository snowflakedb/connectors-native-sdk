-- Copyright (c) 2024 Snowflake Inc.

CREATE OR REPLACE VIEW PUBLIC.GENERIC_CONNECTOR_STATS AS
    SELECT ir.id AS run_id,
           ir.resource_ingestion_definition_id AS resource_ingestion_definition_id,
           ir.ingestion_configuration_id AS ingestion_configuration_id,
           ir.ingestion_process_id AS ingestion_process_id,
           rid.name AS resource_ingestion_definition_name,
           ir.started_at,
           ir.updated_at,
           ir.completed_at,
           ir.status,
           ir.ingested_rows,
           DATEDIFF(second, ir.started_at, ir.completed_at) AS duration_s,
           div0(ir.ingested_rows, duration_s) AS throughput_rps,
           ir.metadata
        FROM STATE.INGESTION_RUN ir
            JOIN STATE.RESOURCE_INGESTION_DEFINITION rid ON ir.resource_ingestion_definition_id = rid.id
    UNION
    SELECT ir.id AS run_id,
           ip.resource_ingestion_definition_id AS resource_ingestion_definition_id,
           ip.ingestion_configuration_id AS ingestion_configuration_id,
           ir.ingestion_process_id AS ingestion_process_id,
           rid.name AS resource_ingestion_definition_name,
           ir.started_at,
           ir.updated_at,
           ir.completed_at,
           ir.status,
           ir.ingested_rows,
           DATEDIFF(second, ir.started_at, ir.completed_at) AS duration_s,
           div0(ir.ingested_rows, duration_s) AS throughput_rps,
           ir.metadata
        FROM STATE.INGESTION_RUN ir
            JOIN STATE.INGESTION_PROCESS ip ON ir.ingestion_process_id = ip.id
            JOIN STATE.RESOURCE_INGESTION_DEFINITION rid ON ip.resource_ingestion_definition_id = rid.id;

CREATE OR REPLACE VIEW PUBLIC.CONNECTOR_STATS AS
    SELECT * FROM PUBLIC.GENERIC_CONNECTOR_STATS;
GRANT SELECT ON VIEW PUBLIC.CONNECTOR_STATS TO APPLICATION ROLE ADMIN;
GRANT SELECT ON VIEW PUBLIC.CONNECTOR_STATS TO APPLICATION ROLE VIEWER;

CREATE OR REPLACE VIEW PUBLIC.AGGREGATED_CONNECTOR_STATS AS
    SELECT date_trunc('HOUR', started_at) AS run_date,
       sum(ingested_rows) AS updated_rows
        FROM PUBLIC.GENERIC_CONNECTOR_STATS
        GROUP BY date_trunc('HOUR', started_at);
GRANT SELECT ON VIEW PUBLIC.AGGREGATED_CONNECTOR_STATS TO APPLICATION ROLE ADMIN;
GRANT SELECT ON VIEW PUBLIC.AGGREGATED_CONNECTOR_STATS TO APPLICATION ROLE VIEWER;

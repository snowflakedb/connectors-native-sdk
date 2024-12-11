-- Copyright (c) 2024 Snowflake Inc.

CREATE OR REPLACE VIEW PUBLIC.SYNC_STATUS AS
    WITH connector_status AS (SELECT value:status::string AS cstatus FROM state.app_state WHERE key = 'connector_status'),
         last_synced_at AS (SELECT MAX(completed_at) AS last_date FROM generic_connector_stats WHERE status = 'COMPLETED'),
         in_progress AS (SELECT IFF(COUNT(*) > 0, TRUE, FALSE) AS in_progress FROM generic_connector_stats WHERE status = 'IN_PROGRESS'),
         enabled_resources AS (SELECT IFF(COUNT(*) > 0, TRUE, FALSE) AS any_enabled FROM ingestion_definitions WHERE is_enabled = TRUE)
    SELECT CASE
               WHEN cstatus in ('PAUSED', 'STARTING') THEN 'PAUSED'
               WHEN last_date IS NOT NULL THEN 'LAST_SYNCED'
               WHEN (last_date IS NULL AND in_progress) OR any_enabled THEN 'SYNCING_DATA'
               WHEN last_date IS NULL AND NOT in_progress AND NOT any_enabled THEN 'NOT_SYNCING'
           END AS status,
           CASE
               WHEN status = 'LAST_SYNCED' THEN last_date
               ELSE null
           END AS last_synced_at
       FROM connector_status, last_synced_at, in_progress, enabled_resources;

GRANT SELECT ON VIEW PUBLIC.SYNC_STATUS TO APPLICATION ROLE ADMIN;
GRANT SELECT ON VIEW PUBLIC.SYNC_STATUS TO APPLICATION ROLE VIEWER;

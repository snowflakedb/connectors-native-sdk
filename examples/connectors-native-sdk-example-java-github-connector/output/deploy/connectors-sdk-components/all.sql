-- Copyright (c) 2024 Snowflake Inc.

-- Core connector objects
EXECUTE IMMEDIATE FROM 'core.sql';

-- Connector configuration prerequisites
EXECUTE IMMEDIATE FROM 'prerequisites.sql';

-- Connector configuration flow
EXECUTE IMMEDIATE FROM 'configuration/app_config.sql';
EXECUTE IMMEDIATE FROM 'configuration/connector_configuration.sql';
EXECUTE IMMEDIATE FROM 'configuration/connection_configuration.sql';
EXECUTE IMMEDIATE FROM 'configuration/finalize_configuration.sql';
EXECUTE IMMEDIATE FROM 'configuration/update_connection_configuration.sql';
EXECUTE IMMEDIATE FROM 'configuration/update_warehouse.sql';
EXECUTE IMMEDIATE FROM 'configuration/reset_configuration.sql';

-- Connector lifecycle
EXECUTE IMMEDIATE FROM 'lifecycle/pause.sql';
EXECUTE IMMEDIATE FROM 'lifecycle/resume.sql';

-- Ingestion
EXECUTE IMMEDIATE FROM 'ingestion/resource_ingestion_definition.sql';
EXECUTE IMMEDIATE FROM 'ingestion/ingestion_process.sql';
EXECUTE IMMEDIATE FROM 'ingestion/ingestion_definitions_view.sql';
EXECUTE IMMEDIATE FROM 'ingestion/ingestion_run.sql';
EXECUTE IMMEDIATE FROM 'ingestion/resource_management.sql';

-- Observability
EXECUTE IMMEDIATE FROM 'observability/connector_stats.sql';
EXECUTE IMMEDIATE FROM 'observability/sync_status.sql';

-- Scheduler
EXECUTE IMMEDIATE FROM 'scheduler/scheduler.sql';

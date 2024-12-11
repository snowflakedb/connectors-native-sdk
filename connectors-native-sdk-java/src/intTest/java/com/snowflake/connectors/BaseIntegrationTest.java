/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import org.junit.jupiter.api.BeforeAll;

public class BaseIntegrationTest extends BaseTest {

  @BeforeAll
  void baseIntegrationBeforeAll() {
    prepareDatabaseObjects();
  }

  private void prepareDatabaseObjects() {
    session.sql("CREATE SCHEMA STATE").collect();
    session
        .sql(
            "CREATE TABLE STATE.PREREQUISITES (ID STRING NOT NULL, TITLE VARCHAR NOT NULL,"
                + " DESCRIPTION VARCHAR NOT NULL, LEARNMORE_URL VARCHAR, DOCUMENTATION_URL VARCHAR,"
                + " GUIDE_URL VARCHAR, CUSTOM_PROPERTIES VARIANT, IS_COMPLETED BOOLEAN DEFAULT"
                + " FALSE, POSITION INTEGER NOT NULL)")
        .collect();
    session
        .sql("CREATE TABLE STATE.APP_CONFIG(KEY STRING, VALUE VARIANT, UPDATED_AT TIMESTAMP_NTZ)")
        .collect();
    session
        .sql(
            "CREATE TABLE STATE.RESOURCE_INGESTION_STATE(TIMESTAMP TIMESTAMP_NTZ DEFAULT SYSDATE(),"
                + " KEY STRING, VALUE VARIANT)")
        .collect();
    session
        .sql(
            "CREATE TABLE STATE.RESOURCE_INGESTION_DEFINITION_OLD(KEY STRING, VALUE VARIANT,"
                + " UPDATED_AT TIMESTAMP_NTZ)")
        .collect();
    session
        .sql(
            "CREATE TABLE STATE.APP_STATE(KEY STRING NOT NULL, VALUE VARIANT NOT NULL, UPDATED_AT"
                + " TIMESTAMP_NTZ)")
        .collect();
    session
        .sql(
            "CREATE TABLE STATE.RESOURCE_INGESTION_DEFINITION(ID STRING NOT NULL, NAME STRING,"
                + " ENABLED BOOLEAN, PARENT_ID STRING, RESOURCE_ID VARIANT, RESOURCE_METADATA"
                + " VARIANT, INGESTION_CONFIGURATION VARIANT, UPDATED_AT TIMESTAMP_NTZ NOT NULL)")
        .collect();
    session
        .sql(
            "CREATE TABLE STATE.NOTIFICATIONS_STATE(TIMESTAMP TIMESTAMP_NTZ DEFAULT SYSDATE(), KEY"
                + " STRING, VALUE VARIANT)")
        .collect();
    session
        .sql(
            "CREATE TABLE IF NOT EXISTS STATE.INGESTION_PROCESS("
                + "id STRING NOT NULL,"
                + "resource_ingestion_definition_id STRING NOT NULL,"
                + "ingestion_configuration_id STRING NOT NULL,"
                + "type STRING NOT NULL,"
                + "status STRING NOT NULL,"
                + "created_at TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),"
                + "updated_at TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),"
                + "finished_at TIMESTAMP_NTZ,"
                + "metadata VARIANT"
                + ")")
        .collect();
    session
        .sql(
            "CREATE TABLE IF NOT EXISTS STATE.INGESTION_RUN("
                + "id STRING NOT NULL,"
                + "resource_ingestion_definition_id STRING,"
                + "ingestion_configuration_id STRING,"
                + "ingestion_process_id STRING,"
                + "started_at TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),"
                + "updated_at TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),"
                + "completed_at TIMESTAMP_NTZ,"
                + "status STRING NOT NULL,"
                + "ingested_rows INTEGER NOT NULL DEFAULT 0"
                + ")")
        .collect();
    session
        .sql(
            "CREATE OR REPLACE VIEW PUBLIC.APP_PROPERTIES AS "
                + "SELECT 'display_name' AS KEY, 'Connectors Java SDK'::VARIANT AS VALUE;")
        .collect();

    session
        .sql(
            "CREATE TABLE IF NOT EXISTS STATE.CONNECTOR_ERRORS_LOG (CODE STRING NOT NULL, MESSAGE"
                + " STRING NOT NULL, CONTEXT VARIANT NOT NULL, CREATED_AT TIMESTAMP_NTZ NOT NULL)")
        .collect();

    session
        .sql(
            "CREATE OR REPLACE PROCEDURE PUBLIC.CONFIGURE_CONNECTOR_INTERNAL(config VARIANT) "
                + "RETURNS VARIANT LANGUAGE SQL AS BEGIN RETURN config; END;")
        .collect();
  }
}

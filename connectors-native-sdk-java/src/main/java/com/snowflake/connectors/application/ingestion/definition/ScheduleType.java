/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

/** Type of the ingestion schedule. */
public enum ScheduleType {

  /** Constant interval schedule, mathing the regex pattern of {@code [1-9][0-9]*[dhm]}. */
  INTERVAL,

  /** Cron interval schedule, mathing the standard cron pattern. */
  CRON,

  /**
   * The ingestion schedule will be derived from the {@link
   * com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey#GLOBAL_SCHEDULE
   * global connector schedule}.
   */
  GLOBAL
}

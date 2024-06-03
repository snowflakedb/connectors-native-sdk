/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

/** Resource ingestion strategy. */
public enum IngestionStrategy {

  /** Create snapshots of the ingested data. */
  SNAPSHOT,

  /** Increment previously ingested data with the new data. */
  INCREMENTAL
}

package com.snowflake.connectors.common.table;

/** Drop mode for object dropping queries. */
public enum DropMode {

  /** Adds {@code IF EXISTS} option to the query. */
  IF_EXISTS,

  /**
   * Does not add anything to the query, the query will fail if the dropped object does not exist.
   */
  MUST_EXIST_ON_DROP
}

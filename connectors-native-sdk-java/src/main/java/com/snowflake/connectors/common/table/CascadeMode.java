package com.snowflake.connectors.common.table;

/**
 * Cascade mode for object dropping queries.
 *
 * @see <a
 *     href="https://docs.snowflake.com/en/sql-reference/constraints-drop#dropping-tables-schemas-databases">Dropping
 *     Tables/Schemas/Databases</a>
 */
public enum CascadeMode {

  /** Adds {@code CASCADE} option to the query. */
  CASCADE,

  /** Adds {@code RESTRICT} option to the query. */
  RESTRICT
}

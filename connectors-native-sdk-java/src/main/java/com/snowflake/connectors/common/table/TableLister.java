/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.connectors.common.object.SchemaName;
import java.util.List;

/** Lists tables in the schema */
public interface TableLister {

  /**
   * Returns list of tables for a given schema
   *
   * @param schema
   * @return list of tables for a given schema
   */
  List<TableProperties> showTables(SchemaName schema);

  /**
   * Returns list of tables for a given schema with additional filter expression.
   *
   * @param schema
   * @param like filter expression, eg. "MYTABLE", "%TAB%"
   * @return list of tables
   */
  List<TableProperties> showTables(SchemaName schema, String like);
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util;

import com.snowflake.snowpark_java.Row;

public class RowUtil {

  public static Row row(Object... values) {
    return new Row(values);
  }
}

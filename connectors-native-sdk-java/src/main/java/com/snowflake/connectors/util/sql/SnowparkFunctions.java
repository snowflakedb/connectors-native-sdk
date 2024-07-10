/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Functions;
import java.time.Instant;

/**
 * Class containing overloaded Snowpark's functions which shouldn't be used in Connector's code.
 *
 * @see Functions
 */
public class SnowparkFunctions {

  /**
   * It shouldn't be used because it uses wrong timestamp mapping which causes saving a timestamp in
   * a wrong time zone. Use {@link #lit(Object)} with a Timestamp argument mapped by {@link
   * TimestampUtil#toTimestamp(Instant)}
   *
   * @param instant instant
   * @return the result column
   */
  @Deprecated
  public static Column lit(Instant instant) {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a Column expression for a literal value. Delegates to {@link Functions#lit(Object)}
   * This function should be used instead of {@link Functions#lit(Object)} in order to avoid passing
   * {@link Instant} argument which causes wrong timestamp mapping.
   *
   * @see #lit(Instant)
   * @param literal the literal value
   * @return the result column
   */
  public static Column lit(Object literal) {
    return Functions.lit(literal);
  }
}

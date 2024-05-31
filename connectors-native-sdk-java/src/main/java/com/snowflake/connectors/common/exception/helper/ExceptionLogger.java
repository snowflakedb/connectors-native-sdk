/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.helper;

/**
 * A generic exception logging implementation.
 *
 * @param <E> type of the logged exception
 */
public interface ExceptionLogger<E> {

  /**
   * Logs the provided exception.
   *
   * @param exception exception to log
   */
  void log(E exception);
}

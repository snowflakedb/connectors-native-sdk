/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.helper;

import com.snowflake.connectors.common.exception.ConnectorException;

/**
 * A generic mapper used for changing any exception into an instance of {@link ConnectorException}.
 *
 * @param <E> type of the mapped exception
 */
public interface ExceptionMapper<E> {

  /**
   * Maps the provided exception into an instance of {@link ConnectorException}.
   *
   * @param exception exception to map
   * @return mapped exception
   */
  ConnectorException map(E exception);
}

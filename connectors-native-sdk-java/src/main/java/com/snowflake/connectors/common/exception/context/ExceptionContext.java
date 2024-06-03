/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.context;

import java.util.Map;

/**
 * A custom context for the {@link com.snowflake.connectors.common.exception.ConnectorException
 * ConnectorException}.
 *
 * <p>If you do not want to use provide a custom exception context - use {@link
 * EmptyExceptionContext#INSTANCE EmptyExceptionContext}.
 *
 * <p>If you want to use a mutable exception context - use {@link MutableExceptionContext}.
 */
public interface ExceptionContext {

  /**
   * Returns properties of this context as a map.
   *
   * @return properties of this context as a map
   */
  Map<String, Object> asMap();
}

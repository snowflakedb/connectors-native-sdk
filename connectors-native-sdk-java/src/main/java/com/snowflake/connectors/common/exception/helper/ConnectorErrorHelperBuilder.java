/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.helper;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link ConnectorErrorHelper}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ExceptionMapper}
 *   <li>{@link ExceptionLogger}
 * </ul>
 */
public class ConnectorErrorHelperBuilder {

  private ExceptionMapper<Exception> unknownExceptionMapper;
  private ExceptionLogger<ConnectorException> connectorExceptionLogger;

  /**
   * Creates a new {@link ConnectorErrorHelperBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ExceptionMapper}
   *   <li>a default implementation of {@link ExceptionLogger}
   * </ul>
   *
   * @param scopeName name for the scope of the event table log entry
   * @throws NullPointerException if the provided session object is null
   */
  ConnectorErrorHelperBuilder(Session ignored, String scopeName) {
    this.unknownExceptionMapper = new DefaultUnknownExceptionMapper();
    this.connectorExceptionLogger = new DefaultConnectorExceptionLogger(scopeName);
  }

  /**
   * Sets the exception mapper used for mapping unknown (other than {@link ConnectorException})
   * exceptions.
   *
   * @param mapper exception mapper
   * @return this builder
   */
  public ConnectorErrorHelperBuilder withUnknownExceptionMapper(ExceptionMapper<Exception> mapper) {
    this.unknownExceptionMapper = mapper;
    return this;
  }

  /**
   * Sets the exception logger used for logging connector exceptions.
   *
   * @param logger exception logger
   * @return this builder
   */
  public ConnectorErrorHelperBuilder withConnectorExceptionLogger(
      ExceptionLogger<ConnectorException> logger) {
    this.connectorExceptionLogger = logger;
    return this;
  }

  /**
   * Builds a new error helper instance.
   *
   * @return new error helper instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public ConnectorErrorHelper build() {
    requireNonNull(unknownExceptionMapper);
    requireNonNull(connectorExceptionLogger);

    return new DefaultConnectorErrorHelper(unknownExceptionMapper, connectorExceptionLogger);
  }
}

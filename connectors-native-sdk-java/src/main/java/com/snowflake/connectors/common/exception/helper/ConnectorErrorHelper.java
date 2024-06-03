/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.helper;

import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import java.util.function.Supplier;

/**
 * Provides utility methods for catching, logging and mapping exceptions within the connector. The
 * purpose of this helper is to abstract away repetitive try-catch blocks and provide a central
 * point for exception logging and mapping.
 */
public interface ConnectorErrorHelper {

  /**
   * Wraps the provided action in a try/catch block, and if any exception is caught - logs it and
   * maps it to a {@link ConnectorException} instance.
   *
   * @param action action to execute
   * @param <T> type of the result of the action
   * @return the result of the action
   * @throws ConnectorException caught, logged and rethrown exception
   */
  <T> T withExceptionLogging(Supplier<T> action) throws ConnectorException;

  /**
   * Wraps the provided action in a try/catch block, and if any exception is caught - returns it as
   * an instance of {@link ConnectorResponse}, without rethrowing the exception.
   *
   * @param action any action returning a {@link ConnectorResponse}
   * @return Response returned by the supplier or caught and wrapped exception
   */
  ConnectorResponse withExceptionWrapping(Supplier<ConnectorResponse> action);

  /**
   * Wraps the provided action in a try/catch block, and if any exception is caught - logs it and
   * returns it as an instance of {@link ConnectorResponse}, without rethrowing the exception.
   *
   * @param action any action returning a {@link ConnectorResponse}
   * @return Response returned by the supplier or caught, logged and wrapped exception
   */
  default ConnectorResponse withExceptionLoggingAndWrapping(Supplier<ConnectorResponse> action) {
    return withExceptionWrapping(() -> withExceptionLogging(action));
  }

  /**
   * Returns a new instance of {@link ConnectorErrorHelperBuilder}.
   *
   * @param session Snowpark session object
   * @param scopeName name for the scope of the event table log entry
   * @return a new builder instance
   */
  static ConnectorErrorHelperBuilder builder(Session session, String scopeName) {
    return new ConnectorErrorHelperBuilder(session, scopeName);
  }

  /**
   * Builds a default instance of the {@link ConnectorErrorHelper}.
   *
   * <p>Default implementation of the helper uses:
   *
   * <ul>
   *   <li>a default implementation of {@link ExceptionMapper}
   *   <li>a default implementation of {@link ExceptionLogger}
   * </ul>
   *
   * @param session Snowpark session object
   * @param scopeName name for the scope of the event table log entry
   * @return a new error helper instance
   */
  static ConnectorErrorHelper buildDefault(Session session, String scopeName) {
    return builder(session, scopeName).build();
  }
}

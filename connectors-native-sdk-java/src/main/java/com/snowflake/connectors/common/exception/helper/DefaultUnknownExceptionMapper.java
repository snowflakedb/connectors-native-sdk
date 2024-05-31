/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.helper;

import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.exception.UnknownConnectorException;

/**
 * Default exception mapper, mapping any exception into an instance of {@link
 * UnknownConnectorException}.
 */
class DefaultUnknownExceptionMapper implements ExceptionMapper<Exception> {

  @Override
  public ConnectorException map(Exception exception) {
    return new UnknownConnectorException(exception);
  }
}

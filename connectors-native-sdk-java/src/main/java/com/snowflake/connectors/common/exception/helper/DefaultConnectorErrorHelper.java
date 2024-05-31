/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.helper;

import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.response.ConnectorResponse;
import java.util.function.Supplier;

/** Default implementation of {@link ConnectorErrorHelper}. */
class DefaultConnectorErrorHelper implements ConnectorErrorHelper {

  private final ExceptionMapper<Exception> unknownExceptionMapper;
  private final ExceptionLogger<ConnectorException> connectorExceptionLogger;

  DefaultConnectorErrorHelper(
      ExceptionMapper<Exception> unknownExceptionMapper,
      ExceptionLogger<ConnectorException> connectorExceptionLogger) {
    this.unknownExceptionMapper = unknownExceptionMapper;
    this.connectorExceptionLogger = connectorExceptionLogger;
  }

  @Override
  public <T> T withExceptionLogging(Supplier<T> action) throws ConnectorException {
    try {
      return action.get();
    } catch (ConnectorException exception) {
      connectorExceptionLogger.log(exception);
      throw exception;
    } catch (Exception exception) {
      var mappedException = unknownExceptionMapper.map(exception);
      connectorExceptionLogger.log(mappedException);
      throw mappedException;
    }
  }

  @Override
  public ConnectorResponse withExceptionWrapping(Supplier<ConnectorResponse> action) {
    try {
      return action.get();
    } catch (ConnectorException exception) {
      return exception.getResponse();
    } catch (Exception exception) {
      return unknownExceptionMapper.map(exception).getResponse();
    }
  }
}

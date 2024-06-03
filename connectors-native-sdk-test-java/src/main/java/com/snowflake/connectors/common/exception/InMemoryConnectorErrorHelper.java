/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import java.util.function.Supplier;

/** In memory implementation of {@link ConnectorErrorHelper}. */
public class InMemoryConnectorErrorHelper implements ConnectorErrorHelper {

  public InMemoryConnectorErrorHelper() {}

  @Override
  public <T> T withExceptionLogging(Supplier<T> action) throws ConnectorException {
    return action.get();
  }

  @Override
  public ConnectorResponse withExceptionWrapping(Supplier<ConnectorResponse> action) {
    return action.get();
  }
}

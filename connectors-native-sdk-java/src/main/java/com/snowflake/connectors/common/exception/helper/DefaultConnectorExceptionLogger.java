/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.helper;

import com.snowflake.connectors.common.exception.ConnectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default exception logger, logging any instance of {@link ConnectorException} into the <a
 * href="https://docs.snowflake.com/en/developer-guide/logging-tracing/logging-tracing-overview">
 * event table</a>.
 */
class DefaultConnectorExceptionLogger implements ExceptionLogger<ConnectorException> {

  private final Logger logger;

  DefaultConnectorExceptionLogger(String scopeName) {
    this.logger = LoggerFactory.getLogger(scopeName);
  }

  @Override
  public void log(ConnectorException exception) {
    logger.error(
        "An error with code {} has been encountered: {}",
        exception.getResponse().getResponseCode(),
        exception.getMessage());
  }
}

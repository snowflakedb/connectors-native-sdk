/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/**
 * Exception thrown when enabling of a resource failed. It could happen during ingestion process or
 * resource ingestion definition update.
 */
public class ResourceEnablingException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String ERROR_CODE = "ENABLE_RESOURCE_ERROR";

  /**
   * Creates a new {@link ResourceEnablingException}.
   *
   * @param cause exception cause
   */
  public ResourceEnablingException(Throwable cause) {
    super(
        ERROR_CODE,
        format(
            "Error while enabling resource. Changes were rolled back. Error: %s",
            cause.getMessage()),
        cause);
  }
}

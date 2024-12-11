/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.disable;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/**
 * Exception thrown when disabling of a resource failed. It could happen during ingestion process or
 * resource ingestion definition update.
 */
public class ResourceDisablingException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String ERROR_CODE = "DISABLE_RESOURCE_ERROR";

  /**
   * Creates a new {@link ResourceDisablingException}.
   *
   * @param cause exception cause
   */
  public ResourceDisablingException(Throwable cause) {
    super(
        ERROR_CODE,
        format(
            "Error while disabling resource. Changes were rolled back. Error: %s",
            cause.getMessage()),
        cause);
  }
}

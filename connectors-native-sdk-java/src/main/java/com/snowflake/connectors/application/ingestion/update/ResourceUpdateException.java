/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.update;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/**
 * Exception thrown when updating of a resource failed. It could happen during ingestion process or
 * resource ingestion definition update.
 */
public class ResourceUpdateException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String ERROR_CODE = "UPDATE_RESOURCE_ERROR";

  /**
   * Creates a new instance of the {@link ResourceUpdateException}
   *
   * @param cause the root cause of this exception
   */
  public ResourceUpdateException(Throwable cause) {
    super(
        ERROR_CODE,
        format(
            "Error occurred while updating resource. Changes were rolled back. Error: %s",
            cause.getMessage()),
        cause);
  }
}

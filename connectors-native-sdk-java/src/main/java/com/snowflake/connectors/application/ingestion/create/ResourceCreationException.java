package com.snowflake.connectors.application.ingestion.create;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/**
 * Exception thrown when creation of a resource failed. It could happen during ingestion process or
 * resource ingestion definition creation.
 */
public class ResourceCreationException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String ERROR_CODE = "CREATE_RESOURCE_ERROR";

  /**
   * Creates a new {@link ResourceCreationException}.
   *
   * @param cause exception cause
   */
  public ResourceCreationException(Throwable cause) {
    super(
        ERROR_CODE,
        format(
            "Error while creating resource. Changes were rolled back. Error: %s",
            cause.getMessage()),
        cause);
  }
}

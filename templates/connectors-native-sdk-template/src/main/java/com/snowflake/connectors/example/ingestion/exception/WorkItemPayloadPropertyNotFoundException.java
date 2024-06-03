/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.exception;

/** Exception thrown when the provided property does not exist in the work item payload. */
public class WorkItemPayloadPropertyNotFoundException extends RuntimeException {

  private static final String ERROR_MESSAGE = "Property [%s] not found in the WorkItem payload";

  public WorkItemPayloadPropertyNotFoundException(String property) {
    super(String.format(ERROR_MESSAGE, property));
  }
}

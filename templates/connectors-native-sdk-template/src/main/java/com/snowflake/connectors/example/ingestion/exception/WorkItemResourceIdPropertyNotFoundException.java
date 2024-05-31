/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.exception;

/** Exception thrown when the provided property does not exist in the work item resource id. */
public class WorkItemResourceIdPropertyNotFoundException extends RuntimeException {

  private static final String ERROR_MESSAGE = "Property [%s] not found in the WorkItem resourceId";

  public WorkItemResourceIdPropertyNotFoundException(String property) {
    super(String.format(ERROR_MESSAGE, property));
  }
}

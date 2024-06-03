/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import java.util.List;

/** Exception thrown when validation of resource ingestion definition fails. */
public class ResourceIngestionDefinitionValidationException extends RuntimeException {

  private final List<String> validationErrors;

  /**
   * Creates a new {@link ResourceIngestionDefinitionValidationException}.
   *
   * @param validationErrors validation errors
   */
  public ResourceIngestionDefinitionValidationException(List<String> validationErrors) {
    super(
        String.format(
            "Resource validation failed with %s errors: %s",
            validationErrors.size(), String.join(" ", validationErrors)));
    this.validationErrors = validationErrors;
  }

  /**
   * Returns a list of encountered validation errors.
   *
   * @return list of encountered validation errors.
   */
  public List<String> getValidationErrors() {
    return validationErrors;
  }
}

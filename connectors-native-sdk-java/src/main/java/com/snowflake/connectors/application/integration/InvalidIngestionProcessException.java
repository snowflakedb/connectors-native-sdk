/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.integration;

/** Exception thrown when an invalid ingestion process is encountered. */
public class InvalidIngestionProcessException extends RuntimeException {

  /**
   * Creates a new {@link InvalidIngestionProcessException}.
   *
   * @param message exception message
   */
  public InvalidIngestionProcessException(String message) {
    super(message);
  }

  /**
   * Creates a new {@link InvalidIngestionProcessException} for a non-existent ingestion process.
   *
   * @param processId ingestion process id
   */
  static InvalidIngestionProcessException processDoesNotExist(String processId) {
    return new InvalidIngestionProcessException(
        String.format("Ingestion Process with id '%s' does not exist", processId));
  }

  /**
   * Creates a new {@link InvalidIngestionProcessException} for a non-existent resource ingestion
   * definition.
   *
   * @param processId ingestion process id
   * @param resourceIngestionDefinitionId resource ingestion definition id
   */
  static InvalidIngestionProcessException resourceIngestionDefinitionDoesNotExist(
      String processId, String resourceIngestionDefinitionId) {
    return new InvalidIngestionProcessException(
        String.format(
            "Invalid Ingestion Process id='%s': Resource Ingestion Definition with id '%s' does not"
                + " exist",
            processId, resourceIngestionDefinitionId));
  }

  /**
   * Creates a new {@link InvalidIngestionProcessException} for a non-existent ingestion
   * configuration.
   *
   * @param processId ingestion process id
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration id
   */
  static InvalidIngestionProcessException ingestionConfigurationDoesNotExist(
      String processId, String resourceIngestionDefinitionId, String ingestionConfigurationId) {
    return new InvalidIngestionProcessException(
        String.format(
            "Invalid Ingestion Process id='%s':Ingestion Configuration with id '%s' is not defined"
                + " for Resource Ingestion Definition with id '%s'",
            processId, ingestionConfigurationId, resourceIngestionDefinitionId));
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.update;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/** Callback which is called at the beginning of updating a resource ingestion configuration */
public interface UpdateResourceValidator {

  /**
   * Callback that is called at the beginning of updating a resource ingestion configuration process
   * intended to initial input validation.
   *
   * <p>This callback can be used in order to add custom logic which validates whether the resource
   * ingestion configuration is allowed to be overridden. If it returns a response with code
   * different from OK, the update logic will not be executed and UPDATE_RESOURCE procedure will
   * return the same error response.
   *
   * @param resourceIngestionDefinitionId id of Resource Ingestion Definition
   * @param ingestionConfigurations updated Resource Ingestion Configuration
   * @return a response with the code {@code OK} if the validation was successful and the resource
   *     can be updated, otherwise a response with an error code and an error message
   */
  ConnectorResponse validate(
      String resourceIngestionDefinitionId,
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations);
}

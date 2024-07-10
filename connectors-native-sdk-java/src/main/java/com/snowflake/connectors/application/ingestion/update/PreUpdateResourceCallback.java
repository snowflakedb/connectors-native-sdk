/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.update;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/** Callback that is called before executing the main logic of resource updating procedure */
public interface PreUpdateResourceCallback {

  /**
   * Callback which is called before executing the main logic of resource updating procedure and
   * after validation step. If it returns a response with code different from OK, the update logic
   * will not be executed and UPDATE_RESOURCE procedure will return this error.
   *
   * @param resourceIngestionDefinitionId id of Resource Ingestion Definition
   * @param ingestionConfigurations updated Resource Ingestion Configuration
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(
      String resourceIngestionDefinitionId,
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations);
}

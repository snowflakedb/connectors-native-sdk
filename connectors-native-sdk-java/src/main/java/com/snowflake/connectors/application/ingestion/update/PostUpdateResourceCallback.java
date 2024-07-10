/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.update;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/** Callback which is called at the end of updating a resource */
public interface PostUpdateResourceCallback {

  /**
   * Callback which is called at the end of updating a resource. If it returns a response with code
   * different from OK, the UPDATE_RESOURCE procedure will return this error, but the update logic
   * will not be rolled back.
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

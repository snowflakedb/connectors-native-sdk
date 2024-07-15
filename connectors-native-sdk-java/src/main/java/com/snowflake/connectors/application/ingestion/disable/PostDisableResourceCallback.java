/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.disable;

import com.snowflake.connectors.common.response.ConnectorResponse;

/** Callback which is called at the end of disabling a resource */
public interface PostDisableResourceCallback {

  /**
   * Callback which is called at the end of disabling a resource.
   *
   * @param resourceIngestionDefinitionId id of a resource ingestion definition which is planned to
   *     be disabled
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(String resourceIngestionDefinitionId);
}

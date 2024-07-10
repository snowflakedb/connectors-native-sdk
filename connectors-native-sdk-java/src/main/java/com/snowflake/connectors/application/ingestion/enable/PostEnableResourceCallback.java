/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

import com.snowflake.connectors.common.response.ConnectorResponse;

/** Callback which is called at the end of enabling a resource */
public interface PostEnableResourceCallback {

  /**
   * Callback which is called at the end of enabling a resource. If it returns a response with code
   * different from OK, the ENABLE_RESOURCE procedure will return this error, but the enabling logic
   * will not be rolled back.
   *
   * @param resourceIngestionDefinitionId id of a resource ingestion definition which is planned to
   *     be enabled
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(String resourceIngestionDefinitionId);
}

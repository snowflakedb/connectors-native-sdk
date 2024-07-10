/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

import com.snowflake.connectors.common.response.ConnectorResponse;

/** Callback which is called before executing the main logic of enabling a resource */
public interface PreEnableResourceCallback {

  /**
   * Callback which is called before executing the main logic of enabling a resource and after
   * validation (whether a given resource exists and whether the resource is not already enabled).
   * If it returns a response with code different from OK, the enabling logic will not be executed
   * and ENABLE_RESOURCE procedure will return this error.
   *
   * @param resourceIngestionDefinitionId id of a resource ingestion definition which is planned to
   *     be enabled
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(String resourceIngestionDefinitionId);
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.disable;

import com.snowflake.connectors.common.response.ConnectorResponse;

/** Callback which is called at the beginning of disabling a resource */
public interface PreDisableResourceCallback {

  /**
   * Callback which is called at the beginning of disabling a resource. It is called after simple
   * validation (whether a given resource exists and whether the resource is not already disabled).
   * If it returns a response with code different from OK, the disabling logic will not be executed
   * and DISABLE_RESOURCE procedure will return this error.
   *
   * @param resourceIngestionDefinitionId id of a resource ingestion definition which is planned to
   *     be disabled
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(String resourceIngestionDefinitionId);
}

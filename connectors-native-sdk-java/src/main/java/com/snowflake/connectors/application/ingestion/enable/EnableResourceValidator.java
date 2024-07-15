/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

import com.snowflake.connectors.common.response.ConnectorResponse;

/** Callback which is called at the beginning of enabling a resource */
public interface EnableResourceValidator {

  /**
   * Callback which is called at the beginning of enabling a resource but after simple validation
   * (whether a given resource exists and whether the resource is not already enabled). It can be
   * used in order to add custom logic which validates whether the resource can be enabled. If it
   * returns a response with code different from OK, the enabling logic will not be executed and
   * ENABLE_RESOURCE procedure will return this error.
   *
   * @param resourceIngestionDefinitionId id of a resource ingestion definition which is planned to
   *     be enabled
   * @return a response with the code {@code OK} if the execution was successful and the resource
   *     can be enabled, otherwise a response with an error code and an error message
   */
  ConnectorResponse validate(String resourceIngestionDefinitionId);
}

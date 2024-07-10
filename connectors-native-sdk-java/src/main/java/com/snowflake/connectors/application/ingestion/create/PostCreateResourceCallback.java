/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.create;

import com.snowflake.connectors.common.response.ConnectorResponse;

/** Callback which is called at the end of creating a resource */
public interface PostCreateResourceCallback {

  /**
   * Callback which is called at the end of creating a resource. If it returns a response with code
   * different from OK, the CREATE_RESOURCE procedure will return this error, but the creation logic
   * will not be rolled back.
   *
   * @param resourceIngestionDefinitionId id of a resource ingestion definition which has just been
   *     created
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(String resourceIngestionDefinitionId);
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.create;

import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.common.response.ConnectorResponse;

/** Callback which is called before executing the main logic of creating a resource */
public interface PreCreateResourceCallback {

  /**
   * Callback which is called before executing the main logic of creating a resource and after
   * validation. If it returns a response with code different from OK, the creation logic will not
   * be executed and CREATE_RESOURCE procedure will return this error.
   *
   * @param resource resource object created from procedure's arguments
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(VariantResource resource);
}

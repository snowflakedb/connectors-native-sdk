/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.create;

import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.common.response.ConnectorResponse;

/** Callback which is called at the beginning of creating a resource */
public interface CreateResourceValidator {

  /**
   * Callback which is called at the beginning of creating a resource but after simple validation
   * (whether a resource with given id already exists and whether the resource has a valid
   * structure). It can be used in order to add custom logic which validates whether the resource
   * can be created. If it returns a response with code different from OK, the creation logic will
   * not be executed and CREATE_RESOURCE procedure will return this error.
   *
   * @param resource resource object created from procedure's arguments
   * @return a response with the code {@code OK} if the validation was successful and the resource
   *     can be created, otherwise a response with an error code and an error message
   */
  ConnectorResponse validate(VariantResource resource);
}

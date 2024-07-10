/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.create;

import com.snowflake.connectors.application.ingestion.create.CreateResourceValidator;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Custom implementation of {@link CreateResourceValidator}, used by the {@link
 * TemplateCreateResourceHandler}.
 */
public class TemplateCreateResourceValidator implements CreateResourceValidator {

  @Override
  public ConnectorResponse validate(VariantResource resource) {
    // TODO: IMPLEMENT ME create resource validate: If there is a validation required when creating
    // a resource, it is a right place to implement the validation logic.
    // See more in docs:
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/create_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/create_resource_reference
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME create resource validate'");
  }
}

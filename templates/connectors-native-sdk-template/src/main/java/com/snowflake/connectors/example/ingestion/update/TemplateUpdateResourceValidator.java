/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.update;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.update.UpdateResourceValidator;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/**
 * Custom implementation of {@link UpdateResourceValidator}, used by the {@link
 * TemplateUpdateResourceHandler}.
 */
public class TemplateUpdateResourceValidator implements UpdateResourceValidator {

  @Override
  public ConnectorResponse validate(
      String resourceIngestionDefinitionId,
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations) {
    // TODO: IMPLEMENT ME update resource validate: If there is a validation required of an updated
    // resource it is a right place to implement the validation logic.
    // See more in docs:
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/update_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/update_resource_reference
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME update resource validate'");
  }
}

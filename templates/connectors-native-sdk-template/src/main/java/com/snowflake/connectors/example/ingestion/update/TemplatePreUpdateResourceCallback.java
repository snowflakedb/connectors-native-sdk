/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.update;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.update.PreUpdateResourceCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/**
 * Custom implementation of {@link PreUpdateResourceCallback}, used by the {@link
 * TemplateUpdateResourceHandler}.
 */
public class TemplatePreUpdateResourceCallback implements PreUpdateResourceCallback {

  @Override
  public ConnectorResponse execute(
      String resourceIngestionDefinitionId,
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations) {
    // TODO: IMPLEMENT ME pre update resource callback: If it is required to do some custom
    // operations before the resource is updated, it is a proper place to implement the logic.
    // See more in docs:
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/update_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/update_resource_reference
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME pre update resource"
            + " callback'");
  }
}

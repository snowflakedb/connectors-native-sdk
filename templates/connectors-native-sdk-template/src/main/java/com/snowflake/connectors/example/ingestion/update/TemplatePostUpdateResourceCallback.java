/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.update;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.update.PostUpdateResourceCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/**
 * Custom implementation of {@link PostUpdateResourceCallback}, used by the {@link
 * TemplateUpdateResourceHandler}.
 */
public class TemplatePostUpdateResourceCallback implements PostUpdateResourceCallback {

  @Override
  public ConnectorResponse execute(
      String resourceIngestionDefinitionId,
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations) {
    // TODO: IMPLEMENT ME post update resource callback: If it is required to do some custom
    // operations after the resource is updated, it is a proper place to implement the logic.
    // See more in docs:
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/update_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/update_resource_reference
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME post update resource"
            + " callback'");
  }
}

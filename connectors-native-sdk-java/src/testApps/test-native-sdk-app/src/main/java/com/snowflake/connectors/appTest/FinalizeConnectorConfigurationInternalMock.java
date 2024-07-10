/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.appTest;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;

public class FinalizeConnectorConfigurationInternalMock {

  public Variant execute(Session session, Variant config) {
    var configurationRepository = ConfigurationRepository.getInstance(session);
    var modifiedConfig =
        new Variant(
            Map.of(
                "unquoted_identifier",
                    getIdentifier("unquoted_identifier", config).getVariantValue(),
                "quoted_identifier", getIdentifier("quoted_identifier", config).getVariantValue(),
                "unquoted_obj_name", getObjectName("unquoted_obj_name", config).getVariantValue(),
                "quoted_obj_name", getObjectName("quoted_obj_name", config).getVariantValue()));

    configurationRepository.update("mock_custom_config_src", config);
    configurationRepository.update("mock_custom_config_modified", modifiedConfig);

    return ConnectorResponse.success().toVariant();
  }

  private static Identifier getIdentifier(String fieldName, Variant config) {
    return Identifier.from(config.asMap().get(fieldName).asString());
  }

  private static ObjectName getObjectName(String fieldName, Variant config) {
    return ObjectName.fromString(config.asMap().get(fieldName).asString());
  }
}

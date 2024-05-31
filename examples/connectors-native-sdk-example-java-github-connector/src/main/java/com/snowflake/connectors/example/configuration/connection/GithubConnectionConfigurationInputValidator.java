/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import static com.snowflake.connectors.example.configuration.connection.GithubConnectionConfiguration.INTEGRATION_PARAM;
import static com.snowflake.connectors.example.configuration.connection.GithubConnectionConfiguration.SECRET_PARAM;
import static java.lang.String.format;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationInputValidator;
import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.example.configuration.utils.Configuration;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Custom implementation of {@link ConnectionConfigurationInputValidator}, used by the {@link
 * GithubConnectionConfigurationHandler}, providing validation of the provided connection
 * configuration.
 */
public class GithubConnectionConfigurationInputValidator
    implements ConnectionConfigurationInputValidator {

  private static final String ERROR_CODE = "INVALID_CONNECTION_CONFIGURATION";

  @Override
  public ConnectorResponse validate(Variant config) {
    var integrationCheck = checkParameter(config, INTEGRATION_PARAM, false);
    if (!integrationCheck.isOk()) {
      return integrationCheck;
    }

    var secretCheck = checkParameter(config, SECRET_PARAM, true);
    if (!secretCheck.isOk()) {
      return secretCheck;
    }

    return ConnectorResponse.success();
  }

  private ConnectorResponse checkParameter(
      Variant config, String parameterName, boolean fullyQualified) {
    var connectorConfig = Configuration.fromCustomConfig(config);
    var value = connectorConfig.getValue(parameterName);

    if (value.isEmpty()) {
      return ConnectorResponse.error(
          ERROR_CODE, format("Missing configuration parameter: %s", parameterName));
    }

    try {
      var objectName = ObjectName.fromString(value.get());
      if (fullyQualified && !objectName.isFullyQualified()) {
        return ConnectorResponse.error(
            ERROR_CODE, format("Parameter %s is not a fully qualified object name", parameterName));
      }

      return ConnectorResponse.success();
    } catch (ConnectorException exception) {
      return exception.getResponse();
    }
  }
}

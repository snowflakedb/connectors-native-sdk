/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.AGENT_ROLE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.AGENT_USERNAME;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.CORTEX_USER_ROLE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.CORTEX_WAREHOUSE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DATA_OWNER_ROLE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_DATABASE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_SCHEMA;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.OPERATIONAL_WAREHOUSE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.WAREHOUSE;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.exception.InternalConnectorException;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultConnectorConfigurationServiceTest extends BaseIntegrationTest {

  private static final Variant CONFIG =
      new Variant(
          Map.of(
              WAREHOUSE.getPropertyName(),
              "warehouse",
              DESTINATION_DATABASE.getPropertyName(),
              "destination_db",
              DESTINATION_SCHEMA.getPropertyName(),
              "destination_schema",
              DATA_OWNER_ROLE.getPropertyName(),
              "role",
              OPERATIONAL_WAREHOUSE.getPropertyName(),
              "operational_warehouse",
              AGENT_USERNAME.getPropertyName(),
              "username",
              AGENT_ROLE.getPropertyName(),
              "agent",
              CORTEX_WAREHOUSE.getPropertyName(),
              "cortex_warehouse",
              CORTEX_USER_ROLE.getPropertyName(),
              "cortex_user_role"));

  private ConnectorConfigurationService connectorService;

  @BeforeAll
  void beforeAll() {
    connectorService = ConnectorConfigurationService.getInstance(session);
  }

  @BeforeEach
  public void cleanupConfig() {
    session.sql("DELETE FROM STATE.APP_CONFIG WHERE KEY = 'connector_configuration'").collect();
  }

  @Test
  public void shouldInsertFullConfigurationToConnectorConfig() {
    // when
    connectorService.updateConfiguration(CONFIG);

    // then
    Variant configFromDb = connectorService.getConfiguration();
    assertThat(configFromDb).isEqualTo(CONFIG);
  }

  @Test
  public void shouldReplaceConfiguration() {
    // given
    connectorService.updateConfiguration(CONFIG);

    // and
    Variant newConfig =
        new Variant(
            Map.of(
                WAREHOUSE.getPropertyName(),
                "warehouse_new",
                DATA_OWNER_ROLE.getPropertyName(),
                "role_new"));

    // when
    connectorService.updateConfiguration(newConfig);

    // then
    Variant configFromDb = connectorService.getConfiguration();
    assertThat(configFromDb).isEqualTo(newConfig);
  }

  @Test
  public void shouldThrowConnectorConfigurationNotFoundExceptionWhenThereIsNoConfigurationInDb() {
    // expect
    assertThatExceptionOfType(ConnectorConfigurationNotFoundException.class)
        .isThrownBy(() -> connectorService.getConfiguration())
        .withMessage("Connector configuration record not found in database.");
  }

  @Test
  public void
      shouldThrowInternalConnectorExceptionWhenUpdatingApp_configAndInputConfigIsNotAValidJson() {
    // expect
    assertThatExceptionOfType(InternalConnectorException.class)
        .isThrownBy(() -> connectorService.updateConfiguration(new Variant("wrong_value")))
        .withMessage("Given configuration is not a valid json");
  }
}

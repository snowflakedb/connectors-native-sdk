/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.Collection;

/** All keys allowed for the connector configuration properties. */
public enum ConnectorConfigurationKey {

  /** Main warehouse used by the connector. */
  WAREHOUSE("warehouse"),

  /** Database in which the connector data is stored. */
  DESTINATION_DATABASE("destination_database"),

  /** Schema in which the connector data is stored. */
  DESTINATION_SCHEMA("destination_schema"),

  /** Name of the role, which is the owner of the connector data. */
  DATA_OWNER_ROLE("data_owner_role"),

  /** Operational warehouse used by the connector. */
  OPERATIONAL_WAREHOUSE("operational_warehouse"),

  /** Global schedule for the connector data ingestion. */
  GLOBAL_SCHEDULE("global_schedule"),

  /** Username which is used by db connector's agent when connecting with Snowflake. */
  AGENT_USERNAME("agent_username"),

  /** Role which is used by db connector's agent when connecting with Snowflake. */
  AGENT_ROLE("agent_role"),

  /** Warehouse used for creating a Cortex Search object. */
  CORTEX_WAREHOUSE("cortex_warehouse"),

  /** Name of the role, that is able to use a Cortex Search available in the application. */
  CORTEX_USER_ROLE("cortex_user_role");

  private final String propertyName;

  ConnectorConfigurationKey(String propertyName) {
    this.propertyName = propertyName;
  }

  /**
   * Returns the name of the connector configuration property key.
   *
   * @return name of the connector configuration property key
   */
  public String getPropertyName() {
    return propertyName;
  }

  /** A collection containing names of all allowed connector configuration property keys. */
  public static final Collection<String> ALLOWED_PROPERTY_NAMES =
      Arrays.stream(ConnectorConfigurationKey.values())
          .map(ConnectorConfigurationKey::getPropertyName)
          .collect(toSet());
}

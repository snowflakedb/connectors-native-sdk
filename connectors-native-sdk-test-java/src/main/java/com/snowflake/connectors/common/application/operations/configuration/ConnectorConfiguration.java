/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.application.operations.configuration;

import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;

/** A base class for performing operations on connector configuration view. * */
public class ConnectorConfiguration {

  private static final String CONNECTOR_CONFIG_PATH = "PUBLIC.CONNECTOR_CONFIGURATION";

  /**
   * Get the set values for the configurations group.
   *
   * @param session Snowpark java session object (<a
   *     href="https://docs.snowflake.com/en/developer-guide/snowpark/java/creating-session">documentation</a>).
   * @param configGroup Connector configuration group filter.
   * @return Snowpark Java Row object (<a
   *     href="https://docs.snowflake.com/fr/developer-guide/snowpark/reference/java/com/snowflake/snowpark_java/class-use/Row.html">documentation</a>).
   */
  static Row[] getConnectorConfiguration(Session session, String configGroup) {
    return session
        .sql(
            "SELECT CONFIG_GROUP, CONFIG_KEY, VALUE FROM "
                + CONNECTOR_CONFIG_PATH
                + " WHERE CONFIG_GROUP = '"
                + configGroup
                + "' ORDER BY CONFIG_KEY ASC")
        .collect();
  }

  /**
   * Get the set values for the alerts configurations group.
   *
   * @param session Snowpark java session object (<a
   *     href="https://docs.snowflake.com/en/developer-guide/snowpark/java/creating-session">documentation</a>).
   * @return Snowpark Java Row object (<a
   *     href="https://docs.snowflake.com/fr/developer-guide/snowpark/reference/java/com/snowflake/snowpark_java/class-use/Row.html">documentation</a>).
   */
  public static Row[] getAlertsConfiguration(Session session) {
    return getConnectorConfiguration(session, "alerts_configuration");
  }
}

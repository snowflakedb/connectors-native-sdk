package com.snowflake.connectors.sdk.example_github_connector.application;

import com.snowflake.connectors.sdk.example_github_connector.common.KeyValueTable;
import com.snowflake.snowpark_java.Session;

public class StateApis {

  public static KeyValueTable configApi(Session session) {
    return new KeyValueTable(session, "STATE.APP_CONFIGURATION", false);
  }

  public static KeyValueTable resourcesConfiguration(Session session) {
    return new KeyValueTable(session, "STATE.RESOURCE_CONFIGURATION", false);
  }

  public static KeyValueTable ingestionState(Session session) {
    return new KeyValueTable(session, "STATE.APP_STATE", true);
  }
}

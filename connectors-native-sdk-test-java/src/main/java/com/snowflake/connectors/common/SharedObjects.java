package com.snowflake.connectors.common;

import static java.lang.String.format;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.SchemaName;
import com.snowflake.snowpark_java.Session;

/** Shared objects for use in integration and app tests. */
public final class SharedObjects {

  /** Name of the database used for storing shared objects. */
  public static final Identifier SHARED_TEST_DB = Identifier.from("SHARED_TEST_OBJECTS_NO_CLEANUP");

  /** Name of the database schema used for storing shared objects. */
  public static final SchemaName SHARED_TEST_DB_PUBLIC_SCHEMA =
      SchemaName.from(SHARED_TEST_DB.getValue(), "PUBLIC");

  /** Name of the shared generic string secret, with the value of {@code TEST}. */
  public static final ObjectName TEST_SECRET =
      ObjectName.from(SHARED_TEST_DB.getValue(), "PUBLIC", "TEST_SECRET");

  /** Name of the shared network rule, configured for {@code api.github.com:443}. */
  public static final ObjectName TEST_GH_NETWORK_RULE =
      ObjectName.from(SHARED_TEST_DB.getValue(), "PUBLIC", "TEST_GH_NETWORK_RULE");

  /**
   * Name of the shared external access integration, configured with {@link #TEST_GH_NETWORK_RULE}
   * and {@link #TEST_SECRET}.
   */
  public static final Identifier TEST_GH_EAI = Identifier.from("TEST_GH_EAI");

  /**
   * Creates all the shared objects specified above.
   *
   * @param session Snowpark session object
   */
  public static void createSharedObjects(Session session) {
    var showDbsQuery = format("SHOW DATABASES LIKE '%s'", SHARED_TEST_DB.getUnquotedValue());
    var showEAIsQuery =
        format("SHOW EXTERNAL ACCESS INTEGRATIONS LIKE '%s'", TEST_GH_EAI.getUnquotedValue());

    var dbExists = session.sql(showDbsQuery).collect().length > 0;
    var eaiExists = session.sql(showEAIsQuery).collect().length > 0;

    if (!dbExists) {
      session.sql(format("CREATE DATABASE %s", SHARED_TEST_DB.getValue())).toLocalIterator();
      session
          .sql(
              format(
                  "CREATE SECRET %s TYPE = GENERIC_STRING SECRET_STRING = 'TEST'",
                  TEST_SECRET.getValue()))
          .collect();
      session
          .sql(
              format(
                  "CREATE NETWORK RULE %s MODE = EGRESS TYPE = HOST_PORT"
                      + " VALUE_LIST=('api.github.com:443')",
                  TEST_GH_NETWORK_RULE.getValue()))
          .collect();
    }

    if (!eaiExists) {
      session
          .sql(
              format(
                  "CREATE EXTERNAL ACCESS INTEGRATION %s ALLOWED_NETWORK_RULES = (%s)"
                      + " ALLOWED_AUTHENTICATION_SECRETS = (%s) ENABLED = TRUE",
                  TEST_GH_EAI.getValue(), TEST_GH_NETWORK_RULE.getValue(), TEST_SECRET.getValue()))
          .collect();
    }
  }

  /**
   * Adds usage grants to the specified application for all the shared objects specified above.
   *
   * @param session Snowpark session object
   * @param appName application instance name
   */
  public static void addGrantsOnSharedObjectsToApplication(Session session, String appName) {
    session
        .sql(
            format(
                "GRANT USAGE ON DATABASE %s TO APPLICATION %s", SHARED_TEST_DB.getValue(), appName))
        .collect();
    session
        .sql(
            format(
                "GRANT USAGE ON SCHEMA %s TO APPLICATION %s",
                SHARED_TEST_DB_PUBLIC_SCHEMA.getValue(), appName))
        .collect();
    session
        .sql(format("GRANT READ ON SECRET %s TO APPLICATION %s", TEST_SECRET.getValue(), appName))
        .collect();
    session
        .sql(
            format(
                "GRANT USAGE ON INTEGRATION %s TO APPLICATION %s", TEST_GH_EAI.getValue(), appName))
        .collect();
  }

  private SharedObjects() {}
}

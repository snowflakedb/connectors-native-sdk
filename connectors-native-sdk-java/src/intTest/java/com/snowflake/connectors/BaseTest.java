/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import com.snowflake.connectors.common.snowflake.SnowflakeCredentials;
import com.snowflake.snowpark_java.Session;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseTest {

  protected static Session session;
  protected static final String TEST_ID =
      UUID.randomUUID().toString().replace('-', '_').toUpperCase();
  protected static final String DATABASE_NAME = "CONNECTORS_NATIVE_SDK_JAVA_IT_" + TEST_ID;

  protected static final String WAREHOUSE_NAME = "XS";

  @BeforeAll
  static void baseSetup() throws IOException {
    session = Session.builder().configs(SnowflakeCredentials.sessionConfigFromFile()).create();
    basePrepareDatabase(session);
  }

  private static void basePrepareDatabase(Session session) {
    session.sql("CREATE OR REPLACE DATABASE " + DATABASE_NAME).collect();
    session.sql("USE DATABASE " + DATABASE_NAME).collect();
    assert Objects.equals(session.getCurrentDatabase().orElse(null), "\"" + DATABASE_NAME + "\"");
  }

  @AfterAll
  public static void baseCleanup() {
    session.sql("DROP DATABASE " + DATABASE_NAME).collect();
  }
}

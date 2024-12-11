/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import com.snowflake.connectors.common.snowflake.SnowflakeCredentials;
import com.snowflake.snowpark_java.Session;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.Random.class)
public class BaseTest {

  protected Session session;
  protected final String TEST_ID = UUID.randomUUID().toString().replace('-', '_').toUpperCase();
  protected final String DATABASE_NAME = "CONNECTORS_NATIVE_SDK_JAVA_IT_" + TEST_ID;

  protected static final String WAREHOUSE_NAME = "XSMALL";

  @BeforeAll
  public void baseBeforeAll() throws IOException {
    session = Session.builder().configs(SnowflakeCredentials.sessionConfigFromFile()).create();
    basePrepareDatabase(session);
  }

  private void basePrepareDatabase(Session session) {
    session.sql("CREATE OR REPLACE DATABASE " + DATABASE_NAME).collect();
    session.sql("USE DATABASE " + DATABASE_NAME).collect();
    assert Objects.equals(session.getCurrentDatabase().orElse(null), "\"" + DATABASE_NAME + "\"");
  }

  @AfterAll
  public void baseAfterAll() {
    session.sql("DROP DATABASE " + DATABASE_NAME).collect();
  }
}

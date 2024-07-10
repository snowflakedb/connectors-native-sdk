/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.Random.class)
public class BaseNativeSdkIntegrationTest {

  protected static final String WAREHOUSE = "XS";

  protected Session session;
  protected Application application;

  @BeforeEach
  public void beforeEach() {
    application.resetState();
  }

  @BeforeAll
  public void beforeAll() throws IOException {
    session = SnowparkSessionProvider.createSession();
    application = Application.createNewInstance(session);

    session.sql("USE DATABASE " + application.instanceName).collect();
    session.sql("USE SCHEMA PUBLIC").collect();

    application.grantUsageOnWarehouse(WAREHOUSE);
    application.grantExecuteTaskPrivilege();
    application.setDebugMode(true);
  }

  @AfterAll
  public void afterAll() {
    application.dropInstance();
  }

  protected void assertConnectorStatus(
      ConnectorStatus connectorStatus,
      ConnectorStatus.ConnectorConfigurationStatus configurationStatus) {
    assertThatResponseMap(application.getConnectorStatus())
        .hasOKResponseCode()
        .hasField("status", connectorStatus.name())
        .hasField("configurationStatus", configurationStatus.name());
  }

  protected void assertThatPrerequisitesAreExactly(Row[] prerequisites) {
    assertThat(application.getPrerequisites()).containsExactlyInAnyOrder(prerequisites);
  }

  protected void assertThatConfigIsEqualTo(Row[] config) {
    assertThat(application.getConnectorConfiguration()).containsExactlyInAnyOrder(config);
  }

  protected void assertTaskReactorConfig(Row[] config) {
    assertThat(application.getTaskReactorConfig()).containsExactlyInAnyOrder(config);
  }
}

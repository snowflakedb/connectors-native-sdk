/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import com.snowflake.snowpark_java.Session;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * This base test allows for the extension to test scenarios when upgrading from one specific
 * version of SDK to another one. See {@link ConfigurableVersionsUpgradeDowngradeTest} for example
 * usage.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.Random.class)
public abstract class ConfigurableVersionsUpgradeDowngradeBaseTest {

  private static final String WAREHOUSE = "XS";

  private final String randomSuffix = UUID.randomUUID().toString().replace('-', '_');
  private final String appPackageName = "NATIVE_SDK_TEST_APP_" + randomSuffix;
  protected Session session;
  protected Application application;

  @BeforeEach
  public void beforeEach() {
    application.resetState();
  }

  @BeforeAll
  public void beforeAll() throws IOException {
    session = SnowparkSessionProvider.createSession();
    Application.setupApplicationWithCustomVersions(
        firstApplicationVersion(), secondApplicationVersion(), appPackageName);
    application =
        Application.createNewInstance(
            session, firstApplicationVersion().getAppPackageVersion(), appPackageName);

    session.sql("USE DATABASE " + application.instanceName).collect();
    session.sql("USE SCHEMA PUBLIC").collect();

    application.grantUsageOnWarehouse(WAREHOUSE);
    application.grantExecuteTaskPrivilege();
    application.setDebugMode(true);
  }

  @AfterAll
  public void afterAll() {
    application.dropInstance();
    Application.dropApplicationPackage(appPackageName);
  }

  protected abstract ApplicationVersion firstApplicationVersion();

  protected abstract ApplicationVersion secondApplicationVersion();
}

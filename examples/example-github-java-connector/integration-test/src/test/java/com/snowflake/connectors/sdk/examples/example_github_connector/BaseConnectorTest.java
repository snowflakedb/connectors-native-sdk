package com.snowflake.connectors.sdk.examples.example_github_connector;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.io.File;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseConnectorTest {

  static Logger logger = LoggerFactory.getLogger(BaseConnectorTest.class);

  private static final String APP_NAME_PREFIX = "EXAMPLE_GH_JAVA_CONNECTOR";
  private static final String APP_VERSION = "V_1_0_0"; // TODO read from manifest
  static final String API_INTEGRATION = "gh_integration";

  static Session session;

  static int randomPostfix = new Random().nextInt(Integer.MAX_VALUE);

  static String secretsDatabaseName = "SECRETS_DB";

  static String stageDatabaseName = format("%s_STAGE", APP_NAME_PREFIX);

  static String applicationPackageName = format("%s_PACKAGE_%d", APP_NAME_PREFIX, randomPostfix);

  static String applicationInstanceName = format("%s_INSTANCE_%d", APP_NAME_PREFIX, randomPostfix);

  String destinationDatabaseName = format("DEST_DB_%d", randomPostfix);

  @BeforeAll
  static void setupAll() {
    var path = BaseConnectorTest.class.getResource("/it_config.properties").getPath();
    logger.info("path to config: {}", path);
    session = Session.builder().configFile(path).create();

    buildConnector();
    createStage();
    uploadArtifacts();
  }

  @BeforeEach
  void setup() {
    randomPostfix = new Random().nextInt(Integer.MAX_VALUE);
    deployConnector();
    installApplication();
  }

  @AfterEach
  void cleanup() {
    requireNonNull(session);
    logger.info("Application instance name: {}", applicationInstanceName);
    session
        .sql(format("DROP APPLICATION IDENTIFIER('%s') CASCADE", applicationInstanceName))
        .collect();
    session
        .sql(format("DROP APPLICATION PACKAGE IDENTIFIER('%s')", applicationPackageName))
        .collect();
  }

  @AfterAll
  static void cleanupSpec() {
    session.sql(format("DROP DATABASE IDENTIFIER('%s')", stageDatabaseName)).collect();
  }

  static void buildConnector() {
    var currentDir = currentDir();
    logger.info("Building connector in {}", currentDir);
    var exitStatus = BuildingHelper.runCommand("make build", currentDir);
    assert exitStatus == 0;
  }

  static void deployConnector() {
    createApplicationPackage();
  }

  static void createStage() {
    session.sql(format("CREATE OR REPLACE DATABASE IDENTIFIER('%s')", stageDatabaseName)).collect();
    session.sql(format("CREATE STAGE IF NOT EXISTS IDENTIFIER('%s')", stageName())).collect();
  }

  static String stageName() {
    return stageDatabaseName + ".public.artifacts";
  }

  static void uploadArtifacts() {
    File directory = currentDir();
    String absPath = directory.getAbsolutePath();

    session
        .file()
        .put(
            format("file://%s/sf_build/*", absPath),
            format("@%s/%s", stageName(), APP_VERSION),
            Map.of("AUTO_COMPRESS", "FALSE"));
  }

  static void createApplicationPackage() {
    session.sql(format("DROP DATABASE IF EXISTS IDENTIFIER('%s')", applicationPackageName));
    session
        .sql(
            format(
                "CREATE APPLICATION PACKAGE IF NOT EXISTS IDENTIFIER('%s')",
                applicationPackageName))
        .collect();
  }

  static void installApplication() {
    session.sql(format("DROP DATABASE IF EXISTS IDENTIFIER('%s')", applicationInstanceName));
    session
        .sql(
            format(
                "CREATE APPLICATION IDENTIFIER('%s') FROM APPLICATION PACKAGE IDENTIFIER('%s')"
                    + " USING @%s/%s",
                applicationInstanceName, applicationPackageName, stageName(), APP_VERSION))
        .collect();
    session
        .sql(
            format(
                "grant usage on integration %s to application IDENTIFIER('%s')",
                API_INTEGRATION, applicationInstanceName))
        .collect();
    session
        .sql(
            format(
                "grant usage on database %s to application IDENTIFIER('%s')",
                secretsDatabaseName, applicationInstanceName))
        .collect();
    session
        .sql(
            format(
                "grant usage on schema %s.PUBLIC to application IDENTIFIER('%s')",
                secretsDatabaseName, applicationInstanceName))
        .collect();
    session
        .sql(
            format(
                "grant read on secret %s.PUBLIC.GITHUB_TOKEN to application IDENTIFIER('%s')",
                secretsDatabaseName, applicationInstanceName))
        .collect();
    session
        .sql(
            format(
                "grant create database ON ACCOUNT TO APPLICATION IDENTIFIER('%s')",
                applicationInstanceName))
        .collect();
    session
        .sql(
            format(
                "grant usage on warehouse xs to application IDENTIFIER('%s')",
                applicationInstanceName))
        .collect();
    session
        .sql(
            format(
                "grant execute task on account to application IDENTIFIER('%s')",
                applicationInstanceName))
        .collect();
    session
        .sql(
            format(
                "grant execute managed task on account to application IDENTIFIER('%s')",
                applicationInstanceName))
        .collect();
    session.sql(format("USE DATABASE %s", applicationInstanceName)).collect();
  }

  static File currentDir() {
    String currentDir = System.getProperty("user.dir");
    if (currentDir.endsWith("/integration-test")) {
      currentDir = currentDir.substring(0, currentDir.lastIndexOf("/integration-test"));
    }
    return new File(currentDir);
  }

  Row[] runQuery(String query) {
    return session.sql(query).collect();
  }
}

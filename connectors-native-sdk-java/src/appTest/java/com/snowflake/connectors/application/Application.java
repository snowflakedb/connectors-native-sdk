/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application;

import static java.lang.String.format;

import com.snowflake.snowpark_java.Session;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

  public static final String APP_VERSION = "1.0";
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
  private static final String RANDOM_SUFFIX = UUID.randomUUID().toString().replace('-', '_');
  private static final String APP_PACKAGE_NAME = "NATIVE_SDK_TEST_APP_" + RANDOM_SUFFIX;
  public final String instanceName;
  private final Session session;

  public Application(Session session, String instanceName) {
    this.session = session;
    this.instanceName = instanceName;
  }

  public static File getAppDir() {
    return getFileRelativeToProjectRoot("src/testApps/test-native-sdk-app");
  }

  private static File getFileRelativeToProjectRoot(String relativePath) {
    return new File(getProjectRoot(), relativePath);
  }

  private static File getProjectRoot() {
    return new File(System.getProperty("user.dir"));
  }

  public static void setupApplication() {
    LOG.info("Application setup started. Application package name: '{}'", APP_PACKAGE_NAME);
    publishNativeSdkToMavenLocal();
    buildDeployAndCreateVersion();
  }

  private static void publishNativeSdkToMavenLocal() {
    LOG.info("Publishing connectors-native-sdk to maven local...");
    var command = "make publish_sdk_to_maven_local";
    runCommand(command, getAppDir());
  }

  private static void buildDeployAndCreateVersion() {
    LOG.info("Initializing application...");
    var command =
        String.format(
            "make build_and_create_app_version APP_PACKAGE_NAME=%s VERSION=%s"
                + " CONNECTORS_NATIVE_SDK_VERSION=%s",
            APP_PACKAGE_NAME, APP_VERSION, "+");
    runCommand(command, getAppDir());
  }

  public static void dropApplicationPackage() {
    LOG.info("Dropping application data...");
    var command = String.format("make drop_application APP_PACKAGE_NAME=%s", APP_PACKAGE_NAME);
    runCommand(command, getAppDir());
  }

  public static Application createNewInstance(Session session) {
    String applicationName = generateNewInstanceName();
    LOG.info(
        "Creating new application instance with name {} using application package {}",
        applicationName,
        APP_PACKAGE_NAME);
    String command =
        String.format(
            "CREATE APPLICATION %s FROM APPLICATION PACKAGE %s USING VERSION \"%s\"",
            applicationName, APP_PACKAGE_NAME, APP_VERSION);
    session.sql(command).collect();
    return new Application(session, applicationName);
  }

  public static String generateNewInstanceName() {
    var randomInstanceSuffix = UUID.randomUUID().toString().replace('-', '_');
    return String.format("%s_INSTANCE_%s", APP_PACKAGE_NAME, randomInstanceSuffix);
  }

  public static String runCommand(String command, File workingDir) {
    LOG.info("Running '{}' in dir: '{}'}", command, workingDir.getAbsolutePath());
    try {
      var process =
          new ProcessBuilder("/bin/sh", "-c", command)
              .directory(workingDir)
              .redirectErrorStream(true)
              .start();
      var reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

      StringBuilder processOutput = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        LOG.info(line);
        processOutput.append(line);
      }
      int status = process.waitFor();

      assert status == 0 : format("Command: '%s', failed.", command);
      return processOutput.toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void dropInstance() {
    LOG.info("Dropping application instance with name {}", instanceName);
    session.sql(String.format("DROP APPLICATION IF EXISTS %s CASCADE", instanceName)).collect();
  }

  public void upgrade() {
    var query = "ALTER APPLICATION %s UPGRADE USING VERSION \"%s\"";
    session.sql(format(query, instanceName, Application.APP_VERSION)).collect();
  }

  public void grantExecuteTaskPrivilege() {
    session.sql("GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION " + instanceName).collect();
  }

  public void revokeExecuteTaskPrivilege() {
    session.sql("REVOKE EXECUTE TASK ON ACCOUNT FROM APPLICATION " + instanceName).collect();
  }

  public void grantUsageOnWarehouse(String warehouse) {
    session
        .sql(
            String.format("GRANT USAGE ON WAREHOUSE %s TO APPLICATION %s", warehouse, instanceName))
        .collect();
  }

  public void revokeUsageOnWarehouse(String warehouse) {
    session
        .sql(
            String.format(
                "REVOKE USAGE ON WAREHOUSE %s FROM APPLICATION %s", warehouse, instanceName))
        .collect();
  }

  public void setDebugMode(boolean enable) {
    session
        .sql(String.format("ALTER APPLICATION %s SET DEBUG_MODE = %s", instanceName, enable))
        .collect();
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application;

import static com.snowflake.connectors.application.CommandRunner.runCommand;
import static java.lang.String.format;

import com.snowflake.snowpark_java.Session;
import java.io.File;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

  public static final String APP_VERSION = "1.0";
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
  public final String instanceName;
  private final Session session;

  public Application(Session session, String instanceName) {
    this.session = session;
    this.instanceName = instanceName;
  }

  public static File getSnowflakeCliConfigFile() {
    return getFileRelativeToProjectRoot(System.getProperty("configurationFile"));
  }

  public static File getFileRelativeToProjectRoot(String relativePath) {
    return new File(getProjectRoot(), relativePath);
  }

  private static File getProjectRoot() {
    return new File(System.getProperty("user.dir"));
  }

  public static void setupApplication(String appDir, String appPackageName) {
    LOG.info("Application setup started. Application package name: '{}'", appPackageName);
    publishNativeSdkToMavenLocal(appDir);
    buildDeployAndCreateVersion(appDir, appPackageName);
  }

  private static void publishNativeSdkToMavenLocal(String appDir) {
    LOG.info("Publishing connectors-native-sdk to maven local...");
    var command = "make publish_sdk_to_maven_local";
    runCommand(command, getFileRelativeToProjectRoot(appDir));
  }

  private static void buildDeployAndCreateVersion(String appDir, String appPackageName) {
    LOG.info("Initializing application...");
    var command =
        String.format(
            "make build_and_create_app_version APP_PACKAGE_NAME=%s VERSION=\"%s\""
                + " CONNECTORS_NATIVE_SDK_VERSION=%s CONNECTION_FILE=%s",
            appPackageName, APP_VERSION, "+", getSnowflakeCliConfigFile());
    runCommand(command, getFileRelativeToProjectRoot(appDir));
  }

  public static void dropApplicationPackage(String appDir, String appPackageName) {
    LOG.info("Dropping application data...");
    var command =
        String.format(
            "make drop_application APP_PACKAGE_NAME=%s  CONNECTION_FILE=%s",
            appPackageName, getSnowflakeCliConfigFile());
    runCommand(command, getFileRelativeToProjectRoot(appDir));
  }

  public static Application createNewInstance(Session session, String appPackageName) {
    String applicationName = generateNewInstanceName(appPackageName);
    LOG.info(
        "Creating new application instance with name {} using application package {}",
        applicationName,
        appPackageName);
    String command =
        String.format(
            "CREATE APPLICATION %s FROM APPLICATION PACKAGE %s USING VERSION \"%s\"",
            applicationName, appPackageName, APP_VERSION);
    session.sql(command).collect();
    return new Application(session, applicationName);
  }

  private static String generateNewInstanceName(String appPackageName) {
    var randomInstanceSuffix = UUID.randomUUID().toString().replace('-', '_');
    return String.format("%s_INSTANCE_%s", appPackageName, randomInstanceSuffix);
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
}

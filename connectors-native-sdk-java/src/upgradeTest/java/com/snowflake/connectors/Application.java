/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static java.lang.String.format;

import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

  public static final String INITIAL_APP_VERSION = "1.0";
  public static final String UPGRADED_APP_VERSION = "2.0";
  public static final String TASK_REACTOR_INSTANCE_NAME = "TR_INSTANCE";
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
  private static final String RANDOM_SUFFIX = UUID.randomUUID().toString().replace('-', '_');
  private static final String APP_PACKAGE_NAME = "NATIVE_SDK_TEST_APP_" + RANDOM_SUFFIX;
  private static final String RELATIVE_APP_DIR = "src/testApps/test-native-sdk-app-upgrade";
  private static final String RELATIVE_SNOWFLAKE_CLI_CREDENTIALS_FILE =
      System.getProperty("configurationFile");
  private static final String RELATIVE_PACKAGES_DIR = "src/testApps/testPackages";
  private static final String RELATIVE_UNPACKED_DIR = "src/testApps/testPackages/unpackedApp";
  private static final String PACKAGE_NAME_FORMAT = "test-native-sdk-app-%s.zip";
  public final String instanceName;
  private final Session session;

  public Application(Session session, String instanceName) {
    this.session = session;
    this.instanceName = instanceName;
  }

  public static File getAppDir() {
    return new File(getProjectRoot(), RELATIVE_APP_DIR);
  }

  public static File getSnowflakeCliConfigFile() {
    return new File(getProjectRoot(), RELATIVE_SNOWFLAKE_CLI_CREDENTIALS_FILE);
  }

  public static File getPackageDir() {
    return new File(getProjectRoot(), RELATIVE_PACKAGES_DIR);
  }

  public static File getUnpackedAppDir() {
    return new File(getProjectRoot(), RELATIVE_UNPACKED_DIR);
  }

  private static File getProjectRoot() {
    return new File(System.getProperty("user.dir"));
  }

  public static void setupApplication() throws IOException {
    LOG.info("Application setup started. Application package name: '{}'", APP_PACKAGE_NAME);
    createVersionFromLatestRelease();

    createSecondVersionFromLocalSources();
  }

  public static void setupApplicationWithCustomVersions(
      ApplicationVersion firstVersion, ApplicationVersion secondVersion, String appPackageName)
      throws IOException {
    unpackApp(firstVersion.getSdkVersion());
    buildDeployAndCreateVersion(
        firstVersion.getSdkVersion(),
        firstVersion.getAppPackageVersion(),
        appPackageName,
        getUnpackedAppDir());

    FileUtils.deleteDirectory(getUnpackedAppDir());

    unpackApp(secondVersion.getSdkVersion());
    createSecondVersion(
        secondVersion.getSdkVersion(),
        secondVersion.getAppPackageVersion(),
        appPackageName,
        getUnpackedAppDir());

    FileUtils.deleteDirectory(getUnpackedAppDir());
  }

  private static void createSecondVersionFromLocalSources() {
    LOG.info("Adding new version to the application using locally built connectors-native-sdk...");
    publishNativeSdkToMavenLocal();

    createSecondVersion("+", UPGRADED_APP_VERSION, APP_PACKAGE_NAME, getAppDir());
  }

  private static void createSecondVersion(
      String secondSdkVersion, String appPackageVersion, String appPackageName, File appDir) {
    var copyInternalCommand = "make copy_internal_components";
    runCommand(copyInternalCommand, appDir);

    var copyCommand =
        format("make copy_sdk_components CONNECTORS_NATIVE_SDK_VERSION=%s", secondSdkVersion);
    runCommand(copyCommand, appDir);

    var deployCommand =
        format(
            "make deploy_app_package APP_PACKAGE_NAME=%s VERSION=\"%s\""
                + " CONNECTORS_NATIVE_SDK_VERSION=%s CONNECTION_FILE=%s",
            appPackageName, appPackageVersion, secondSdkVersion, getSnowflakeCliConfigFile());
    runCommand(deployCommand, appDir);

    var newVersionCommand =
        format(
            "make create_new_version APP_PACKAGE_NAME=%s VERSION=\"%s\" CONNECTION_FILE=%s",
            appPackageName, appPackageVersion, getSnowflakeCliConfigFile());
    runCommand(newVersionCommand, appDir);
  }

  private static void publishNativeSdkToMavenLocal() {
    LOG.info("Publishing connectors-native-sdk to maven local...");
    var command = "make publish_sdk_to_maven_local";
    runCommand(command, getAppDir());
  }

  private static void createVersionFromLatestRelease() throws IOException {
    LOG.info("Retrieving latest published version for connectors-native-sdk from tags...");
    var versionCommand = "make get_latest_released_sdk_version";

    var version =
        runCommand(versionCommand, getAppDir()).stream()
            .filter(it -> it.matches("\\d+\\.\\d+\\.\\d+"))
            .findFirst()
            .orElseThrow(
                () ->
                    new RuntimeException(
                        "Cannot find recent version of connectors-native-sdk with command "
                            + versionCommand));

    LOG.info(format("Latest published version is: %s", version));

    buildDeployAndCreateVersionFromPackage(version, INITIAL_APP_VERSION, APP_PACKAGE_NAME);
  }

  private static void buildDeployAndCreateVersionFromPackage(
      String version, String appPackageVersion, String appPackageName) throws IOException {
    LOG.info("Creating application from package zip...");

    unpackApp(version);

    buildDeployAndCreateVersion(version, appPackageVersion, appPackageName, getUnpackedAppDir());

    FileUtils.deleteDirectory(getUnpackedAppDir());
  }

  private static void unpackApp(String version) {
    LOG.info("Unpacking app for version: {}...", version);
    var packageFile =
        Arrays.stream(Objects.requireNonNull(getPackageDir().listFiles()))
            .filter(it -> it.getName().equals(format(PACKAGE_NAME_FORMAT, version)))
            .findFirst()
            .orElseThrow(
                () ->
                    new RuntimeException(
                        format("No matching package for sdk version: %s", version)));

    getUnpackedAppDir().mkdir();
    var unpackCommand =
        format("unzip %s -d %s", packageFile.getName(), getUnpackedAppDir().getAbsolutePath());

    runCommand(unpackCommand, getPackageDir());
  }

  private static void buildDeployAndCreateVersion(
      String baseSdkVersion, String appPackageVersion, String appPackageName, File appDir) {
    LOG.info("Initializing application...");

    var command =
        format(
            "make build_and_create_app_version APP_PACKAGE_NAME=%s VERSION=\"%s\""
                + " CONNECTORS_NATIVE_SDK_VERSION=%s CONNECTION_FILE=%s",
            appPackageName, appPackageVersion, baseSdkVersion, getSnowflakeCliConfigFile());
    runCommand(command, appDir);
  }

  public static void dropApplicationPackage() {
    dropApplicationPackage(APP_PACKAGE_NAME);
  }

  public static void dropApplicationPackage(String appPackageName) {
    LOG.info("Dropping application data...");
    var command =
        format(
            "make drop_application APP_PACKAGE_NAME=%s CONNECTION_FILE=%s",
            appPackageName, getSnowflakeCliConfigFile());
    runCommand(command, getAppDir());
  }

  public static Application createNewInstance(Session session) {
    return createNewInstance(session, INITIAL_APP_VERSION, APP_PACKAGE_NAME);
  }

  public static Application createNewInstance(
      Session session, String appVersion, String appPackageName) {
    String applicationName = generateNewInstanceName(appPackageName);
    LOG.info(
        "Creating new application instance with name {} using application package {}",
        applicationName,
        appPackageName);
    String command =
        format(
            "CREATE APPLICATION %s FROM APPLICATION PACKAGE %s USING VERSION \"%s\"",
            applicationName, appPackageName, appVersion);
    session.sql(command).collect();
    return new Application(session, applicationName);
  }

  private static String generateNewInstanceName(String appPackageName) {
    var randomInstanceSuffix = UUID.randomUUID().toString().replace('-', '_');
    return format("%s_INSTANCE_%s", appPackageName, randomInstanceSuffix);
  }

  public static List<String> runCommand(String command, File workingDir) {
    LOG.info("Running '{}' in dir: '{}'}", command, workingDir.getAbsolutePath());
    try {
      var process =
          new ProcessBuilder("/bin/sh", "-c", command)
              .directory(workingDir)
              .redirectErrorStream(true)
              .start();
      var reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

      List<String> processOutput = new ArrayList<String>();
      String line;
      while ((line = reader.readLine()) != null) {
        LOG.info(line);
        processOutput.add(line);
      }
      int status = process.waitFor();

      assert status == 0 : format("Command: '%s', failed.", command);
      return processOutput;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void dropInstance() {
    LOG.info("Dropping application instance with name {}", instanceName);
    session.sql(format("DROP APPLICATION IF EXISTS %s CASCADE", instanceName)).collect();
  }

  public void upgrade() {
    alterVersion(UPGRADED_APP_VERSION);
  }

  public void downgrade() {
    alterVersion(INITIAL_APP_VERSION);
  }

  public void alterVersion(String version) {
    var query = "ALTER APPLICATION %s UPGRADE USING VERSION \"%s\"";
    session.sql(format(query, instanceName, version)).collect();
  }

  public void grantExecuteTaskPrivilege() {
    session.sql("GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION " + instanceName).collect();
  }

  public void grantUsageOnWarehouse(String warehouse) {
    session
        .sql(format("GRANT USAGE ON WAREHOUSE %s TO APPLICATION %s", warehouse, instanceName))
        .collect();
  }

  public void resetState() {
    setConnectorStatus(ConnectorStatus.CONFIGURING, ConnectorConfigurationStatus.INSTALLED);
    executeInApp("TRUNCATE TABLE IF EXISTS STATE.APP_CONFIG");
    executeInApp("UPDATE TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY SET IS_INITIALIZED = FALSE");
    executeInApp("DROP TASK IF EXISTS TR_INSTANCE.DISPATCHER_TASK");
  }

  public void setConnectorStatus(
      ConnectorStatus status, ConnectorConfigurationStatus configurationStatus) {
    String connectorStatus =
        format(
            "OBJECT_CONSTRUCT('status', '%s', 'configurationStatus', '%s')",
            status, configurationStatus);

    var query =
        "MERGE INTO STATE.APP_STATE AS dst "
            + "USING (SELECT %1$s AS value) AS src "
            + "ON dst.key = '%2$s' "
            + "WHEN MATCHED THEN UPDATE SET dst.value = src.value "
            + "WHEN NOT MATCHED THEN INSERT VALUES ('%2$s', src.value, current_timestamp())";
    executeInApp(format(query, connectorStatus, "connector_status"));
  }

  public Row[] getConnectorConfiguration() {
    return session.sql("SELECT * FROM PUBLIC.CONNECTOR_CONFIGURATION").collect();
  }

  public Row[] getPrerequisites() {
    return session.sql("SELECT * FROM PUBLIC.PREREQUISITES").collect();
  }

  public List<String> getInitializedTaskReactorInstances() {
    String query =
        "SELECT instance_name FROM TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY WHERE is_initialized ="
            + " TRUE";
    return Arrays.stream(executeInApp(query))
        .map(it -> it.getString(0))
        .collect(Collectors.toList());
  }

  public Row[] getTaskReactorConfig() {
    return executeInApp(format("SELECT * FROM %s.CONFIG", TASK_REACTOR_INSTANCE_NAME));
  }

  public Map<String, Variant> getConnectorStatus() {
    return callProcedure("GET_CONNECTOR_STATUS()");
  }

  public void pause() {
    var response = callProcedure("PAUSE_CONNECTOR()");
    assertThatResponseMap(response).hasOKResponseCode();
  }

  public void resume() {
    var response = callProcedure("RESUME_CONNECTOR()");
    assertThatResponseMap(response).hasOKResponseCode();
  }

  public void completeWizard() {
    completePrerequisites();
    configureConnector();
    configureConnection();
    finalizeConfiguration();
  }

  public void initializeTaskReactorInstance(String warehouse) {
    var response =
        callProcedureInApp(
            format(
                "INITIALIZE_INSTANCE('%s', '%s', null, null, null, null)",
                TASK_REACTOR_INSTANCE_NAME, warehouse),
            "TASK_REACTOR");
    assertThatResponseMap(response).hasOKResponseCode();
  }

  public void completePrerequisites() {
    var response = callProcedure("MARK_ALL_PREREQUISITES_AS_DONE()");
    assertThatResponseMap(response).hasOKResponseCode();
    var completeResponse = callProcedure("COMPLETE_PREREQUISITES_STEP()");
    assertThatResponseMap(completeResponse).hasOKResponseCode();
  }

  public void configureConnector() {
    var response =
        callProcedure(
            format(
                "CONFIGURE_CONNECTOR(parse_json('{\"destination_database\":\"destdb_%s\"}'))",
                RANDOM_SUFFIX));
    assertThatResponseMap(response).hasOKResponseCode();
  }

  public void configureConnection() {
    var response =
        callProcedure(
            "SET_CONNECTION_CONFIGURATION(PARSE_JSON('{\"connection_prop\":\"value123\"}'))");
    assertThatResponseMap(response).hasOKResponseCode();
  }

  public void finalizeConfiguration() {
    var response = callProcedure("FINALIZE_CONNECTOR_CONFIGURATION(PARSE_JSON('{}'))");
    assertThatResponseMap(response).hasOKResponseCode();
  }

  private Map<String, Variant> callProcedure(String procedureQuery) {
    return callProcedure(procedureQuery, "PUBLIC");
  }

  private Map<String, Variant> callProcedure(String procedureQuery, String schema) {
    Variant response = callProcedureRaw(procedureQuery, schema);
    return response == null ? new HashMap<>() : response.asMap();
  }

  private Variant callProcedureRaw(String procedureQuery, String schema) {
    return session.sql(format("CALL %s.%s", schema, procedureQuery)).collect()[0].getVariant(0);
  }

  private Row[] executeInApp(String query) {
    return session.sql(format("CALL PUBLIC.EXECUTE_SQL(%s)", asVarchar(query))).collect();
  }

  private Map<String, Variant> callProcedureInApp(String procedureQuery) {
    return callProcedure(procedureQuery, "PUBLIC");
  }

  private Map<String, Variant> callProcedureInApp(String procedureQuery, String schema) {
    Variant response = callProcedureRawInApp(procedureQuery, schema);
    return response == null ? new HashMap<>() : response.asMap();
  }

  private Variant callProcedureRawInApp(String procedureQuery, String schema) {
    return executeInApp(format("CALL %s.%s", schema, procedureQuery))[0].getVariant(0);
  }
}

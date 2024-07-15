/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static java.lang.String.format;

import com.snowflake.connectors.BaseTest;
import com.snowflake.connectors.SnowsqlConfigurer;
import com.snowflake.connectors.taskreactor.utils.CommandLineHelper;
import com.snowflake.connectors.taskreactor.utils.GradleUtils;
import com.snowflake.connectors.taskreactor.utils.SnowsqlFileExecutor;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public class BaseTaskReactorIntegrationTest extends BaseTest {

  public static final String TASK_REACTOR_TEST_SCHEMA = "TASK_REACTOR_TESTS";
  public static final String WORKER_PROCEDURE_LOCATION =
      TASK_REACTOR_TEST_SCHEMA + ".WORKER_PROCEDURE";
  public static final String WORK_SELECTOR_LOCATION = TASK_REACTOR_TEST_SCHEMA + ".WORK_SELECTOR";
  public static final String EXPIRED_WORK_SELECTOR_LOCATION =
      TASK_REACTOR_TEST_SCHEMA + ".EMPTY_EXPIRED_WORK_SELECTOR";
  private static final String SNOWFLAKE_CONNECTION = "native_sdk_connection";
  private static final String STAGE_NAME = "TASK_REACTOR_STAGE";
  private static final String STAGE_SCHEMA = "STAGE_SCHEMA";
  private static final String JAR_NAME = GradleUtils.getSdkJarName();
  protected static final String FAILING_WORKER_PROCEDURE_LOCATION =
      TASK_REACTOR_TEST_SCHEMA + ".FAILING_WORKER_PROCEDURE";
  protected static final String WORKER_PROCEDURE_CALLS =
      TASK_REACTOR_TEST_SCHEMA + ".WORKER_PROCEDURE_CALLS";
  protected static final String TEST_INSTANCE = "TEST_INSTANCE";

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    SnowsqlConfigurer.configureSnowsqlInDocker();
    buildProject();
    createStage();
    setUpTaskReactorApi();
    createTestObjects();
  }

  @BeforeEach
  public void beforeEach() {
    session.sql("DELETE FROM " + WORKER_PROCEDURE_CALLS).collect();
  }

  private static void buildProject() throws IOException, InterruptedException {
    String command = "./gradlew build -x test";
    int buildResult = CommandLineHelper.runCommand(command, currentDir());
    assert buildResult == 0
        : format(
            "Building with command '%s' has failed in dir '%s'",
            command, currentDir().getAbsolutePath());
  }

  protected static File currentDir() {
    return new File(System.getProperty("user.dir"));
  }

  private static void createStage() {
    session.sql(format("CREATE OR REPLACE SCHEMA %s", STAGE_SCHEMA)).collect();
    session.sql(format("CREATE OR REPLACE STAGE %s.%s", STAGE_SCHEMA, STAGE_NAME)).collect();
    session
        .file()
        .put(
            format("file://%s/build/libs/%s", currentDir().getAbsolutePath(), JAR_NAME),
            format("@%s", STAGE_NAME),
            Map.of("AUTO_COMPRESS", "FALSE"));
  }

  private static void createTestObjects() {
    String pathToSetupFile =
        ClassLoader.getSystemClassLoader().getResource("task_reactor_tests_setup.sql").getPath();

    SnowsqlFileExecutor.from(pathToSetupFile)
        .usingConnection(SNOWFLAKE_CONNECTION)
        .usingDatabase(DATABASE_NAME)
        .replaceText("<test_schema>")
        .with(TASK_REACTOR_TEST_SCHEMA)
        .replaceText("<work_selector>")
        .with(WORK_SELECTOR_LOCATION)
        .replaceText("<expired_work_selector>")
        .with(EXPIRED_WORK_SELECTOR_LOCATION)
        .replaceText("<worker_procedure_calls>")
        .with(WORKER_PROCEDURE_CALLS)
        .replaceText("<worker_procedure>")
        .with(WORKER_PROCEDURE_LOCATION)
        .replaceText("<failing_worker_procedure>")
        .with(FAILING_WORKER_PROCEDURE_LOCATION)
        .execute();
  }

  private static void setUpTaskReactorApi() {
    String pathToTaskReactorSqlFile =
        "src/main/resources/native-connectors-sdk-components/task_reactor.sql";

    SnowsqlFileExecutor.from(pathToTaskReactorSqlFile)
        .usingConnection(SNOWFLAKE_CONNECTION)
        .usingDatabase(DATABASE_NAME)
        .replaceText("CREATE OR ALTER VERSIONED SCHEMA")
        .with("CREATE SCHEMA IF NOT EXISTS")
        .replaceText("IMPORTS = ('/connectors-native-sdk.jar')")
        .with(format("IMPORTS = ('@%s.%s/%s')", STAGE_SCHEMA, STAGE_NAME, JAR_NAME))
        .execute();
  }
}

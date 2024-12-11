/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static java.lang.String.format;

import com.snowflake.connectors.BaseTest;
import com.snowflake.connectors.taskreactor.utils.GradleUtils;
import com.snowflake.connectors.taskreactor.utils.snowflakecli.SnowflakeCliFileExecutionBuilder;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public class BaseTaskReactorIntegrationTest extends BaseTest {

  public static final String TASK_REACTOR_TEST_SCHEMA = "TASK_REACTOR_TESTS";
  public static final String WORKER_PROCEDURE_LOCATION =
      TASK_REACTOR_TEST_SCHEMA + ".WORKER_PROCEDURE";
  public static final String WORK_SELECTOR_LOCATION = TASK_REACTOR_TEST_SCHEMA + ".WORK_SELECTOR";
  public static final String EXPIRED_WORK_SELECTOR_LOCATION =
      TASK_REACTOR_TEST_SCHEMA + ".EMPTY_EXPIRED_WORK_SELECTOR";
  private static final String STAGE_NAME = "TASK_REACTOR_STAGE";
  private static final String STAGE_SCHEMA = "STAGE_SCHEMA";
  private static final String JAR_NAME = GradleUtils.getSdkJarName();
  protected static final String FAILING_WORKER_PROCEDURE_LOCATION =
      TASK_REACTOR_TEST_SCHEMA + ".FAILING_WORKER_PROCEDURE";
  protected static final String WORKER_PROCEDURE_CALLS =
      TASK_REACTOR_TEST_SCHEMA + ".WORKER_PROCEDURE_CALLS";
  protected static final String TEST_INSTANCE = "TEST_INSTANCE";

  @BeforeAll
  void baseTaskReactorIntegrationBeforeAll(@TempDir File tempDir) throws Exception {
    createStage();
    setUpTaskReactorApi(tempDir);
    createTestObjects(tempDir);
  }

  @BeforeEach
  void baseTaskReactorIntegrationBeforeEach() {
    session.sql("DELETE FROM " + WORKER_PROCEDURE_CALLS).collect();
  }

  protected static File currentDir() {
    return new File(System.getProperty("user.dir"));
  }

  private void createStage() {
    session.sql(format("CREATE OR REPLACE SCHEMA %s", STAGE_SCHEMA)).collect();
    session.sql(format("CREATE OR REPLACE STAGE %s.%s", STAGE_SCHEMA, STAGE_NAME)).collect();
    session
        .file()
        .put(
            format("file://%s/build/libs/%s", currentDir().getAbsolutePath(), JAR_NAME),
            format("@%s", STAGE_NAME),
            Map.of("AUTO_COMPRESS", "FALSE"));
  }

  private void createTestObjects(File tempDir) throws IOException {
    String configFile = System.getProperty("configurationFile");
    String pathToSetupFile =
        ClassLoader.getSystemClassLoader().getResource("task_reactor_tests_setup.sql").getPath();

    SnowflakeCliFileExecutionBuilder.forFile(pathToSetupFile)
        .usingConnection(configFile)
        .usingDatabase(DATABASE_NAME)
        .withExecutionFileDir(tempDir)
        .replaceText("<test_schema>", TASK_REACTOR_TEST_SCHEMA)
        .replaceText("<work_selector>", WORK_SELECTOR_LOCATION)
        .replaceText("<expired_work_selector>", EXPIRED_WORK_SELECTOR_LOCATION)
        .replaceText("<worker_procedure_calls>", WORKER_PROCEDURE_CALLS)
        .replaceText("<worker_procedure>", WORKER_PROCEDURE_LOCATION)
        .replaceText("<failing_worker_procedure>", FAILING_WORKER_PROCEDURE_LOCATION)
        .execute();
  }

  private void setUpTaskReactorApi(File tempDir) throws IOException {
    String configFile = System.getProperty("configurationFile");
    String pathToTaskReactorSqlFile =
        "src/main/resources/connectors-sdk-components/task_reactor.sql";

    SnowflakeCliFileExecutionBuilder.forFile(pathToTaskReactorSqlFile)
        .usingConnection(configFile)
        .usingDatabase(DATABASE_NAME)
        .withExecutionFileDir(tempDir)
        .replaceText("CREATE OR ALTER VERSIONED SCHEMA", "CREATE SCHEMA IF NOT EXISTS")
        .replaceText(
            "IMPORTS = ('@java_jar@')",
            format("IMPORTS = ('@%s.%s/%s')", STAGE_SCHEMA, STAGE_NAME, JAR_NAME))
        .replaceText("@snowpark@", System.getProperty("snowpark"))
        .replaceText("@telemetry@", System.getProperty("telemetry"))
        .execute();
  }
}

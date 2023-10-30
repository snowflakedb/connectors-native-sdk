package com.snowflake.connectors.sdk.example_push_based_java_connector;

import com.snowflake.snowpark_java.Session;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseConnectorTest {

  static Logger logger = LoggerFactory.getLogger(ExamplePushBasedJavaConnectorTest.class);
  static String APP_NAME = "EXAMPLE_PUSH_BASED_CONNECTOR_INT_TEST";
  static String DATABASE_NAME = APP_NAME + "_INSTANCE";
  static String DESTINATION_DATABASE = APP_NAME + "_DATA";

  static ObjectsInitializer objectsInitializer;
  static IngestionService ingestionService;
  static IntTestDataSource dataSource;

  static Session session;

  @BeforeAll
  static void setup() throws IOException {
    var currentDir = currentDir();
    buildConnector(currentDir);
    deployConnector(currentDir);
    installConnector(currentDir);

    configureObjectsInitializer();
    configureIngestionService();
    dataSource = new IntTestDataSource();

    objectsInitializer.initDestinationDatabase();

    configureSnowparkSession();
    session.sql("USE DATABASE " + DESTINATION_DATABASE).collect();
    session.sql("USE SCHEMA PUBLIC").collect();
  }

  @AfterAll
  static void cleanup() {
    session.sql("DROP APPLICATION " + DATABASE_NAME + " CASCADE").collect();
    session.sql("DROP DATABASE IF EXISTS " + APP_NAME + "_STAGE").collect();
    session.sql("DROP APPLICATION PACKAGE IF EXISTS " + APP_NAME).collect();
  }

  private static void buildConnector(File currentDir) {
    logger.info("Building connector in {}", currentDir);
    var exitStatus = runCommand("make build APP_NAME=" + APP_NAME, currentDir);
    assert exitStatus == 0;
  }

  private static void deployConnector(File currentDir) {
    logger.info("Deploying connector...");
    var exitStatus = runCommand("make deploy APP_NAME=" + APP_NAME, currentDir);
    assert exitStatus == 0;
  }

  private static void installConnector(File currentDir) {
    logger.info("Installing connector...");
    var exitStatus = runCommand("make install APP_NAME=" + APP_NAME, currentDir);
    assert exitStatus == 0;
  }

  private static File currentDir() {
    String currentDir = System.getProperty("user.dir");
    if (currentDir.endsWith("/integration-test")) {
      currentDir = currentDir.substring(0, currentDir.lastIndexOf("/integration-test"));
    }
    return new File(currentDir);
  }

  private static int runCommand(String command, File workingDir) {
    try {
      var process = new ProcessBuilder(command.split(" ")).directory(workingDir).start();
      var reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
      }
      return process.waitFor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void configureObjectsInitializer() throws IOException {
    Properties properties = new Properties();
    properties.load(BaseConnectorTest.class.getResourceAsStream("/connector.properties"));

    Properties jdbcProperties =
        new Properties() {
          {
            put("account", properties.getProperty("account"));
            put("user", properties.getProperty("user"));
            put("password", properties.getProperty("jdbc.password"));
            put("role", properties.getProperty("role"));
            put("warehouse", properties.getProperty("warehouse"));
            put("database", DATABASE_NAME);
            put("schema", properties.getProperty("native_application.schema_name"));
          }
        };

    objectsInitializer =
        new ObjectsInitializer(
            properties.getProperty("jdbc.url"), jdbcProperties, DESTINATION_DATABASE);
  }

  private static void configureIngestionService() throws IOException {
    Properties properties = new Properties();
    properties.load(BaseConnectorTest.class.getResourceAsStream("/connector.properties"));

    Properties ingestionProperties =
        new Properties() {
          {
            put("account", properties.getProperty("account"));
            put("user", properties.getProperty("user"));
            put("role", properties.getProperty("role"));
            put("warehouse", properties.getProperty("warehouse"));
            put("host", properties.getProperty("ingestion.host"));
            put("scheme", properties.getProperty("ingestion.scheme"));
            put("port", properties.getProperty("ingestion.port"));
            put("private_key", properties.getProperty("ingestion.private_key"));
          }
        };

    ingestionService =
        new IngestionService(
            ingestionProperties,
            DESTINATION_DATABASE,
            properties.getProperty("destination_database.schema_name"));
  }

  private static void configureSnowparkSession() throws IOException {
    Properties properties = new Properties();
    properties.load(BaseConnectorTest.class.getResourceAsStream("/connector.properties"));

    Map<String, String> config =
        new HashMap<>() {
          {
            put("url", properties.getProperty("ingestion.host"));
            put("user", properties.getProperty("user"));
            put("password", properties.getProperty("jdbc.password"));
            put("role", properties.getProperty("role"));
            put("warehouse", properties.getProperty("warehouse"));
            put("account", properties.getProperty("account"));
          }
        };
    session = Session.builder().configs(config).create();
  }
}

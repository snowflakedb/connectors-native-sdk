/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_push_based_java_connector;

import static java.lang.Integer.parseInt;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

public class ExamplePushBasedJavaConnector {

  public static void main(String[] args) throws Exception {
    System.setProperty(
        "org.slf4j.simpleLogger.defaultLogLevel",
        "ERROR"); // to disable info logs from snowflake-ingest-sdk

    ResourceScheduler resourceScheduler = buildService();

    Scanner scanner = new Scanner(System.in);

    while (true) {
      System.out.println("\nEnable/Disable resource using commands:");
      System.out.println("> enable <resource_name>");
      System.out.println("> disable <resource_name>");
      System.out.println("To quit application use:");
      System.out.println("> quit");
      String command = scanner.nextLine();
      String[] commandSplit = command.split(" ");

      String commandType = commandSplit[0];
      if ("enable".equalsIgnoreCase(commandType)) {
        String resource = commandSplit[1];
        resourceScheduler.enableResource(resource);
      } else if ("disable".equalsIgnoreCase(commandType)) {
        String resource = commandSplit[1];
        System.out.println("Disabling " + resource);
        resourceScheduler.disableResource(resource);
      } else if ("quit".equalsIgnoreCase(commandType)) {
        System.out.println("Quiting");
        resourceScheduler.shutdown();
        break;
      } else {
        System.out.println("Invalid command");
      }
    }
  }

  private static ResourceScheduler buildService() throws IOException {
    Properties properties = new Properties();
    properties.load(
        ExamplePushBasedJavaConnector.class.getResourceAsStream("/connector.properties"));

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

    Properties jdbcProperties =
        new Properties() {
          {
            put("account", properties.getProperty("account"));
            put("user", properties.getProperty("user"));
            put("role", properties.getProperty("role"));
            put("warehouse", properties.getProperty("warehouse"));
            put("database", properties.getProperty("native_application.database_name"));
            put("schema", properties.getProperty("native_application.schema_name"));
            put("password", properties.getProperty("jdbc.password"));
          }
        };

    var ingestionService =
        new IngestionService(
            ingestionProperties,
            properties.getProperty("destination_database.database_name"),
            properties.getProperty("destination_database.schema_name"));
    var tableCreator =
        new ObjectsInitializer(
            properties.getProperty("jdbc.url"),
            jdbcProperties,
            properties.getProperty("destination_database.database_name"));

    return new ResourceScheduler(
        tableCreator,
        ingestionService,
        parseInt(properties.getProperty("upload.initial.record-count")),
        parseInt(properties.getProperty("upload.periodical.record-count")),
        parseInt(properties.getProperty("upload.periodical.interval-seconds")),
        parseInt(properties.getProperty("upload.scheduler.pool-size")));
  }
}

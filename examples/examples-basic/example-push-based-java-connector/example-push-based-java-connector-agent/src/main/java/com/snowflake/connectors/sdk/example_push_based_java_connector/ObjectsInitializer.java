/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_push_based_java_connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

class ObjectsInitializer {

  private final Properties snowflakeJdbcProperties;
  private final String jdbcUrl;
  private final String destinationDatabase;

  ObjectsInitializer(
      String jdbcUrl, Properties snowflakeJdbcProperties, String destinationDatabase) {
    this.jdbcUrl = jdbcUrl;
    this.snowflakeJdbcProperties = snowflakeJdbcProperties;
    this.destinationDatabase = destinationDatabase;
  }

  void initDestinationDatabase() {
    callProcedure("CALL INIT_DESTINATION_DATABASE(?)", destinationDatabase);
  }

  void initResource(String resourceName) {
    callProcedure("CALL INIT_RESOURCE(?, ?)", resourceName, destinationDatabase);
  }

  private void callProcedure(String command, String... parameters) {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, snowflakeJdbcProperties)) {
      PreparedStatement statement = connection.prepareStatement(command);
      for (int i = 0; i < parameters.length; i++) {
        statement.setString(i + 1, parameters[i]);
      }

      ResultSet resultSet = statement.executeQuery();
      resultSet.next();
      System.out.println(resultSet.getString(1));
      statement.close();
    } catch (SQLException throwable) {
      throw new RuntimeException(throwable);
    }
  }
}

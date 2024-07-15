/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import com.snowflake.snowpark_java.Session;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class SnowparkSessionProvider {

  static Session createSession() throws IOException {
    var connectionProperties = loadConnectionProperties();
    return Session.builder().configs(connectionProperties).create();
  }

  private static Map<String, String> loadConnectionProperties() throws IOException {
    var credentialsFile = new File(System.getProperty("snowflakeCredentials"));
    Properties properties = new Properties();
    try (FileInputStream f = new FileInputStream(credentialsFile)) {
      properties.load(f);
      return new HashMap<>() {
        {
          put("url", properties.getProperty("host"));
          put("role", properties.getProperty("rolename"));
          put("user", properties.getProperty("user"));
          put("password", properties.getProperty("password"));
          put("account", properties.getProperty("accountname"));
          put("warehouse", properties.getProperty("warehousename"));
        }
      };
    }
  }
}

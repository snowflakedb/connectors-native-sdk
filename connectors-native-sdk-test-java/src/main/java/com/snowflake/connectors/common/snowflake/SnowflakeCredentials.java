/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.snowflake;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Utility for parsing a snowflake credentials file. */
public class SnowflakeCredentials {

  /**
   * Extracts session properties from a snowflake credentials file.
   *
   * <p>Credentials file path must be specified using the {@code snowflakeCredentials} system
   * property.
   *
   * @return session properties extracted from the credentials file
   * @throws IOException if properties from the specified credentials file cannot be loaded
   */
  public static Map<String, String> sessionConfigFromFile() throws IOException {
    File credentialsFile = new File(System.getProperty("snowflakeCredentials"));
    Properties props = new Properties();

    try (FileInputStream input = new FileInputStream(credentialsFile)) {
      props.load(input);

      Map<String, String> sessionConfig = new HashMap<>();
      sessionConfig.put(
          "URL", props.get("protocol") + "://" + props.get("host") + ":" + props.get("port"));
      sessionConfig.put("USER", props.get("user").toString());
      sessionConfig.put("PASSWORD", props.get("password").toString());
      sessionConfig.put("ROLE", props.get("rolename").toString());
      sessionConfig.put("ACCOUNT", props.get("accountname").toString());
      sessionConfig.put("WAREHOUSE", props.get("warehousename").toString());
      return sessionConfig;
    }
  }
}

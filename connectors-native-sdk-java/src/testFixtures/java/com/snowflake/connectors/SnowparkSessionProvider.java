/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import static java.lang.String.format;

import com.snowflake.snowpark_java.Session;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;

public class SnowparkSessionProvider {

  public static Session createSession() throws IOException {
    var connectionProperties = loadConnectionProperties();
    return Session.builder().configs(connectionProperties).create();
  }

  private static Map<String, String> loadConnectionProperties() throws IOException {
    var credentialsFile = Paths.get(System.getProperty("configurationFile"));
    var properties = Toml.parse(credentialsFile);
    var connectionName = properties.getString("default_connection_name");

    return new HashMap<>() {
      {
        put("url", extractOrGetEnv(properties, connectionName, "host"));
        put("role", extractOrGetEnv(properties, connectionName, "role"));
        put("user", extractOrGetEnv(properties, connectionName, "user"));
        put("password", extractOrGetEnv(properties, connectionName, "password"));
        put("account", extractOrGetEnv(properties, connectionName, "account"));
        put("warehouse", extractOrGetEnv(properties, connectionName, "warehouse"));
      }
    };
  }

  private static String extractOrGetEnv(
      TomlParseResult properties, String connectionName, String property) {
    return Optional.ofNullable(
            properties.getString(format("connections.%s.%s", connectionName, property)))
        .orElse(
            System.getenv(
                format(
                    "SNOWFLAKE_CONNECTIONS_%s_%s",
                    connectionName.toUpperCase(), property.toUpperCase())));
  }
}

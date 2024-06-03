/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils;

import static java.util.Objects.nonNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

// TODO("CONN-7402")
public class SnowsqlDockerConfigurator {

  private static final String PATH_TO_CREDENTIALS = "../.env/snowflake_credentials";

  public static void configureSnowsqlInDocker() {
    if (nonNull(System.getenv("SDK_HOME"))) {
      System.out.printf(
          "Configuring SnowSQL with credentials from file [%s].", PATH_TO_CREDENTIALS);
      String homePath = System.getenv().getOrDefault("WORKSPACE", System.getenv("HOME"));
      Path snowsqlDirectoryPath = Path.of(homePath, "/.snowsql");
      new File(snowsqlDirectoryPath.toString()).mkdirs();
      try {
        Files.copy(
            Path.of(PATH_TO_CREDENTIALS),
            Path.of(snowsqlDirectoryPath.toString(), "config"),
            StandardCopyOption.REPLACE_EXISTING);
        System.out.println("SnowSQL configured successfully.");
      } catch (IOException e) {
        System.out.println("SnowSQL configuration failed.");
        throw new RuntimeException(e);
      }
    }
  }
}

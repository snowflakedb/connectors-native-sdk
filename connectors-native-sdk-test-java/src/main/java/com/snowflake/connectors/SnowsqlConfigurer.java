/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowsqlConfigurer {

  private static final Logger LOG = LoggerFactory.getLogger(SnowsqlConfigurer.class);

  public static void configureSnowsqlInDocker() {
    if (System.getenv("SDK_HOME") != null) {
      String configName = "config";
      LOG.info("Configuring SnowSQL");
      var configFile = new File(System.getProperty("snowflakeCredentials"));

      var homePath = System.getenv().getOrDefault("WORKSPACE", System.getenv("HOME"));
      var snowsqlConfigPath = Path.of(homePath, "/.snowsql");
      new File(snowsqlConfigPath.toString()).mkdirs();
      try {
        Path pathAfterCopying =
            Files.copy(
                configFile.toPath(),
                Path.of(snowsqlConfigPath.toString(), configName),
                StandardCopyOption.REPLACE_EXISTING);
        LOG.info(
            "Config path: {}, config file exists: {}",
            pathAfterCopying,
            pathAfterCopying.toFile().exists());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

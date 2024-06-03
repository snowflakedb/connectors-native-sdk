/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class GradleUtils {
  public static String getSdkJarName() {
    String version;
    try {
      version = getVersionFromGradle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return "connectors-native-sdk-" + version + ".jar";
  }

  static String getVersionFromGradle() throws IOException {
    List<String> lines = Files.readAllLines(Paths.get("build.gradle"));
    String versionInQuotes =
        lines.stream()
            .filter(line -> line.startsWith("version "))
            .map(line -> line.split(" ")[1])
            .toArray()[0]
            .toString();
    return versionInQuotes.replaceAll("[\"']", "");
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.gradle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class GradleUtils {
  static String getSdkJarName() throws IOException {
    String version = getVersionFromGradle();
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

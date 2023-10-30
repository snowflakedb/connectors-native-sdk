package com.snowflake.connectors.sdk.examples.example_github_connector;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

class BuildingHelper {

  static int runCommand(String command, File workingDir) {
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
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

// TODO("CONN-7402")
public class CommandLineHelper {
  public static int runCommand(String command, File workingDir)
      throws IOException, InterruptedException {
    Process process = new ProcessBuilder(command.split(" ")).directory(workingDir).start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

    String line;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
    }

    return process.waitFor();
  }
}

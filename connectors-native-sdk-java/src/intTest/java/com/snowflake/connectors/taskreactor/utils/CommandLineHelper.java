/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO("CONN-7402")
public class CommandLineHelper {
  private static final Logger LOG = LoggerFactory.getLogger(CommandLineHelper.class);

  public static int runCommand(String command, File workingDir)
      throws IOException, InterruptedException {
    Process process = new ProcessBuilder(command.split(" ")).directory(workingDir).start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

    String line;
    while ((line = reader.readLine()) != null) {
      LOG.info(line);
    }

    return process.waitFor();
  }
}

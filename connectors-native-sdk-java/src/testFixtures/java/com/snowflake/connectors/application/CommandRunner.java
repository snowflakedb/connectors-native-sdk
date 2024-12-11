/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application;

import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandRunner {

  private static final Logger LOG = LoggerFactory.getLogger(CommandRunner.class);

  public static String runCommand(String command, File workingDir) {
    LOG.info("Running '{}' in dir: '{}'}", command, workingDir.getAbsolutePath());
    try {
      var process =
          new ProcessBuilder("/bin/sh", "-c", command)
              .directory(workingDir)
              .redirectErrorStream(true)
              .start();
      var reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

      StringBuilder processOutput = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        LOG.info(line);
        processOutput.append(line);
      }
      int status = process.waitFor();

      assert status == 0 : format("Command: '%s', failed.", command);
      return processOutput.toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

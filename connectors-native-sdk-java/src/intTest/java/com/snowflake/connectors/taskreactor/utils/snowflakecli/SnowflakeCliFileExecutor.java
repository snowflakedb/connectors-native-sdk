/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils.snowflakecli;

import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class SnowflakeCliFileExecutor {

  private static final String FILE_EXECUTION_ERROR_MSG =
      "SQL Compilation Error occurred while executing queries from file. Check logs to find out the"
          + " root cause.";
  public static final String SQL_COMPILATION_ERROR_TRIGGER = "SQL compilation error:";

  private final String pathToSourceFile;
  private final String targetDatabase;
  private final String configurationFile;
  private final Map<String, String> replacers;

  SnowflakeCliFileExecutor(
      String pathToSourceFile,
      String targetDatabase,
      String configurationFile,
      Map<String, String> replacers) {
    this.pathToSourceFile = pathToSourceFile;
    this.targetDatabase = targetDatabase;
    this.configurationFile = configurationFile;
    this.replacers = replacers;
  }

  public void execute(File executionFileDir) throws IOException {
    var executeFile = new File(executionFileDir, "execute-" + UUID.randomUUID() + ".sql");
    var replacedLines =
        Files.readAllLines(Paths.get(pathToSourceFile)).stream()
            .map(this::replaceLine)
            .collect(Collectors.toList());

    try (var fileWriter = new BufferedWriter(new FileWriter(executeFile))) {
      if (targetDatabase != null) {
        fileWriter.write(format("USE DATABASE %s;", targetDatabase));
        fileWriter.newLine();
      }

      for (String line : replacedLines) {
        fileWriter.write(line);
        fileWriter.newLine();
      }
    }

    try {
      executeFileWithSnowflakeCli(executeFile);
    } finally {
      executeFile.delete();
    }
  }

  private void executeFileWithSnowflakeCli(File executeFile) {
    try {
      boolean executionFailed = false;
      var command =
          new ProcessBuilder(
                  "snow",
                  "--config-file",
                  this.configurationFile,
                  "sql",
                  "-f",
                  executeFile.getAbsolutePath())
              .redirectErrorStream(true)
              .start();

      try (var reader = new BufferedReader(new InputStreamReader(command.getInputStream()))) {
        String logLine;

        while ((logLine = reader.readLine()) != null) {
          if (logLine.contains(SQL_COMPILATION_ERROR_TRIGGER)) {
            executionFailed = true;
          }

          System.out.printf("[SNOWFLAKE CLI]: %s%n", logLine);
        }

        if (executionFailed) {
          throw new RuntimeException(FILE_EXECUTION_ERROR_MSG);
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private String replaceLine(String line) {
    String replacedLine = line;

    for (Map.Entry<String, String> entry : replacers.entrySet()) {
      replacedLine = replacedLine.replace(entry.getKey(), entry.getValue());
    }

    return replacedLine;
  }
}

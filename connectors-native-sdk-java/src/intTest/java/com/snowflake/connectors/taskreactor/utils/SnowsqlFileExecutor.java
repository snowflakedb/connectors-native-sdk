/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class SnowsqlFileExecutor {

  private static final String FILE_EXECUTION_ERROR_MSG =
      "SQL Compilation Error occurred while executing queries from file. Check logs to find out the"
          + " root cause.";
  public static final String SQL_COMPILATION_ERROR_TRIGGER = "SQL compilation error:";
  private static final String EXECUTE_FILE_PATH = "execute.sql";
  private final String pathToSourceFile;
  private final String targetDatabase;
  private final String snowsqlConnection;
  private final Map<String, String> replacers;

  private SnowsqlFileExecutor(
      String pathToSourceFile,
      String targetDatabase,
      String snowsqlConnection,
      Map<String, String> replacers) {
    this.pathToSourceFile = pathToSourceFile;
    this.targetDatabase = targetDatabase;
    this.snowsqlConnection = snowsqlConnection;
    this.replacers = replacers;
  }

  public static SnowsqlFileExecutorManager from(String filePath) {
    return new SnowsqlFileExecutorManager(filePath);
  }

  public void execute() {
    try (var fileReader = new BufferedReader(new FileReader(this.pathToSourceFile));
        var fileWriter = new BufferedWriter(new FileWriter(EXECUTE_FILE_PATH)); ) {
      useDatabase(fileWriter);
      prepareFileContentWithReplacements(fileWriter, fileReader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    executeFileWithSnowsql();
    deleteExecuteFile();
  }

  private void useDatabase(BufferedWriter fileWriter) throws IOException {
    if (nonNull(targetDatabase)) {
      fileWriter.append(format("USE DATABASE %s;", targetDatabase));
      fileWriter.newLine();
    }
  }

  private void prepareFileContentWithReplacements(
      BufferedWriter fileWriter, BufferedReader fileReader) throws IOException {
    String fileLine;
    while ((fileLine = fileReader.readLine()) != null) {
      for (Map.Entry<String, String> e : replacers.entrySet()) {
        fileLine = fileLine.replace(e.getKey(), e.getValue());
      }
      fileWriter.append(fileLine);
      fileWriter.newLine();
    }
  }

  private void executeFileWithSnowsql() {
    try {
      boolean executionFailed = false;
      var command =
          new ProcessBuilder("snowsql", "-c", this.snowsqlConnection, "-f", EXECUTE_FILE_PATH)
              .redirectErrorStream(true)
              .start();
      var reader = new BufferedReader(new InputStreamReader(command.getInputStream()));
      String logLine;
      while ((logLine = reader.readLine()) != null) {
        if (logLine.contains(SQL_COMPILATION_ERROR_TRIGGER)) {
          executionFailed = true;
        }
        System.out.printf("[SNOWSQL]: %s%n", logLine);
      }
      reader.close();
      if (executionFailed) {
        throw new RuntimeException(FILE_EXECUTION_ERROR_MSG);
      }
    } catch (Exception e) {
      deleteExecuteFile();
      throw new RuntimeException(e);
    }
  }

  private void deleteExecuteFile() {
    new File(EXECUTE_FILE_PATH).delete();
  }

  public static class SnowsqlFileExecutorManager {
    private final String pathToSourceFile;
    private String snowsqlConnection;
    private String targetDatabase;
    private final Map<String, String> replacers = new HashMap<>();

    public SnowsqlFileExecutorManager(String pathToSourceFile) {
      this.pathToSourceFile = pathToSourceFile;
    }

    public void execute() {
      var snowsqlFileExecutor =
          new SnowsqlFileExecutor(
              pathToSourceFile, targetDatabase, requireNonNull(snowsqlConnection), replacers);
      snowsqlFileExecutor.execute();
    }

    public SnowsqlFileExecutorManager usingDatabase(String database) {
      this.targetDatabase = database;
      return this;
    }

    public SnowsqlFileExecutorManager usingConnection(String snowsqlConnection) {
      this.snowsqlConnection = snowsqlConnection;
      return this;
    }

    public TextReplacer replaceText(String text) {
      return new TextReplacer(this, text);
    }

    private void addReplacer(TextReplacer replacer) {
      this.replacers.put(replacer.textToReplace, replacer.replacement);
    }
  }

  public static class TextReplacer {
    private final SnowsqlFileExecutorManager builder;
    private final String textToReplace;
    private String replacement;

    public TextReplacer(SnowsqlFileExecutorManager builder, String textToReplace) {
      this.builder = builder;
      this.textToReplace = textToReplace;
    }

    public SnowsqlFileExecutorManager with(String replacement) {
      this.replacement = replacement;
      builder.addReplacer(this);
      return builder;
    }
  }
}

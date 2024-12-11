package com.snowflake.connectors.taskreactor.utils.snowflakecli;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SnowflakeCliFileExecutionBuilder {

  private final String pathToSourceFile;
  private final Map<String, String> replacers;

  private String configurationFile;
  private String targetDatabase;
  private File executionFileDir;

  private SnowflakeCliFileExecutionBuilder(String pathToSourceFile) {
    this.pathToSourceFile = pathToSourceFile;
    this.replacers = new HashMap<>();
  }

  public static SnowflakeCliFileExecutionBuilder forFile(String filePath) {
    return new SnowflakeCliFileExecutionBuilder(filePath);
  }

  public void execute() throws IOException {
    requireNonNull(configurationFile);
    requireNonNull(executionFileDir);

    new SnowflakeCliFileExecutor(pathToSourceFile, targetDatabase, configurationFile, replacers)
        .execute(executionFileDir);
  }

  public SnowflakeCliFileExecutionBuilder usingDatabase(String database) {
    this.targetDatabase = database;
    return this;
  }

  public SnowflakeCliFileExecutionBuilder usingConnection(String snowflakeCliConnection) {
    this.configurationFile = snowflakeCliConnection;
    return this;
  }

  public SnowflakeCliFileExecutionBuilder withExecutionFileDir(File executionFileDir) {
    this.executionFileDir = executionFileDir;
    return this;
  }

  public SnowflakeCliFileExecutionBuilder replaceText(String from, String to) {
    this.replacers.put(from, to);
    return this;
  }
}

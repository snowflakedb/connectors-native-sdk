/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application;

import com.snowflake.snowpark_java.Session;
import java.io.File;
import java.util.UUID;

public class BenchmarkApplication {

  private static final String RANDOM_SUFFIX = UUID.randomUUID().toString().replace('-', '_');
  private static final String APP_PACKAGE_NAME = "BENCHMARK_APP_" + RANDOM_SUFFIX;
  private static final String APP_DIR = "src/testApps/benchmark-app";

  public static void setupApplication() {
    Application.setupApplication(APP_DIR, APP_PACKAGE_NAME);
  }

  public static void dropApplicationPackage() {
    Application.dropApplicationPackage(APP_DIR, APP_PACKAGE_NAME);
  }

  public static Application createNewInstance(Session session) {
    return Application.createNewInstance(session, APP_PACKAGE_NAME);
  }

  public static File getAppDir() {
    return Application.getFileRelativeToProjectRoot(APP_DIR);
  }
}

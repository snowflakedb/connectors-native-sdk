/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import java.io.IOException;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

public class CustomTestExecutionListener implements TestExecutionListener {

  @Override
  public void testPlanExecutionStarted(TestPlan testPlan) {
    SnowsqlConfigurer.configureSnowsqlInDocker();
    try {
      Application.setupApplication();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testPlanExecutionFinished(TestPlan testPlan) {
    Application.dropApplicationPackage();
  }
}

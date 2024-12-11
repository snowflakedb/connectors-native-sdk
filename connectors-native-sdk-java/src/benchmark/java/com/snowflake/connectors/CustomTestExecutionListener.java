/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import com.snowflake.connectors.application.BenchmarkApplication;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

public class CustomTestExecutionListener implements TestExecutionListener {

  @Override
  public void testPlanExecutionStarted(TestPlan testPlan) {
    BenchmarkApplication.setupApplication();
  }

  @Override
  public void testPlanExecutionFinished(TestPlan testPlan) {
    BenchmarkApplication.dropApplicationPackage();
  }
}

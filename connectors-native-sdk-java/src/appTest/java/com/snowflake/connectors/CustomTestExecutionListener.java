/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import com.snowflake.connectors.application.TestNativeSdkApplication;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

public class CustomTestExecutionListener implements TestExecutionListener {

  @Override
  public void testPlanExecutionStarted(TestPlan testPlan) {
    TestNativeSdkApplication.setupApplication();
  }

  @Override
  public void testPlanExecutionFinished(TestPlan testPlan) {
    TestNativeSdkApplication.dropApplicationPackage();
  }
}

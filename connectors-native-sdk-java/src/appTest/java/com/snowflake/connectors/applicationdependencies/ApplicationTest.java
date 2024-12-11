/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.applicationdependencies;

import static com.snowflake.connectors.application.CommandRunner.runCommand;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.application.TestNativeSdkApplication;
import org.junit.jupiter.api.Test;

public class ApplicationTest {

  @Test
  void applicationTestsShouldSucceed() {
    String commandOutput = runCommand("./gradlew clean test", TestNativeSdkApplication.getAppDir());
    assertThat(commandOutput)
        .contains("BUILD SUCCESSFUL")
        .doesNotContain("BUILD FAILED")
        .doesNotContain(":test NO-SOURCE");
  }
}

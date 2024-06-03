/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.applicationdependencies;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.application.Application;
import org.junit.jupiter.api.Test;

public class ApplicationTest {

  @Test
  void applicationTestsShouldSucceed() {
    String commandOutput = Application.runCommand("./gradlew clean test", Application.getAppDir());
    assertThat(commandOutput)
        .contains("BUILD SUCCESSFUL")
        .doesNotContain("BUILD FAILED")
        .doesNotContain(":test NO-SOURCE");
  }
}

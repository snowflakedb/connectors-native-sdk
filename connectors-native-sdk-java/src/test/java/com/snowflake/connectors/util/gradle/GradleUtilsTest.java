/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.gradle;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;

public class GradleUtilsTest {

  @Test
  void shouldReadVersionAndBuildSdkJarName() throws IOException {
    // when
    String name = GradleUtils.getSdkJarName();
    // then
    assertThat(name).matches("^connectors-native-sdk-(?:\\d+\\.){2}\\d+(-SNAPSHOT)?.jar$");
  }
}

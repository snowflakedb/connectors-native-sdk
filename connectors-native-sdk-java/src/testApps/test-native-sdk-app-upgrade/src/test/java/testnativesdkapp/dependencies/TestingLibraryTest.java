/** Copyright (c) 2024 Snowflake Inc. */
package testnativesdkapp.dependencies;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.common.assertions.common.state.TestState;
import org.junit.jupiter.api.Test;

class TestingLibraryTest {

  @Test
  void testStateAssertFromTestingLibraryShouldWork() {
    // given
    TestState config = new TestState(true, "text value", 127);

    // then
    assertThat(config).hasFlag(true).hasIntState(127).hasStringState("text value");
  }
}

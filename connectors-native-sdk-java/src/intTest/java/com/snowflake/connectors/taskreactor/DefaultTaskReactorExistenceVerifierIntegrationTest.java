/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

// TODO make this test more readable (relying on extends clause is not easy to notice)
public class DefaultTaskReactorExistenceVerifierIntegrationTest {

  @Nested
  class NotExistingInstanceTest extends BaseTest {

    @Test
    void shouldReturnFalseWhenTaskRectorSchemaDoesNotExist() {
      // given
      TaskReactorExistenceVerifier verifier = TaskReactorExistenceVerifier.getInstance(session);

      // when
      boolean isConfigured = verifier.isTaskReactorConfigured();

      // then
      assertThat(isConfigured).isFalse();
    }
  }

  @Nested
  class ExistingInstanceTest extends BaseTaskReactorIntegrationTest {

    @Test
    void shouldReturnTrueWhenTaskRectorSchemaExists() {
      // given
      TaskReactorExistenceVerifier verifier = TaskReactorExistenceVerifier.getInstance(session);

      // when
      boolean isConfigured = verifier.isTaskReactorConfigured();

      // then
      assertThat(isConfigured).isTrue();
    }
  }
}

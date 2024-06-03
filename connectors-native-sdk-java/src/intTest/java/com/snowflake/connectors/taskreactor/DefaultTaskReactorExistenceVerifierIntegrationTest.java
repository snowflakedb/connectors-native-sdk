/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class DefaultTaskReactorExistenceVerifierIntegrationTest {

  @Nested
  class NotExistingInstanceTest extends BaseTest {

    TaskReactorExistenceVerifier verifier = TaskReactorExistenceVerifier.getInstance(session);

    @Test
    void shouldReturnFalseWhenTaskRectorSchemaDoesNotExist() {
      // when
      boolean isConfigured = verifier.isTaskReactorConfigured();

      // then
      assertThat(isConfigured).isFalse();
    }
  }

  @Nested
  class ExistingInstanceTest extends BaseTaskReactorIntegrationTest {

    TaskReactorExistenceVerifier verifier = TaskReactorExistenceVerifier.getInstance(session);

    @Test
    void shouldReturnTrueWhenTaskRectorSchemaExists() {
      // when
      boolean isConfigured = verifier.isTaskReactorConfigured();

      // then
      assertThat(isConfigured).isTrue();
    }
  }
}

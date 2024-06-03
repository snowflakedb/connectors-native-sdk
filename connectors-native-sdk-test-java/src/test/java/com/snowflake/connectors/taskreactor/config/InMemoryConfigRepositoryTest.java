/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import static com.snowflake.connectors.taskreactor.WorkSelectorType.PROCEDURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.util.variant.VariantMapperException;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class InMemoryConfigRepositoryTest {
  private final InMemoryConfigRepository configRepository = new InMemoryConfigRepository();

  @AfterEach
  void afterEach() {
    configRepository.clear();
  }

  @Test
  void shouldThrowExceptionWhenConfigIsEmpty() {
    // expect
    assertThatThrownBy(configRepository::getConfig).isInstanceOf(VariantMapperException.class);
  }

  @Test
  void shouldReturnConfig() {
    // given
    configRepository.updateConfig(
        Map.of(
            "SCHEMA", "schema-test",
            "WORKER_PROCEDURE", "TEST_PROC",
            "WORK_SELECTOR_TYPE", "PROCEDURE",
            "WORK_SELECTOR", "work-selector-test",
            "EXPIRED_WORK_SELECTOR", "expired-work-selector-test",
            "WAREHOUSE", "test-warehouse"));

    // when
    TaskReactorConfig result = configRepository.getConfig();

    // then
    assertThat(result)
        .extracting(
            TaskReactorConfig::schema,
            TaskReactorConfig::workerProcedure,
            TaskReactorConfig::workSelector,
            TaskReactorConfig::workSelectorType,
            TaskReactorConfig::expiredWorkSelector,
            TaskReactorConfig::warehouse)
        .containsExactly(
            "schema-test",
            "TEST_PROC",
            "work-selector-test",
            PROCEDURE,
            "expired-work-selector-test",
            "test-warehouse");
  }

  @Test
  void shouldIgnoreUnknownAndReturnConfig() {
    // given
    configRepository.updateConfig(
        Map.of(
            "SCHEMA", "schema-test",
            "WORKER_PROCEDURE", "TEST_PROC",
            "WORK_SELECTOR_TYPE", "PROCEDURE",
            "WORK_SELECTOR", "work-selector-test",
            "WAREHOUSE", "test-warehouse",
            "EXPIRED_WORK_SELECTOR", "expired-work-selector-test",
            "UNKNOWN", "key"));

    // when
    TaskReactorConfig result = configRepository.getConfig();

    // then
    assertThat(result)
        .extracting(
            TaskReactorConfig::schema,
            TaskReactorConfig::workerProcedure,
            TaskReactorConfig::workSelector,
            TaskReactorConfig::workSelectorType,
            TaskReactorConfig::expiredWorkSelector,
            TaskReactorConfig::warehouse)
        .containsExactly(
            "schema-test",
            "TEST_PROC",
            "work-selector-test",
            PROCEDURE,
            "expired-work-selector-test",
            "test-warehouse");
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.connectors.common.table.KeyValue;
import com.snowflake.connectors.taskreactor.WorkSelectorType;
import com.snowflake.connectors.util.variant.VariantMapperException;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultConfigRepositoryTest {
  private final InMemoryDefaultKeyValueTable repository = new InMemoryDefaultKeyValueTable();
  private final ConfigRepository configRepository = new DefaultConfigRepository(repository);

  @BeforeEach
  void beforeEach() {
    repository.getRepository().clear();
  }

  @Test
  void shouldReturnCorrectlyMappedConfiguration() {
    // given
    repository.updateAll(
        List.of(
            new KeyValue("SCHEMA", new Variant("schema-test")),
            new KeyValue("WORKER_PROCEDURE", new Variant("worker-procedure-test")),
            new KeyValue("WORK_SELECTOR_TYPE", new Variant("PROCEDURE")),
            new KeyValue("WORK_SELECTOR", new Variant("work-selector-test")),
            new KeyValue("EXPIRED_WORK_SELECTOR", new Variant("expired-work-selector-test")),
            new KeyValue("WAREHOUSE", new Variant("warehouse-test"))));

    // when
    TaskReactorConfig result = configRepository.getConfig();

    assertThat(result)
        .extracting(
            TaskReactorConfig::schema,
            TaskReactorConfig::workerProcedure,
            TaskReactorConfig::workSelectorType,
            TaskReactorConfig::workSelector,
            TaskReactorConfig::expiredWorkSelector,
            TaskReactorConfig::warehouse)
        .containsExactly(
            "schema-test",
            "worker-procedure-test",
            WorkSelectorType.PROCEDURE,
            "work-selector-test",
            "expired-work-selector-test",
            "warehouse-test");
  }

  @Test
  void shouldThrowWneSelectorTypeIsUnknown() {
    // given
    repository.updateAll(
        List.of(
            new KeyValue("SCHEMA", new Variant("schema-test")),
            new KeyValue("WORKER_PROCEDURE", new Variant("worker-procedure-test")),
            new KeyValue("WORK_SELECTOR_TYPE", new Variant("NOT-SELECTOR-TYPE")),
            new KeyValue("WORK_SELECTOR", new Variant("work-selector-test")),
            new KeyValue("WAREHOUSE", new Variant("warehouse-test"))));

    // when
    Exception exception = catchException(configRepository::getConfig);

    assertThat(exception).isInstanceOf(VariantMapperException.class);
  }
}

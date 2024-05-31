/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.assertions.configurationRepository.TestConfig;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class DefaultConfigurationRepositoryTest extends BaseIntegrationTest {
  ConfigurationRepository repo = ConfigurationRepository.getInstance(session);

  @Test
  public void shouldStoreAndReadConfigurationBasedOnCustomClass() {
    // given
    TestConfig config = new TestConfig(true, "text value", 127);

    // when
    repo.update("my_config", config);
    Optional<TestConfig> result = repo.fetch("my_config", TestConfig.class);

    // then

    assertThat(result.get())
        .isTestConfigObject()
        .hasFlag(true)
        .hasStringConfig("text value")
        .hasIntConfig(127);
  }

  @Test
  public void shouldStoreAndReadStringConfiguration() {
    // given
    String config = "my text";

    // when
    repo.update("my_string_config", config);
    Optional<String> result = repo.fetch("my_string_config", String.class);

    // then
    assertThat(result).isPresent().hasValue(config);
  }

  @Test
  public void shouldStoreAndReadIntConfiguration() {
    // given
    Integer config = 12312;

    // when
    repo.update("my_int_config", config);
    Optional<Integer> result = repo.fetch("my_int_config", Integer.class);

    // then
    assertThat(result).isPresent().hasValue(config);
  }

  @Test
  public void shouldStoreAndReadVariantConfiguration() {
    // given
    Variant config = new Variant(Map.of("name", "John", "age", 18));

    // when
    repo.update("my_variant_config", config);
    Optional<Variant> result = repo.fetch("my_variant_config", Variant.class);

    // then
    assertThat(result).isPresent().hasValue(config);
  }

  @Test
  public void shouldReturnEmptyOptionalWhenConfigurationDoesNotExistFetch() {
    // when
    Optional<TestConfig> result = repo.fetch("not-existing-config", TestConfig.class);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  public void shouldReturnEmptyOptionalWhenConfigurationIsNull() {
    // given
    String config = null;

    // when
    repo.update("null_config", config);
    Optional<String> result = repo.fetch("null_config", String.class);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  public void shouldFetchAllConfigurationsOfDifferentTypes() {
    // given
    Integer intConfig = 12312;
    String strConfig = "my text";
    TestConfig complexConfig = new TestConfig(true, "text value", 127);

    // when
    repo.update("int-config", intConfig);
    repo.update("str-config", strConfig);
    repo.update("complex-config", complexConfig);
    ConfigurationMap configurationMap = repo.fetchAll();

    // then
    Optional<Integer> actualIntConfig = configurationMap.get("int-config", Integer.class);
    Optional<String> actualStrConfig = configurationMap.get("str-config", String.class);
    Optional<TestConfig> actualComplexConfig =
        configurationMap.get("complex-config", TestConfig.class);

    assertThat(actualIntConfig).isPresent().hasValue(intConfig);
    assertThat(actualStrConfig).isPresent().hasValue(strConfig);

    assertThat(actualComplexConfig.get())
        .isTestConfigObject()
        .hasFlag(true)
        .hasStringConfig("text value")
        .hasIntConfig(127);
  }

  @Test
  public void shouldReturnEmptyOptionalWhenConfigurationDoesNotExistFetchAll() {
    // when
    ConfigurationMap configurationMap = repo.fetchAll();
    Optional<TestConfig> result = configurationMap.get("not-existing-config", TestConfig.class);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  public void shouldUpdateUpdatedAtOnEveryConfigurationUpdate() {
    // given
    Integer intConfig = 12312;
    repo.update("int-config", intConfig);
    Timestamp oldUpdatedAt =
        session.sql("SELECT updated_at FROM state.app_config").collect()[0].getTimestamp(0);

    // when
    repo.update("int-config", 65432);
    Timestamp newUpdatedAt =
        session
            .sql("SELECT updated_at FROM state.app_config WHERE key = 'int-config'")
            .collect()[0]
            .getTimestamp(0);

    // then
    assertThat(oldUpdatedAt)
        .withFailMessage(
            "Expected updated_at_on to be not equal but was <%s> == <%s>",
            oldUpdatedAt, newUpdatedAt)
        .isNotEqualTo(newUpdatedAt);
  }

  @Test
  public void shouldDeleteConfiguration() {
    // given
    repo.update("config-to-delete", "string-value");

    // when
    repo.delete("config-to-delete");
    Optional<String> result = repo.fetch("config-to-delete", String.class);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  public void shouldNotFailWhenConfigurationToBeDeletedDoesNotExist() {
    assertThatCode(() -> repo.delete("config-that-does-not-exist")).doesNotThrowAnyException();
  }
}

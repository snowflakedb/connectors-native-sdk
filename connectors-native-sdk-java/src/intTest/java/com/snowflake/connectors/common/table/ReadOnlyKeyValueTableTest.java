/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.snowpark_java.Functions.col;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ReadOnlyKeyValueTableTest extends BaseIntegrationTest {

  private ReadOnlyKeyValueTable table;

  @BeforeAll
  void beforeAll() {
    table = new ReadOnlyKeyValueTable(session, "PUBLIC.APP_PROPERTIES");
  }

  @Test
  void shouldFetchValueForAKey() {
    // expect
    assertThat(table.fetch("display_name")).isEqualTo(new Variant("Connectors Java SDK"));
  }

  @Test
  void shouldThrowKeyNotFoundExceptionWhenKeyDoesNotExist() {
    // expect
    assertThatThrownBy(() -> table.fetch("non-existing-key"))
        .isInstanceOf(KeyNotFoundException.class)
        .hasMessage("Key not found in configuration: non-existing-key");
  }

  @Test
  void shouldFetchAllValues() {
    // expect
    assertThat(table.fetchAll().values()).containsOnly(new Variant("Connectors Java SDK"));
  }

  @Test
  void shouldThrowUnsupportedOperationExceptionWhenTryingToUpdateValue() {
    // expect
    assertThatThrownBy(() -> table.update("key", new Variant("value")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("ReadOnlyKeyValueTable does not allow to modify data!");
  }

  @Test
  void shouldFetchAllValuesForAFilter() {
    // given
    Column filter = col("KEY").equal_to(lit("display_name"));

    // expect
    assertThat(table.getAllWhere(filter)).containsOnly(new Variant("Connectors Java SDK"));
  }

  @Test
  void shouldThrowUnsupportedOperationExceptionWhenTryingToUpdateManyValues() {
    // expect
    assertThatThrownBy(() -> table.updateMany(List.of("id"), "key", new Variant("value")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("ReadOnlyKeyValueTable does not allow to modify data!");
  }

  @Test
  void shouldThrowUnsupportedOperationExceptionWhenTryingToUpdateAllValues() {
    // expect
    assertThatThrownBy(() -> table.updateAll(List.of(new KeyValue("id", new Variant("value")))))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("ReadOnlyKeyValueTable does not allow to modify data!");
  }

  @Test
  void shouldThrowUnsupportedOperationExceptionWhenTryingToDeleteValue() {
    // expect
    assertThatThrownBy(() -> table.delete("key"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("ReadOnlyKeyValueTable does not allow to modify data!");
  }
}

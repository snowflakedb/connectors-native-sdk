/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.state.DefaultAppendOnlyStateRepository;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryDefaultAppendOnlyStateRepositoryTest {

  private AppendOnlyTable table;

  @BeforeEach
  void setup() {
    table = new InMemoryAppendOnlyKeyValueTable();
  }

  @Test
  void shouldStoreAndReadConfigurationBasedOnCustomClass() {
    // given
    var repo = new DefaultAppendOnlyStateRepository<>(table, MyTestState.class);
    var config = new MyTestState(true, "text value", 127);

    // when
    repo.insert("my_config", config);
    var result = repo.fetch("my_config");

    // then
    assertThat(result).isEqualTo(new MyTestState(true, "text value", 127));
  }

  @Test
  void shouldStoreAndReadConfigurationBasedOnVariantClass() {
    // given
    var repo = new DefaultAppendOnlyStateRepository<>(table, Variant.class);
    var variant = new Variant("{ \"state\": \"enabled\", \"age\": 18 }");

    // when
    repo.insert("my_variant", variant);
    var result = repo.fetch("my_variant");

    // then
    assertThat(result).isEqualTo(variant);
  }

  @Test
  void shouldStoreAndReadStringConfiguration() {
    // given
    var repo = new DefaultAppendOnlyStateRepository<>(table, String.class);
    var config = "my text";

    // when
    repo.insert("my_string_config", config);
    var result = repo.fetch("my_string_config");

    // then
    assertThat(result).isEqualTo("my text");
  }

  @Test
  void shouldStoreAndReadIntConfiguration() {
    // given
    var repo = new DefaultAppendOnlyStateRepository<>(table, Integer.class);
    var config = 12312;

    // when
    repo.insert("my_string_config", config);
    var result = repo.fetch("my_string_config");

    // then
    assertThat(result).isEqualTo(12312);
  }

  @Test
  void shouldStoreAndReadLatestConfigurationBasedOnCustomClass() {
    // given
    var repo = new DefaultAppendOnlyStateRepository<>(table, MyTestState.class);
    var config1 = new MyTestState(true, "text value one", 127);
    var config2 = new MyTestState(true, "text value two", 128);
    var config3 = new MyTestState(true, "text value three", 129);

    // when
    repo.insert("my_config", config1);
    repo.insert("my_other_config", config2);
    repo.insert("my_config", config3);
    var myResult = repo.fetch("my_config");
    var otherResult = repo.fetch("my_other_config");

    // then
    assertThat(myResult).isEqualTo(new MyTestState(true, "text value three", 129));
    assertThat(otherResult).isEqualTo(new MyTestState(true, "text value two", 128));
  }

  public static class MyTestState {

    public boolean flag;
    public String stringState;
    public int intState;

    public MyTestState() {}

    public MyTestState(boolean flag, String stringState, int intState) {
      this.flag = flag;
      this.stringState = stringState;
      this.intState = intState;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MyTestState that = (MyTestState) o;
      return flag == that.flag
          && intState == that.intState
          && Objects.equals(stringState, that.stringState);
    }

    @Override
    public int hashCode() {
      return Objects.hash(flag, stringState, intState);
    }
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common.object;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.assertions.NativeSdkAssertions;
import com.snowflake.connectors.common.object.SchemaName;
import org.assertj.core.api.AbstractObjectAssert;

/** AssertJ based assertions for {@link SchemaName}. */
public class SchemaNameAssert extends AbstractObjectAssert<SchemaNameAssert, SchemaName> {

  /**
   * Creates a new {@link SchemaNameAssert}.
   *
   * @param schemaName asserted schema name
   * @param selfType self type
   */
  public SchemaNameAssert(SchemaName schemaName, Class<SchemaNameAssert> selfType) {
    super(schemaName, selfType);
  }

  /**
   * Asserts that this schema name has a value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public SchemaNameAssert hasValue(String value) {
    assertThat(actual.getValue()).isEqualTo(value);
    return this;
  }

  /**
   * Asserts that this schema name has a database value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public SchemaNameAssert hasDatabaseValue(String value) {
    if (value == null) {
      assertThat(actual.getDatabase()).isNotPresent();
    } else {
      assertThat(actual.getDatabase()).isPresent().get(IDENTIFIER).hasValue(value);
    }

    return this;
  }

  /**
   * Asserts that this schema name has a schema value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public SchemaNameAssert hasSchemaValue(String value) {
    NativeSdkAssertions.assertThat(actual.getSchema()).hasValue(value);
    return this;
  }
}

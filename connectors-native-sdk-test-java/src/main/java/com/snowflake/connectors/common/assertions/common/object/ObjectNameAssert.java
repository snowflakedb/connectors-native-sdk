/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common.object;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.IDENTIFIER;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.SCHEMA_NAME;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.common.object.ObjectName;
import org.assertj.core.api.AbstractObjectAssert;

/** AssertJ based assertions for {@link ObjectName}. */
public class ObjectNameAssert extends AbstractObjectAssert<ObjectNameAssert, ObjectName> {

  public ObjectNameAssert(ObjectName objectName, Class<ObjectNameAssert> selfType) {
    super(objectName, selfType);
  }

  /**
   * Asserts that this object name has a value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public ObjectNameAssert hasValue(String value) {
    assertThat(actual.getValue()).isEqualTo(value);
    return this;
  }

  /**
   * Asserts that this object name has a database value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public ObjectNameAssert hasDatabaseValue(String value) {
    if (value == null) {
      assertThat(actual.getDatabase()).isNotPresent();
    } else {
      assertThat(actual.getDatabase()).isPresent().get(IDENTIFIER).hasValue(value);
    }

    return this;
  }

  /**
   * Asserts that this object name has a schema value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public ObjectNameAssert hasSchemaValue(String value) {
    if (value == null) {
      assertThat(actual.getSchema()).isNotPresent();
    } else {
      assertThat(actual.getSchema()).isPresent().get(IDENTIFIER).hasValue(value);
    }

    return this;
  }

  /**
   * Asserts that this object name has a schema name value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public ObjectNameAssert hasSchemaNameValue(String value) {
    if (value == null) {
      assertThat(actual.getSchemaName()).isNotPresent();
    } else {
      assertThat(actual.getSchemaName()).isPresent().get(SCHEMA_NAME).hasValue(value);
    }

    return this;
  }

  /**
   * Asserts that this object name has a name value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public ObjectNameAssert hasNameValue(String value) {
    assertThat(actual.getName()).hasValue(value);
    return this;
  }
}

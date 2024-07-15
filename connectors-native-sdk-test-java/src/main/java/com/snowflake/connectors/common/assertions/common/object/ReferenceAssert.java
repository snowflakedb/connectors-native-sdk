/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common.object;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.object.Reference;
import org.assertj.core.api.AbstractObjectAssert;

/** AssertJ based assertions for {@link Reference}. */
public class ReferenceAssert extends AbstractObjectAssert<ReferenceAssert, Reference> {

  public ReferenceAssert(Reference reference, Class<ReferenceAssert> selfType) {
    super(reference, selfType);
  }

  /**
   * Asserts that this reference has a name equal to the specified value.
   *
   * @param name expected name
   * @return this assertion
   */
  public ReferenceAssert hasName(String name) {
    assertThat(actual.getName()).isEqualTo(name);
    return this;
  }

  /**
   * Asserts that this reference has a value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public ReferenceAssert hasValue(String value) {
    assertThat(actual.getValue()).isEqualTo(value);
    return this;
  }
}

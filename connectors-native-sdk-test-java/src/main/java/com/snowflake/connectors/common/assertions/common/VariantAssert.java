/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.snowpark_java.types.Variant;
import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link Variant}. */
public class VariantAssert extends AbstractAssert<VariantAssert, Variant> {

  /**
   * Creates a new {@link VariantAssert}.
   *
   * @param variant asserted variant
   * @param selfType self type
   */
  public VariantAssert(Variant variant, Class<VariantAssert> selfType) {
    super(variant, selfType);
  }

  /**
   * Asserts that this variant, represented as a JSON string, is equal to the specified value.
   *
   * @param expected expected value
   * @return this assertion
   */
  public VariantAssert isEqualTo(Variant expected) {
    if (expected == null) {
      assertThat(actual).isNull();
    } else {
      assertThatJson(actual.asJsonString()).isEqualTo(expected.asJsonString());
    }

    return this;
  }
}

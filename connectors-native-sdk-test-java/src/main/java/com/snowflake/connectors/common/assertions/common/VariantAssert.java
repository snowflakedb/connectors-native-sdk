/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.snowpark_java.types.Variant;
import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link Variant}. */
public class VariantAssert extends AbstractAssert<VariantAssert, Variant> {

  public VariantAssert(Variant variant, Class<VariantAssert> selfType) {
    super(variant, selfType);
  }

  public VariantAssert isEqualTo(Variant expected) {
    if (expected == null) {
      assertThat(actual).isNull();
    } else {
      assertThatJson(actual.asJsonString()).isEqualTo(expected.asJsonString());
    }

    return this;
  }
}

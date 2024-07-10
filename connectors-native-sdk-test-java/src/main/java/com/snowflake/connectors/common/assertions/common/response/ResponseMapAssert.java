/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common.response;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.assertj.core.api.AbstractMapAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.MapAssert;

/** AssertJ based assertions for a map representing a variant response. */
public class ResponseMapAssert
    extends AbstractMapAssert<MapAssert<String, Variant>, Map<String, Variant>, String, Variant> {

  public ResponseMapAssert(Map<String, Variant> response, Class<ResponseMapAssert> selfType) {
    super(response, selfType);
  }

  private static String variantAsString(Variant variant) {
    return variant == null ? null : variant.asString();
  }

  /**
   * Asserts that this response has a value of {@code responseCode} field equal to {@code OK}.
   *
   * @return this assertion
   */
  public ResponseMapAssert hasOKResponseCode() {
    hasResponseCode("OK");
    return this;
  }

  public ResponseMapAssert hasResponseCode(String code) {
    assertThat(variantAsString(actual.get("response_code"))).isEqualTo(code);
    return this;
  }

  /**
   * Asserts that this response has a value of {@code message} field equal to the specified value.
   *
   * @param message expected field value
   * @return this assertion
   */
  public ResponseMapAssert hasMessage(String message) {
    assertThat(variantAsString(actual.get("message"))).isEqualTo(message);
    return this;
  }

  /**
   * Asserts that this response has a value of {@code message} field containing the specified value.
   *
   * @param message expected field value
   * @return this assertion
   */
  public ResponseMapAssert hasMessageContaining(String message) {
    assertThat(variantAsString(actual.get("message"))).contains(message);
    return this;
  }

  /**
   * Asserts that this response has a value of {@code id} field equal to the specified value.
   *
   * @param id expected field value
   * @return this assertion
   */
  public ResponseMapAssert hasId(String id) {
    assertThat(variantAsString(actual.get("id"))).isEqualTo(id);
    return this;
  }

  /**
   * Asserts that this response has a json value equal to the specified value.
   *
   * @param json expected json value
   * @return this assertion
   */
  public ResponseMapAssert isJson(String json) {
    assertThat(actual.toString()).isEqualTo(json);
    return this;
  }

  /**
   * Asserts that this response has a json field with the specified value
   *
   * @param fieldName name of the json field
   * @param expectedValue value of the field
   * @return this assertion
   */
  public ResponseMapAssert hasField(String fieldName, String expectedValue) {
    Assertions.assertThat(variantAsString(actual.get(fieldName))).isEqualTo(expectedValue);
    return this;
  }
}

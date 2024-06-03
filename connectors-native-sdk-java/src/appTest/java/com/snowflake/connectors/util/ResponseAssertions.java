/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util;

import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.assertj.core.api.Assertions;

public class ResponseAssertions {

  private final Map<String, Variant> response;

  public ResponseAssertions(Map<String, Variant> response) {
    this.response = response;
  }

  public static ResponseAssertions assertThat(Map<String, Variant> response) {
    return new ResponseAssertions(response);
  }

  public ResponseAssertions hasResponseCode(String expectedCode) {
    return hasField("response_code", expectedCode);
  }

  public ResponseAssertions hasOkResponseCode() {
    return hasResponseCode("OK");
  }

  public ResponseAssertions hasMessage(String expectedMessage) {
    return hasField("message", expectedMessage);
  }

  public ResponseAssertions hasId(String expectedId) {
    return hasField("id", expectedId);
  }

  public ResponseAssertions hasField(String fieldName, String expectedValue) {
    Assertions.assertThat(extractStringField(response, fieldName)).isEqualTo(expectedValue);
    return this;
  }

  private static String extractStringField(Map<String, Variant> response, String fieldName) {
    return variantAsString(response.get(fieldName));
  }

  private static String variantAsString(Variant variant) {
    return variant == null ? null : variant.asString();
  }
}

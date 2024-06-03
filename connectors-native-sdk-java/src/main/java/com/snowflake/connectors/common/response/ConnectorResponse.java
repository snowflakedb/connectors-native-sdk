/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.response;

import com.snowflake.snowpark_java.types.Variant;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A standard response format used by the connectors sdk, consisting of a response code, a message,
 * and a custom additional payload.
 *
 * <p>The message is optional in case of a successful response, otherwise it is obligatory.
 */
public class ConnectorResponse {

  private static final String OK_CODE = "OK";
  private static final String RESPONSE_CODE_FIELD_NAME = "response_code";
  private static final String MESSAGE_FIELD_NAME = "message";
  private final String responseCode;
  private final String message;
  private final Map<String, Variant> additionalPayload;

  /**
   * Creates a new {@link ConnectorResponse}, with the provided response code, message, and no
   * additional response payload.
   *
   * @param responseCode response code
   * @param message response message
   */
  public ConnectorResponse(String responseCode, String message) {
    this(responseCode, message, new HashMap<>());
  }

  /**
   * Creates a new {@link ConnectorResponse}, with the provided response code, message, and
   * additional response payload.
   *
   * @param responseCode response code
   * @param message response message
   * @param additionalPayload additional response payload
   */
  public ConnectorResponse(
      String responseCode, String message, Map<String, Variant> additionalPayload) {
    validateResponse(responseCode, message);
    this.responseCode = responseCode;
    this.message = message;
    this.additionalPayload = additionalPayload;
  }

  /**
   * Creates a new connector response instance, with response code of {@code OK}, no message, and no
   * additional payload.
   *
   * @return new successful connector response instance
   */
  public static ConnectorResponse success() {
    return new ConnectorResponse(OK_CODE, null);
  }

  /**
   * Creates a new response instance, with response code of {@code OK}, provided message, and no
   * additional payload.
   *
   * @param message response message
   * @return new successful response instance
   */
  public static ConnectorResponse success(String message) {
    return new ConnectorResponse(OK_CODE, message);
  }

  /**
   * Creates a new response instance, with response code of {@code OK}, provided message, and
   * additional payload.
   *
   * @param message response message
   * @param additionalPayload additional response payload
   * @return new successful response instance
   */
  public static ConnectorResponse success(String message, Map<String, Variant> additionalPayload) {
    return new ConnectorResponse(OK_CODE, message, additionalPayload);
  }

  /**
   * Creates a new error response instance, with provided response code, message, and no additional
   * payload.
   *
   * @param errorCode response code
   * @param errorMessage response message
   * @return new error response instance
   */
  public static ConnectorResponse error(String errorCode, String errorMessage) {
    return new ConnectorResponse(errorCode, errorMessage);
  }

  /**
   * Creates a new error response instance, with provided response code, message, and additional
   * payload.
   *
   * @param errorCode response code
   * @param errorMessage response message
   * @param additionalPayload additional response payload
   * @return new error response instance
   */
  public static ConnectorResponse error(
      String errorCode, String errorMessage, Map<String, Variant> additionalPayload) {
    return new ConnectorResponse(errorCode, errorMessage, additionalPayload);
  }

  /**
   * Creates a new error response instance by parsing the provided exception body.
   *
   * <p>Provided exception body must be a valid connector response, written as a JSON String.
   *
   * @param exceptionBody exception body
   * @return new error response instance
   * @throws InvalidConnectorResponseException if the provided String is not a valid response JSON
   *     String
   */
  public static ConnectorResponse fromException(String exceptionBody) {
    try {
      return deserializeVariant(new Variant(exceptionBody));
    } catch (UncheckedIOException exception) {
      throw new InvalidConnectorResponseException(exceptionBody + " is not a valid response body");
    }
  }

  /**
   * Creates a new error response instance from the provided Variant.
   *
   * <p>Provided Variant must be a valid connector response, containing all the necessary
   * properties.
   *
   * @param variant variant with the response
   * @return new error response instance
   * @throws InvalidConnectorResponseException if the provided Variant is not a valid response
   */
  public static ConnectorResponse fromVariant(Variant variant) {
    return deserializeVariant(variant);
  }

  private static ConnectorResponse deserializeVariant(Variant variant) {
    var variantAsMap = variant.asMap();
    return new ConnectorResponse(
        asStringOrNull(variantAsMap.get(RESPONSE_CODE_FIELD_NAME)),
        asStringOrNull(variantAsMap.get(MESSAGE_FIELD_NAME)),
        extractAdditionalPayload(variantAsMap));
  }

  private static Map<String, Variant> extractAdditionalPayload(Map<String, Variant> rawPayload) {
    return rawPayload.entrySet().stream()
        .filter(it -> !List.of(RESPONSE_CODE_FIELD_NAME, MESSAGE_FIELD_NAME).contains(it.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static String asStringOrNull(Variant variant) {
    return variant != null ? variant.asString() : null;
  }

  /**
   * Returns the response code.
   *
   * @return response code
   */
  public String getResponseCode() {
    return responseCode;
  }

  /**
   * Returns the response message.
   *
   * @return response message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Returns the additional response payload.
   *
   * @return additional response payload
   */
  public Map<String, Variant> getAdditionalPayload() {
    return this.additionalPayload;
  }

  /**
   * Returns the properties of this response in a Map.
   *
   * @return properties of this response in a Map
   */
  public Map<String, Variant> toMap() {
    var responseAsMap = new HashMap<String, Variant>();
    responseAsMap.put(RESPONSE_CODE_FIELD_NAME, new Variant(responseCode));

    if (message != null) {
      responseAsMap.put(MESSAGE_FIELD_NAME, new Variant(message));
    }
    if (additionalPayload != null) {
      responseAsMap.putAll(additionalPayload);
    }

    return responseAsMap;
  }

  /**
   * Returns the properties of this response in a Variant.
   *
   * @return properties of this response in a Variant
   */
  public Variant toVariant() {
    return new Variant(toMap());
  }

  /**
   * Returns the properties of this response appended to the provided Variant.
   *
   * @param baseVariant base Variant to which the properties of this response should be appended
   * @return properties of this response appended to the provided Variant
   */
  public Variant toVariant(Variant baseVariant) {
    var newVariant = baseVariant.asMap();
    newVariant.putAll(toMap());
    return new Variant(newVariant);
  }

  /**
   * Returns the properties of this response in a JSON String.
   *
   * @return properties of this response in a JSON String
   */
  public String toJson() {
    return toVariant().asJsonString();
  }

  /**
   * Returns whether the code of this response is equal to the provided code.
   *
   * @param code response code
   * @return whether the code of this response is equal to the provided code
   */
  public boolean is(String code) {
    return code.equals(responseCode);
  }

  /**
   * Returns whether the code of this response is equal to {@code OK}.
   *
   * @return whether the code of this response is equal to {@code OK}
   */
  public boolean isOk() {
    return is(OK_CODE);
  }

  private void validateResponse(String responseCode, String message) {
    if (responseCode == null) {
      throw new InvalidConnectorResponseException("responseCode must not be null");
    }

    if (!OK_CODE.equals(responseCode) && message == null) {
      throw new InvalidConnectorResponseException(
          "message must not be null if responseCode is not OK");
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(responseCode, message);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    ConnectorResponse response = (ConnectorResponse) other;
    return Objects.equals(responseCode, response.responseCode)
        && Objects.equals(message, response.message);
  }
}

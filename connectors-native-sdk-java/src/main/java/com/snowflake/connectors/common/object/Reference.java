/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import java.util.regex.Pattern;

/** Representation of Snowflake reference */
public class Reference {

  private static final Pattern REFERENCE_PATTERN = Pattern.compile("^reference\\('(.+)'\\)$");
  private final String referenceName;
  private final String value;

  private Reference(String referenceName, String value) {
    this.referenceName = referenceName;
    this.value = value;
  }

  /**
   * Creates new Reference object with empty parameters.
   *
   * @return new Reference object with empty parameters
   */
  public static Reference empty() {
    return new Reference(null, fromName(""));
  }

  /**
   * Creates new Reference object from provided string.
   *
   * @param reference exact string of reference.
   * @return Reference object with wrapped name inside.
   */
  public static Reference of(String reference) {
    if (reference == null || reference.isEmpty()) {
      return Reference.empty();
    } else if (validate(reference)) {
      String referenceName =
          REFERENCE_PATTERN
              .matcher(reference)
              .results()
              .findFirst()
              .map(result -> result.group(1))
              .orElse(null);

      return new Reference(referenceName, reference);
    }

    throw new InvalidReferenceException(reference);
  }

  /**
   * Creates new Reference object from provided name and wraps value with: reference('').
   *
   * @param referenceName reference name .
   * @return Reference object with reference name wrapped in reference('').
   */
  public static Reference from(String referenceName) {
    if (referenceName == null || referenceName.isEmpty()) {
      return Reference.empty();
    } else if (!validate(referenceName)) {
      return new Reference(referenceName, fromName(referenceName));
    }

    throw new InvalidReferenceException(referenceName);
  }

  /**
   * Validates the provided string against the reference pattern
   *
   * @param reference string to validate
   * @return Whether the provided string is a valid Snowflake reference
   */
  public static boolean validate(String reference) {
    if (reference == null) {
      return false;
    }
    return REFERENCE_PATTERN.matcher(reference).matches();
  }

  private static String fromName(String referenceName) {
    return String.format("reference('%s')", referenceName);
  }

  /**
   * Returns the reference name.
   *
   * @return reference name
   */
  public String referenceName() {
    return referenceName;
  }

  /**
   * Returns the reference value.
   *
   * @return reference value
   */
  public String value() {
    return value;
  }
}

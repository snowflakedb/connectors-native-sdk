/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Representation of a Snowflake object reference.
 *
 * <p>Read more about object references at Snowflake <a
 * href="https://docs.snowflake.com/en/sql-reference/references">here</a> and <a
 * href="https://docs.snowflake.com/en/developer-guide/native-apps/requesting-refs">here</a>.
 */
public class Reference {

  /** Raw regex pattern of a valid reference. */
  private static final String PATTERN_RAW =
      format("^reference\\('%s'\\)$", Identifier.UNQUOTED_PATTERN_RAW);

  /** Compiled regex pattern of a valid reference. */
  public static final Pattern PATTERN = Pattern.compile(PATTERN_RAW, CASE_INSENSITIVE);

  private final String name;
  private final String value;

  private Reference(String name, String value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Creates a new reference instance from the provided String.
   *
   * @param reference reference String
   * @return new reference instance
   */
  public static Reference of(String reference) {
    if (reference == null) {
      throw new InvalidReferenceException(null);
    }

    if (isValid(reference)) {
      var referenceName =
          PATTERN
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
   * Creates a new reference instance from the provided reference name.
   *
   * <p>The provided name is wrapped in the {@code reference('%s')} String before the instance
   * creation.
   *
   * @param referenceName reference name
   * @return new reference instance
   */
  public static Reference from(String referenceName) {
    if (referenceName == null) {
      throw new InvalidReferenceNameException(null);
    }

    var reference = format("reference('%s')", referenceName);
    if (isValid(reference)) {
      return new Reference(referenceName, reference);
    }

    throw new InvalidReferenceNameException(referenceName);
  }

  /**
   * Returns whether the provided String is a valid reference.
   *
   * @param reference String to check
   * @return whether the provided String is a valid reference
   */
  public static boolean isValid(String reference) {
    return reference != null && PATTERN.matcher(reference).matches();
  }

  /**
   * Returns the reference name.
   *
   * @return reference name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the reference value.
   *
   * <p>The returned value has the {@code reference('name')} form.
   *
   * @return reference value
   */
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return getValue();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    var that = (Reference) o;
    return Objects.equals(value, that.value) && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, name);
  }
}

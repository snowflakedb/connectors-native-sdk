/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Representation of a Snowflake <a
 * href="https://docs.snowflake.com/en/sql-reference/identifiers-syntax">identifier</a>.
 */
public class Identifier {

  private static final String UNQUOTED_PATTERN_RAW = "([a-zA-Z_][\\w$]*)";
  private static final String QUOTED_PATTERN_RAW = "(\"([^\"]|\"\")+\")";

  /** Raw regex pattern of a valid identifier. */
  public static final String PATTERN_RAW =
      format("(%s|%s)", UNQUOTED_PATTERN_RAW, QUOTED_PATTERN_RAW);

  /** Compiled regex {@link #PATTERN_RAW pattern} of a valid identifier. */
  public static final Pattern PATTERN = Pattern.compile(PATTERN_RAW);

  private final String name;
  private final boolean quoted;

  /**
   * Returns a new instance of an empty identifier.
   *
   * @return empty identifier
   */
  public static Identifier empty() {
    return new Identifier(null, false);
  }

  /**
   * Creates a new identifier instance from the provided String, without any additional quoting.
   *
   * @param identifier identifier String
   * @return new identifier instance
   */
  public static Identifier from(String identifier) {
    if (isEmpty(identifier)) {
      return Identifier.empty();
    } else if (validate(identifier)) {
      return new Identifier(identifier, false);
    }

    throw new InvalidIdentifierException(identifier);
  }

  /**
   * Creates a new identifier instance from the provided String, with additional quoting if
   * necessary to create a valid quoted identifier.
   *
   * @param identifier identifier String
   * @return new identifier instance
   */
  public static Identifier fromWithAutoQuoting(String identifier) {
    if (isEmpty(identifier)) {
      return Identifier.empty();
    } else if (validate(identifier)) {
      return new Identifier(
          identifier, !(identifier.startsWith("\"") && identifier.endsWith("\"")));
    } else if (validate(escapeIdentifier(identifier))) {
      return new Identifier(identifier, true);
    }

    throw new InvalidIdentifierException(identifier);
  }

  /**
   * Validates the provided String against the {@link #PATTERN identifier pattern}.
   *
   * @param identifier String to validate
   * @return whether the provided String is a valid Snowflake identifier
   */
  public static boolean validate(String identifier) {
    return PATTERN.matcher(identifier).matches();
  }

  /**
   * Returns whether the provided identifier is null or empty.
   *
   * @param identifier identifier to check
   * @return whether the provided identifier is null or empty
   */
  public static boolean isNullOrEmpty(Identifier identifier) {
    return identifier == null || identifier.isEmpty();
  }

  /**
   * Validates whether the provided identifier is not null and not empty.
   *
   * @param identifier identifier to check
   * @throws EmptyIdentifierException if the provided identifier is null or empty
   */
  public static void validateNullOrEmpty(Identifier identifier) throws EmptyIdentifierException {
    if (isNullOrEmpty(identifier)) {
      throw new EmptyIdentifierException();
    }
  }

  private static String escapeIdentifier(String text) {
    var result = text.replace("\"", "\"\"");
    return format("\"%s\"", result);
  }

  private static boolean isEmpty(String identifier) {
    return identifier == null || identifier.isEmpty();
  }

  private Identifier(String name, boolean quoted) {
    this.name = name;
    this.quoted = quoted;
  }

  /**
   * Returns the raw identifier name.
   *
   * @return identifier name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns whether the identifier is considered empty (name is null or empty).
   *
   * @return whether the identifier is considered empty
   */
  public boolean isEmpty() {
    return name == null || name.isEmpty();
  }

  /**
   * Returns the identifier name in an SQL-friendly form, wrapping the raw name in quotes if the
   * identifier is quoted.
   *
   * @return SQL-friendly identifier name
   */
  public String toSqlString() {
    return quoted ? escapeIdentifier(name) : name;
  }

  @Override
  public String toString() {
    return format("Identifier[name = %s, quoted = %s]", name, quoted);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Identifier that = (Identifier) o;
    return Objects.equals(name, that.name) && Objects.equals(quoted, that.quoted);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, quoted);
  }
}

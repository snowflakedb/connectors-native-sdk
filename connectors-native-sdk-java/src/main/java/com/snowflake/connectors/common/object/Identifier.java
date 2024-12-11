/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;

import com.snowflake.snowpark_java.types.Variant;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Representation of a Snowflake object identifier.
 *
 * <p>An {@link Identifier} object can be a representation of:
 *
 * <ul>
 *   <li>an unquoted object identifier
 *   <li>a quoted object identifier
 * </ul>
 *
 * <p>Read more about object identifiers at Snowflake <a
 * href="https://docs.snowflake.com/en/sql-reference/identifiers">here</a> and <a
 * href="https://docs.snowflake.com/en/sql-reference/identifiers-syntax">here</a>.
 */
public class Identifier {

  /** Raw regex pattern of a valid unquoted all uppercase identifier. */
  private static final String UNQUOTED_UPPERCASE_PATTERN_RAW = "([A-Z_][A-Z0-9_$]*)";

  /**
   * Compiled regex {@link #UNQUOTED_UPPERCASE_PATTERN_RAW pattern} of a valid unquoted all
   * uppercase identifier.
   */
  private static final Pattern UNQUOTED_UPPERCASE_PATTERN =
      Pattern.compile(UNQUOTED_UPPERCASE_PATTERN_RAW);

  /** Raw regex pattern of a valid unquoted identifier. */
  static final String UNQUOTED_PATTERN_RAW = "([a-zA-Z_][a-zA-Z0-9_$]*)";

  /** Compiled regex {@link #UNQUOTED_PATTERN_RAW pattern} of a valid unquoted identifier. */
  private static final Pattern UNQUOTED_PATTERN = Pattern.compile(UNQUOTED_PATTERN_RAW);

  /** Raw regex pattern of a valid quoted identifier. */
  private static final String QUOTED_PATTERN_RAW = "(\"([^\"]|\"\")*\")";

  /** Compiled regex {@link #QUOTED_PATTERN_RAW pattern} of a valid quoted identifier. */
  private static final Pattern QUOTED_PATTERN = Pattern.compile(QUOTED_PATTERN_RAW);

  /** Raw regex pattern of a valid identifier. */
  static final String PATTERN_RAW = format("(%s|%s)", UNQUOTED_PATTERN_RAW, QUOTED_PATTERN_RAW);

  /** Compiled regex {@link #PATTERN_RAW pattern} of a valid identifier. */
  static final Pattern PATTERN = Pattern.compile(PATTERN_RAW);

  private final String value;

  private Identifier(String value) {
    this.value = value;
  }

  /**
   * Creates a new identifier instance from the provided String, without any additional quoting.
   *
   * @param identifier identifier String
   * @return new identifier instance
   * @throws InvalidIdentifierException if an invalid or null identifier is provided
   */
  public static Identifier from(String identifier) {
    return from(identifier, AutoQuoting.DISABLED);
  }

  /**
   * Creates a new identifier instance from the provided String, with possible additional auto
   * quoting if enabled.
   *
   * @param identifier identifier String
   * @param autoQuoting whether auto quoting should be used
   * @return new identifier instance
   */
  public static Identifier from(String identifier, AutoQuoting autoQuoting) {
    if (identifier == null) {
      throw new InvalidIdentifierException(null);
    }

    return autoQuoting == AutoQuoting.ENABLED
        ? parseWithAutoQuoting(identifier)
        : parseWithoutAutoQuoting(identifier);
  }

  /** Parses the provided String identifier for AutoQuoting.DISABLED. */
  private static Identifier parseWithoutAutoQuoting(String identifier) {
    if (isValid(identifier)) {
      return new Identifier(identifier);
    }

    throw new InvalidIdentifierException(identifier);
  }

  /** Parses the provided String identifier for AutoQuoting.ENABLED. */
  private static Identifier parseWithAutoQuoting(String identifier) {
    if (UNQUOTED_UPPERCASE_PATTERN.matcher(identifier).matches()) {
      return new Identifier(identifier);
    }

    var quotedIdentifier = quoteIdentifier(identifier);
    if (isValid(quotedIdentifier)) {
      return new Identifier(quotedIdentifier);
    }

    throw new InvalidIdentifierException(quotedIdentifier);
  }

  /**
   * Returns whether the provided String matches is a valid identifier.
   *
   * @param identifier String to check
   * @return whether the provided String is a valid identifier
   */
  public static boolean isValid(String identifier) {
    return identifier != null && PATTERN.matcher(identifier).matches();
  }

  /**
   * Returns whether the provided String is a valid unquoted identifier.
   *
   * @param identifier String to check
   * @return whether the provided String is a valid unquoted identifier
   */
  public static boolean isUnquoted(String identifier) {
    return identifier != null && UNQUOTED_PATTERN.matcher(identifier).matches();
  }

  /**
   * Returns whether the provided String is a valid quoted identifier.
   *
   * @param identifier String to check
   * @return whether the provided String is a valid quoted identifier
   */
  public static boolean isQuoted(String identifier) {
    return identifier != null && QUOTED_PATTERN.matcher(identifier).matches();
  }

  /**
   * Escapes any double quote characters in the provided String (by doubling them) and wraps the
   * resulting String is double quotes.
   *
   * <p>If the provided String is already a valid quoted identifier - it is returned without any
   * changes.
   *
   * @param identifier identifier String
   * @return escaped and quoted identifier String
   */
  private static String quoteIdentifier(String identifier) {
    if (isQuoted(identifier)) {
      return identifier;
    }

    return format("\"%s\"", identifier.replace("\"", "\"\""));
  }

  /**
   * Unescapes any double quote characters in the provided String (by replacing doubled characters
   * with single ones) and unwraps the resulting String from double quotes.
   *
   * <p>If the provided String is already a valid unquoted identifier - it is returned without any
   * changes.
   *
   * @param identifier quoted identifier String
   * @return unescaped and unquoted identifier String
   */
  private static String unquoteIdentifier(String identifier) {
    if (isUnquoted(identifier)) {
      return identifier;
    }

    var unescapedIdentifier = identifier.replace("\"\"", "\"");
    return unescapedIdentifier.substring(1, unescapedIdentifier.length() - 1);
  }

  /**
   * Returns the identifier value.
   *
   * <p>Returned value is ready to use in any SQL queries, no additional quoting or character
   * escaping is required.
   *
   * @return identifier value
   */
  public String getValue() {
    return value;
  }

  /**
   * Returns the identifier value in an unquoted form.
   *
   * <p>If the identifier value is already unquoted - it is returned without any changes.
   *
   * <p>Otherwise unescapes any double quote characters in the identifier value (by replacing
   * doubled characters with single ones) and unwraps the resulting String from double quotes.
   *
   * @return unquoted identifier value
   */
  public String getUnquotedValue() {
    return unquoteIdentifier(value);
  }

  /**
   * Returns the identifier value in a quoted form.
   *
   * <p>If the identifier value is already quoted - it is returned without any changes.
   *
   * <p>Otherwise escapes any double quote characters in the identifier value (by doubling them) and
   * wraps the resulting String is double quotes.
   *
   * @return unquoted identifier value
   */
  public String getQuotedValue() {
    return quoteIdentifier(value);
  }

  /**
   * Returns the identifier value wrapped in a Snowpark Variant object.
   *
   * @return identifier value in a Variant
   */
  public Variant getVariantValue() {
    return new Variant(format("\"%s\"", value.replace("\"", "\\\"")));
  }

  /**
   * Returns whether the identifier is unquoted.
   *
   * @return whether the identifier is unquoted
   */
  public boolean isUnquoted() {
    return isUnquoted(value);
  }

  /**
   * Returns whether the identifier is quoted.
   *
   * @return whether the identifier is quoted
   */
  public boolean isQuoted() {
    return isQuoted(value);
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

    var that = (Identifier) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  /** Setting for auto quoting on identifier instance creation. */
  public enum AutoQuoting {
    /**
     * Adds quotes to the given identifier and escapes any double quote characters if the provided
     * String is not all-uppercase (does not match the {@link #UNQUOTED_UPPERCASE_PATTERN pattern}).
     */
    ENABLED,

    /**
     * Never adds any additional quotes to the provided String, never escapes any double quote
     * characters.
     */
    DISABLED
  }
}

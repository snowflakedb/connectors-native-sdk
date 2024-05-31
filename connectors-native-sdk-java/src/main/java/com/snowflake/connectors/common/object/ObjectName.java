/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static com.snowflake.connectors.common.object.Identifier.isNullOrEmpty;
import static com.snowflake.connectors.common.object.Identifier.validateNullOrEmpty;
import static java.lang.String.format;

import java.util.Objects;
import java.util.Stack;
import java.util.regex.Pattern;

/**
 * Representation of a Snowflake <a
 * href="https://docs.snowflake.com/en/sql-reference/name-resolution">object name</a>.
 */
public class ObjectName {

  /** Raw regex pattern of a valid object name. */
  public static final String PATTERN_RAW =
      format(
          "^((%s\\.){0,2}|(%s\\.\\.))%s$",
          Identifier.PATTERN_RAW, Identifier.PATTERN_RAW, Identifier.PATTERN_RAW);

  /** Raw regex pattern of a double-dot object name notation. */
  public static final String DOUBLE_DOT_PATTERN_RAW =
      format("^%s\\.\\.%s$", Identifier.PATTERN_RAW, Identifier.PATTERN_RAW);

  /** Compiled regex {@link #PATTERN_RAW pattern} of a valid object name. */
  public static final Pattern PATTERN = Pattern.compile(PATTERN_RAW);

  /**
   * Compiled regex {@link #DOUBLE_DOT_PATTERN_RAW pattern} of a double-dot object name notation.
   */
  public static final Pattern DOUBLE_DOT_PATTERN = Pattern.compile(DOUBLE_DOT_PATTERN_RAW);

  private final Identifier database;
  private final Identifier schema;
  private final Identifier name;

  /**
   * Creates a new object name instance from the provided object name identifier.
   *
   * @param name object name identifier
   * @return new object name instance
   * @throws EmptyIdentifierException if the provided object name identifier is null or empty
   */
  public static ObjectName from(Identifier name) {
    validateNullOrEmpty(name);
    return new ObjectName(Identifier.empty(), Identifier.empty(), name);
  }

  /**
   * Creates a new object name instance from the provided schema name and object name identifiers.
   *
   * @param schema schema name identifier
   * @param name object name identifier
   * @return new object name instance
   * @throws EmptyIdentifierException if the provided object name identifier is null or empty
   */
  public static ObjectName from(Identifier schema, Identifier name) {
    validateNullOrEmpty(name);
    return isNullOrEmpty(schema) ? from(name) : new ObjectName(Identifier.empty(), schema, name);
  }

  /**
   * Creates a new object name instance from the provided raw schema name and object name
   * identifiers.
   *
   * @param schema raw schema name identifier
   * @param name raw object name identifier
   * @return new object name instance
   * @throws EmptyIdentifierException if the provided object name identifier is null or empty
   */
  public static ObjectName from(String schema, String name) {
    return ObjectName.from(Identifier.from(schema), Identifier.from(name));
  }

  /**
   * Creates a new object name instance from the provided database name, schema name, and object
   * name identifiers.
   *
   * @param database database name identifier
   * @param schema schema name identifier
   * @param name object name identifier
   * @return new object name instance
   * @throws EmptyIdentifierException if the provided object name identifier is null or empty
   */
  public static ObjectName from(Identifier database, Identifier schema, Identifier name) {
    validateNullOrEmpty(name);
    return isNullOrEmpty(database) ? from(schema, name) : new ObjectName(database, schema, name);
  }

  /**
   * Creates a new object name instance from the provided raw database name, schema name, and object
   * name identifiers.
   *
   * @param database raw database name identifier
   * @param schema raw schema name identifier
   * @param name raw object name identifier
   * @return new object name instance
   * @throws EmptyIdentifierException if the provided object name identifier is null or empty
   */
  public static ObjectName from(String database, String schema, String name) {
    return ObjectName.from(
        Identifier.from(database), Identifier.from(schema), Identifier.from(name));
  }

  /**
   * Creates a new object name instance from the provided raw object name (fully qualified or not).
   *
   * @param objectName raw object name
   * @return new object name instance
   * @throws InvalidObjectNameException if the provided object name is not a valid Snowflake object
   *     name
   * @throws EmptyIdentifierException if the provided object name identifier is null or empty
   */
  public static ObjectName fromString(String objectName) {
    if (!validate(objectName)) {
      throw new InvalidObjectNameException(objectName);
    }

    var identifierMatcher = Identifier.PATTERN.matcher(objectName);
    var identifierStack = new Stack<String>();

    while (identifierMatcher.find()) {
      identifierStack.push(identifierMatcher.group());
    }

    var name =
        identifierStack.isEmpty() ? Identifier.empty() : Identifier.from(identifierStack.pop());
    var schema =
        identifierStack.isEmpty() || validateDoubleDot(objectName)
            ? Identifier.empty()
            : Identifier.from(identifierStack.pop());
    var database =
        identifierStack.isEmpty() ? Identifier.empty() : Identifier.from(identifierStack.pop());

    return new ObjectName(database, schema, name);
  }

  /**
   * Returns whether the provided raw object name is a valid Snowflake object name (matches the
   * {@link #PATTERN pattern}).
   *
   * @param objectName raw object name
   * @return whether provided raw object name is a valid Snowflake object name
   */
  public static boolean validate(String objectName) {
    return PATTERN.matcher(objectName).matches();
  }

  /**
   * Returns whether the provided raw object name is a valid Snowflake object name in the double-dot
   * notation (matches the {@link #DOUBLE_DOT_PATTERN pattern}).
   *
   * @param objectName raw object name
   * @return whether provided raw object name is a valid Snowflake object name in the double-dot
   *     notation
   */
  public static boolean validateDoubleDot(String objectName) {
    return DOUBLE_DOT_PATTERN.matcher(objectName).matches();
  }

  private ObjectName(Identifier database, Identifier schema, Identifier name) {
    this.database = database;
    this.schema = schema;
    this.name = name;
  }

  /**
   * Returns the object name in an SQL-friendly form.
   *
   * <p>The returned String can be a fully qualified name, a database-less name, a double-dot
   * notated name, or a simple unqualified object name.
   *
   * <p>If needed the individual identifiers will be quoted using {@link Identifier#toSqlString()}.
   *
   * @return SQL-friendly object name
   */
  public String getEscapedName() {
    var builder = new StringBuilder();
    if (!database.isEmpty()) {
      builder.append(database.toSqlString()).append('.');
    }

    if (!schema.isEmpty()) {
      builder.append(schema.toSqlString()).append('.');
    }

    if (schema.isEmpty() && !database.isEmpty() && !name.isEmpty()) {
      builder.append("..");
    }

    if (!name.isEmpty()) {
      builder.append(name.toSqlString());
    }

    return builder.toString();
  }

  /**
   * Returns whether this object name is fully qualified.
   *
   * @return whether this object name is fully qualified
   */
  public boolean isFullyQualified() {
    return !database.isEmpty() && !name.isEmpty();
  }

  /**
   * Returns the database of this object name.
   *
   * @return database of this object name.
   */
  public Identifier getDatabase() {
    return database;
  }

  /**
   * Returns the schema of this object name.
   *
   * @return schema of this object name.
   */
  public Identifier getSchema() {
    return schema;
  }

  /**
   * Returns the name of this object name.
   *
   * @return name of this object name.
   */
  public Identifier getName() {
    return name;
  }

  @Override
  public String toString() {
    return format(
        "ObjectName[name = %s, schema = %s, database = %s]",
        name.toSqlString(), schema.toSqlString(), database.toSqlString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ObjectName that = (ObjectName) o;
    return Objects.equals(database.getName(), that.database.getName())
        && Objects.equals(schema.getName(), that.schema.getName())
        && Objects.equals(name.getName(), that.name.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(database.getName(), schema.getName(), name.getName());
  }
}

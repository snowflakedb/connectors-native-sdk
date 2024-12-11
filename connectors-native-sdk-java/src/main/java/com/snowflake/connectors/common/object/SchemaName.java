/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;

import java.util.Objects;
import java.util.Optional;
import java.util.Stack;
import java.util.regex.Pattern;

/**
 * Representation of a Snowflake schema name.
 *
 * <p>A {@link SchemaName} object can be a representation of:
 *
 * <ul>
 *   <li>an individual schema identifier
 *   <li>a {@code db.schema} object
 * </ul>
 *
 * <p>Read more about schema names at Snowflake <a
 * href="https://docs.snowflake.com/en/sql-reference/identifiers">here</a>.
 */
public class SchemaName {

  /** Raw regex pattern of a valid schema name. */
  private static final String PATTERN_RAW =
      format("^((%s\\.){0,1})%s$", Identifier.PATTERN_RAW, Identifier.PATTERN_RAW);

  /** Compiled regex {@link #PATTERN_RAW pattern} of a valid schema name. */
  private static final Pattern PATTERN = Pattern.compile(PATTERN_RAW);

  private final Optional<Identifier> database;
  private final Identifier schema;

  /**
   * Creates a new schema name instance from the provided raw schema name.
   *
   * @param schemaName raw schema name
   * @return new schema name instance
   * @throws InvalidObjectNameException if the provided String is not a valid Snowflake schema name
   */
  public static SchemaName fromString(String schemaName) {
    if (schemaName == null || !isValid(schemaName)) {
      throw new InvalidSchemaNameException(schemaName);
    }

    var identifierMatcher = Identifier.PATTERN.matcher(schemaName);
    var identifierStack = new Stack<String>();

    while (identifierMatcher.find()) {
      identifierStack.push(identifierMatcher.group());
    }

    var schema = identifierStack.isEmpty() ? null : Identifier.from(identifierStack.pop());
    var database = identifierStack.isEmpty() ? null : Identifier.from(identifierStack.pop());

    return from(database, schema);
  }

  /**
   * Creates a new schema name instance from the provided schema identifier.
   *
   * @param schema schema identifier
   * @return new schema name instance
   * @throws InvalidIdentifierException if the provided schema identifier is null
   */
  public static SchemaName from(Identifier schema) {
    return from(null, schema);
  }

  /**
   * Creates a new schema name instance from the provided raw database and schema identifiers.
   *
   * @param database raw database identifier
   * @param schema raw schema identifier
   * @return new schema name instance
   * @throws InvalidIdentifierException if the provided database or schema is not a valid Snowflake
   *     identifier
   */
  public static SchemaName from(String database, String schema) {
    return from(Identifier.from(database), Identifier.from(schema));
  }

  /**
   * Creates a new schema name instance from the provided database and schema identifiers.
   *
   * @param database database identifier
   * @param schema schema identifier
   * @return new schema name instance
   * @throws InvalidIdentifierException if the provided schema identifier is null
   */
  public static SchemaName from(Identifier database, Identifier schema) {
    if (schema == null) {
      throw new InvalidIdentifierException(null);
    }

    return new SchemaName(database, schema);
  }

  /**
   * Returns whether the provided String is a valid schema name.
   *
   * @param schemaName raw schema name
   * @return whether the provided String is a valid schema name
   */
  public static boolean isValid(String schemaName) {
    return PATTERN.matcher(schemaName).matches();
  }

  private SchemaName(Identifier database, Identifier schema) {
    this.database = Optional.ofNullable(database);
    this.schema = schema;
  }

  /**
   * Returns the schema name value.
   *
   * <p>Returned value is ready to use in any SQL queries, no additional quoting or character
   * escaping is required.
   *
   * <p>The returned String can be a fully qualified name or a simple unqualified schema name.
   *
   * <p>The individual identifiers will be converted to String using {@link Identifier#getValue()}.
   * They may be quoted or unquoted.
   *
   * @return SQL-friendly object name
   */
  public String getValue() {
    return database
        .map(db -> format("%s.%s", db.getValue(), schema.getValue()))
        .orElseGet(schema::getValue);
  }

  /**
   * Returns whether this schema name is fully qualified.
   *
   * @return whether this schema name is fully qualified
   */
  public boolean isFullyQualified() {
    return database.isPresent();
  }

  /**
   * Returns the database of this schema name.
   *
   * @return database of this schema name.
   */
  public Optional<Identifier> getDatabase() {
    return database;
  }

  /**
   * Returns the schema of this schema name.
   *
   * @return schema of this schema name.
   */
  public Identifier getSchema() {
    return schema;
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

    var that = (SchemaName) o;
    return Objects.equals(database, that.database) && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, schema);
  }
}

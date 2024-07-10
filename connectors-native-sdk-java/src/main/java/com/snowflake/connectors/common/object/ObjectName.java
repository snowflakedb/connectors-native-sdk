/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;

import com.snowflake.snowpark_java.types.Variant;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;
import java.util.regex.Pattern;

/**
 * Representation of a Snowflake object name.
 *
 * <p>An {@link ObjectName} object can be a representation of:
 *
 * <ul>
 *   <li>an individual name identifier
 *   <li>a {@code schema.name} object
 *   <li>a {@code db.schema.name} object
 *   <li>a {@code db..name} object
 * </ul>
 *
 * <p>Read more about object names at Snowflake <a
 * href="https://docs.snowflake.com/en/sql-reference/identifiers">here</a> and <a
 * href="https://docs.snowflake.com/en/sql-reference/name-resolution">here</a>.
 */
public class ObjectName {

  /** Raw regex pattern of a valid object name. */
  private static final String PATTERN_RAW =
      format(
          "^((%s\\.){0,2}|(%s\\.\\.))%s$",
          Identifier.PATTERN_RAW, Identifier.PATTERN_RAW, Identifier.PATTERN_RAW);

  /** Compiled regex {@link #PATTERN_RAW pattern} of a valid object name. */
  private static final Pattern PATTERN = Pattern.compile(PATTERN_RAW);

  /** Raw regex pattern of a double-dot object name notation. */
  private static final String DOUBLE_DOT_PATTERN_RAW =
      format("^%s\\.\\.%s$", Identifier.PATTERN_RAW, Identifier.PATTERN_RAW);

  /**
   * Compiled regex {@link #DOUBLE_DOT_PATTERN_RAW pattern} of a double-dot object name notation.
   */
  private static final Pattern DOUBLE_DOT_PATTERN = Pattern.compile(DOUBLE_DOT_PATTERN_RAW);

  private final Optional<Identifier> database;
  private final Optional<Identifier> schema;
  private final Identifier name;

  /**
   * Creates a new object name instance from the provided raw object name (fully qualified or not).
   *
   * @param objectName raw object name
   * @return new object name instance
   * @throws InvalidObjectNameException if the provided String is not a valid Snowflake object name
   */
  public static ObjectName fromString(String objectName) {
    if (objectName == null || !isValid(objectName)) {
      throw new InvalidObjectNameException(objectName);
    }

    var isDoubleDot = DOUBLE_DOT_PATTERN.matcher(objectName).matches();
    var identifierMatcher = Identifier.PATTERN.matcher(objectName);
    var identifierStack = new Stack<String>();

    while (identifierMatcher.find()) {
      identifierStack.push(identifierMatcher.group());
    }

    var name = identifierStack.isEmpty() ? null : Identifier.from(identifierStack.pop());
    var schema =
        identifierStack.isEmpty() || isDoubleDot ? null : Identifier.from(identifierStack.pop());
    var database = identifierStack.isEmpty() ? null : Identifier.from(identifierStack.pop());

    return from(database, schema, name);
  }

  /**
   * Creates a new object name instance from the provided name identifier.
   *
   * @param name name identifier
   * @return new object name instance
   * @throws InvalidIdentifierException if the provided name identifier is null
   */
  public static ObjectName from(Identifier name) {
    return from(null, null, name);
  }

  /**
   * Creates a new object name instance from the provided raw schema and name identifiers.
   *
   * @param schema raw schema identifier
   * @param name raw object identifier
   * @return new object name instance
   * @throws InvalidIdentifierException if the provided schema or name is not a valid Snowflake
   *     identifier
   */
  public static ObjectName from(String schema, String name) {
    return from(Identifier.from(schema), Identifier.from(name));
  }

  /**
   * Creates a new object name instance from the provided schema and name identifiers.
   *
   * @param schema schema identifier
   * @param name name identifier
   * @return new object name instance
   * @throws InvalidIdentifierException if the provided name identifier is null
   */
  public static ObjectName from(Identifier schema, Identifier name) {
    return from(null, schema, name);
  }

  /**
   * Creates a new object name instance from the provided schema name and name identifier.
   *
   * @param schemaName schema name
   * @param name name identifier
   * @return new object name instance
   * @throws InvalidIdentifierException if the provided name identifier is null
   */
  public static ObjectName from(SchemaName schemaName, Identifier name) {
    return from(schemaName.getDatabase().orElse(null), schemaName.getSchema(), name);
  }

  /**
   * Creates a new object name instance from the provided raw database, schema, and name
   * identifiers.
   *
   * @param database raw database identifier
   * @param schema raw schema identifier
   * @param name raw name identifier
   * @return new object name instance
   * @throws InvalidIdentifierException if the provided database, schema, or name is not a valid
   *     Snowflake identifier
   */
  public static ObjectName from(String database, String schema, String name) {
    return from(Identifier.from(database), Identifier.from(schema), Identifier.from(name));
  }

  /**
   * Creates a new object name instance from the provided database, schema, and name identifiers.
   *
   * @param database database identifier
   * @param schema schema identifier
   * @param name name identifier
   * @return new object name instance
   * @throws InvalidIdentifierException if the provided name identifier is null
   */
  public static ObjectName from(Identifier database, Identifier schema, Identifier name) {
    if (name == null) {
      throw new InvalidIdentifierException(null);
    }

    return new ObjectName(database, schema, name);
  }

  /**
   * Returns whether the provided String is a valid object name.
   *
   * @param objectName raw object name
   * @return whether the provided String is a valid object name
   */
  public static boolean isValid(String objectName) {
    return PATTERN.matcher(objectName).matches();
  }

  private ObjectName(Identifier database, Identifier schema, Identifier name) {
    this.database = Optional.ofNullable(database);
    this.schema = Optional.ofNullable(schema);
    this.name = name;
  }

  /**
   * Returns the object name value.
   *
   * <p>Returned value is ready to use in any SQL queries, no additional quoting or character
   * escaping is required.
   *
   * <p>The returned String can be a fully qualified name, a database-less name, a double-dot
   * notated name, or a simple unqualified object name.
   *
   * <p>The individual identifiers will be converted to String using {@link Identifier#getValue()}.
   * They may be quoted or unquoted.
   *
   * @return SQL-friendly object name
   */
  public String getValue() {
    var nameStack = new Stack<String>();

    database.ifPresent(identifier -> nameStack.push(identifier.getValue()));
    schema.ifPresentOrElse(
        identifier -> nameStack.push(identifier.getValue()),
        () -> database.ifPresent(doubleDot -> nameStack.push("")));
    nameStack.push(name.getValue());

    return String.join(".", nameStack);
  }

  /**
   * Returns the object name value as a Snowpark Variant object.
   *
   * @return object name value in a Variant
   */
  public Variant getVariantValue() {
    return new Variant(format("\"%s\"", getValue().replace("\"", "\\\"")));
  }

  /**
   * Returns whether this object name is fully qualified.
   *
   * @return whether this object name is fully qualified
   */
  public boolean isFullyQualified() {
    return database.isPresent();
  }

  /**
   * Returns the database of this object name.
   *
   * @return database of this object name.
   */
  public Optional<Identifier> getDatabase() {
    return database;
  }

  /**
   * Returns the schema of this object name.
   *
   * @return schema of this object name.
   */
  public Optional<Identifier> getSchema() {
    return schema;
  }

  /**
   * Returns the schema name of this object name.
   *
   * @return schema name of this object name.
   */
  public Optional<SchemaName> getSchemaName() {
    return schema.map(schema -> SchemaName.from(database.orElse(null), schema));
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
        name.getValue(),
        schema.map(Identifier::getValue).orElse(null),
        database.map(Identifier::getValue).orElse(null));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    var that = (ObjectName) o;
    return Objects.equals(database, that.database)
        && Objects.equals(schema, that.schema)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, schema, name);
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SchemaNameTest {

  @ParameterizedTest
  @MethodSource("validSchemaNames")
  void shouldCreateValidSchemaNamesFromString(String raw, String dbValue, String schemaValue) {
    // expect
    assertThat(SchemaName.isValid(raw)).isTrue();

    // and
    assertThat(SchemaName.fromString(raw))
        .hasValue(raw)
        .hasDatabaseValue(dbValue)
        .hasSchemaValue(schemaValue);
  }

  @ParameterizedTest
  @MethodSource("invalidSchemaNames")
  void shouldThrowErrorOnInvalidSchemaNamesFromString(String raw) {
    // expect
    assertThat(SchemaName.isValid(raw)).isFalse();
    assertThatExceptionOfType(InvalidSchemaNameException.class)
        .isThrownBy(() -> SchemaName.fromString(raw))
        .withMessage(format("'%s' is not a valid Snowflake schema name", raw));
  }

  @ParameterizedTest
  @MethodSource("validSchemaNameSchemas")
  void shouldCreateValidSchemaNamesFromSchema(String schema) {
    // expect
    assertThat(SchemaName.from(Identifier.from(schema)))
        .hasValue(schema)
        .hasDatabaseValue(null)
        .hasSchemaValue(schema);
  }

  @ParameterizedTest
  @MethodSource("validSchemaNameSchemasAndDbs")
  void shouldCreateValidSchemaNamesFromSchemaAndDb(String db, String schema, String value) {
    // expect
    assertThat(SchemaName.from(Identifier.from(db), Identifier.from(schema)))
        .hasValue(value)
        .hasDatabaseValue(db)
        .hasSchemaValue(schema);

    // and
    assertThat(SchemaName.from(db, schema))
        .hasValue(value)
        .hasDatabaseValue(db)
        .hasSchemaValue(schema);
  }

  @Test
  void shouldThrowErrorOnInvalidObjectNamesFromNameAndSchema() {
    // expect
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> SchemaName.from(Identifier.from("db"), Identifier.from("")))
        .withMessage("'' is not a valid Snowflake identifier");

    // and
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> SchemaName.from(Identifier.from("db"), null))
        .withMessage("'null' is not a valid Snowflake identifier");

    // and
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> SchemaName.from("db", null))
        .withMessage("'null' is not a valid Snowflake identifier");
  }

  private static Stream<Arguments> validSchemaNames() {
    return Stream.of(
        Arguments.of("db.schema", "db", "schema"),
        Arguments.of("DB.schema", "DB", "schema"),
        Arguments.of("schema", null, "schema"),
        Arguments.of("\"d6\".\"sCh3m@\"", "\"d6\"", "\"sCh3m@\""),
        Arguments.of("\"sCh3m@\"", null, "\"sCh3m@\""),
        Arguments.of("\"a.b.c\".\"a.b.c\"", "\"a.b.c\"", "\"a.b.c\""),
        Arguments.of("\"a.b.c\"", null, "\"a.b.c\""));
  }

  private static Stream<String> invalidSchemaNames() {
    return Stream.of(
        "",
        ".",
        ".schema",
        "db.",
        "db.1",
        "db.!@#%",
        "db.name!",
        "db.schema@",
        "db#.schema",
        "312.132",
        "312.&*",
        "db.1schema",
        "2db.schema",
        "db.---",
        "--.---",
        "--.schema",
        "--.schema.",
        "...",
        "a.b.c.a.b.c");
  }

  private static Stream<String> validSchemaNameSchemas() {
    return Stream.of("schema", "sch3m4", "\"sChEm@\"", "\"a.b.c\"", "\"\"");
  }

  private static Stream<Arguments> validSchemaNameSchemasAndDbs() {
    return Stream.of(
        Arguments.of("db", "schema", "db.schema"),
        Arguments.of("d6", "Sch3ma", "d6.Sch3ma"),
        Arguments.of("db", "\"schema\"", "db.\"schema\""),
        Arguments.of("\"d@\"", "\"a.b.c\"", "\"d@\".\"a.b.c\""));
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ObjectNameTest {

  @ParameterizedTest
  @MethodSource("validObjectNames")
  void shouldCreateValidObjectNamesFromString(
      String raw, String dbValue, String schemaValue, String schemaNameValue, String nameValue) {
    // expect
    assertThat(ObjectName.isValid(raw)).isTrue();

    // and
    assertThat(ObjectName.fromString(raw))
        .hasValue(raw)
        .hasDatabaseValue(dbValue)
        .hasSchemaValue(schemaValue)
        .hasSchemaNameValue(schemaNameValue)
        .hasNameValue(nameValue);
  }

  @ParameterizedTest
  @MethodSource("invalidObjectNames")
  void shouldThrowErrorOnInvalidObjectNamesFromString(String raw) {
    // expect
    assertThat(ObjectName.isValid(raw)).isFalse();
    assertThatExceptionOfType(InvalidObjectNameException.class)
        .isThrownBy(() -> ObjectName.fromString(raw))
        .withMessage(String.format("'%s' is not a valid Snowflake object name", raw));
  }

  @Test
  void shouldCreateValidObjectNamesFromName() {
    // expect
    assertThat(ObjectName.from(Identifier.from("someName")))
        .hasValue("someName")
        .hasDatabaseValue(null)
        .hasSchemaValue(null)
        .hasSchemaNameValue(null)
        .hasNameValue("someName");
  }

  @ParameterizedTest
  @MethodSource("validObjectNameNamesAndSchemas")
  void shouldCreateValidObjectNamesFromNameAndSchema(
      String schema, String name, String objNameValue) {
    // expect
    assertThat(ObjectName.from(Identifier.from(schema), Identifier.from(name)))
        .hasValue(objNameValue)
        .hasDatabaseValue(null)
        .hasSchemaValue(schema)
        .hasSchemaNameValue(schema)
        .hasNameValue(name);

    // and
    assertThat(ObjectName.from(schema, name))
        .hasValue(objNameValue)
        .hasDatabaseValue(null)
        .hasSchemaValue(schema)
        .hasSchemaNameValue(schema)
        .hasNameValue(name);
  }

  @Test
  void shouldThrowErrorOnInvalidObjectNamesFromNameAndSchema() {
    // expect
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> ObjectName.from(Identifier.from("schema"), Identifier.from("")))
        .withMessage("'' is not a valid Snowflake identifier");

    // and
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> ObjectName.from(Identifier.from("schema"), null))
        .withMessage("'null' is not a valid Snowflake identifier");

    // and
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> ObjectName.from("schema", null))
        .withMessage("'null' is not a valid Snowflake identifier");
  }

  @ParameterizedTest
  @MethodSource("validObjectNameNamesAndSchemasAndDbs")
  void shouldCreateValidObjectNamesFromNameAndSchemaAndDb(
      String db, String schema, String name, String schemaNameValue, String objNameValue) {
    // expect
    assertThat(ObjectName.from(Identifier.from(db), Identifier.from(schema), Identifier.from(name)))
        .hasValue(objNameValue)
        .hasDatabaseValue(db)
        .hasSchemaValue(schema)
        .hasSchemaNameValue(schemaNameValue)
        .hasNameValue(name);

    // and
    assertThat(ObjectName.from(db, schema, name))
        .hasValue(objNameValue)
        .hasDatabaseValue(db)
        .hasSchemaValue(schema)
        .hasSchemaNameValue(schemaNameValue)
        .hasNameValue(name);
  }

  @Test
  void shouldThrowErrorOnInvalidObjectNamesFromNameAndSchemaAndDb() {
    // expect
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(
            () ->
                ObjectName.from(
                    Identifier.from("db"), Identifier.from("schema"), Identifier.from(null)))
        .withMessage("'null' is not a valid Snowflake identifier");

    // and
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> ObjectName.from(Identifier.from("db"), Identifier.from("schema"), null))
        .withMessage("'null' is not a valid Snowflake identifier");

    // and
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> ObjectName.from("db", "schema", ""))
        .withMessage("'' is not a valid Snowflake identifier");
  }

  @ParameterizedTest
  @MethodSource("validObjectNames")
  void shouldReturnTheSameObjectNameFromVariant(String raw) {
    // expect
    var objectName = ObjectName.fromString(raw);
    assertThat(objectName.getVariantValue().asString()).isEqualTo(raw);
  }

  @Test
  void shouldReturnDoubleDotNotatedValue() {
    // expect
    assertThat(ObjectName.from(Identifier.from("db"), null, Identifier.from("name")))
        .hasValue("db..name");
  }

  private static Stream<Arguments> validObjectNames() {
    return Stream.of(
        Arguments.of("db.schema.name", "db", "schema", "db.schema", "name"),
        Arguments.of("DB.schema.NaMe", "DB", "schema", "DB.schema", "NaMe"),
        Arguments.of("db..name", "db", null, null, "name"),
        Arguments.of("name", null, null, null, "name"),
        Arguments.of("schema.name", null, "schema", "schema", "name"),
        Arguments.of("\"n@M3\"", null, null, null, "\"n@M3\""),
        Arguments.of("\"d6\"..\"n@me\"", "\"d6\"", null, null, "\"n@me\""),
        Arguments.of("db..\"na\"\"me\"", "db", null, null, "\"na\"\"me\""),
        Arguments.of("db..\"na'me\"", "db", null, null, "\"na'me\""),
        Arguments.of("\"a.b.c\"", null, null, null, "\"a.b.c\""),
        Arguments.of("\"a.b.c\"..\"a.b.c\"", "\"a.b.c\"", null, null, "\"a.b.c\""));
  }

  private static Stream<String> invalidObjectNames() {
    return Stream.of(
        "",
        "..",
        "..name",
        "db.schema.",
        "db.schema.1",
        "db.schema.!@#%",
        "db.schema.name!",
        "db.schema@.name",
        "db#.schema.name",
        "312.132.123",
        "312.&*.123",
        "db.schema.1name",
        "db.2schema.name",
        "3db.schema.name",
        "db.schema.---",
        "db.---.---",
        "--.---.---",
        "--.---.",
        "--.---.",
        "...",
        "a.b.c..a.b.c");
  }

  private static Stream<Arguments> validObjectNameNamesAndSchemas() {
    return Stream.of(
        Arguments.of("schema", "name", "schema.name"),
        Arguments.of("Sch3ma", "nAme", "Sch3ma.nAme"),
        Arguments.of("\"schema\"", "name", "\"schema\".name"),
        Arguments.of("schema", "\"a.b.c\"", "schema.\"a.b.c\""));
  }

  private static Stream<Arguments> validObjectNameNamesAndSchemasAndDbs() {
    return Stream.of(
        Arguments.of("db", "schema", "name", "db.schema", "db.schema.name"),
        Arguments.of("DB", "Sch3ma", "nAme", "DB.Sch3ma", "DB.Sch3ma.nAme"),
        Arguments.of("db", "\"schema\"", "name", "db.\"schema\"", "db.\"schema\".name"),
        Arguments.of(
            "\"a.b.c\"", "schema", "\"a.b.c\"", "\"a.b.c\".schema", "\"a.b.c\".schema.\"a.b.c\""));
  }
}

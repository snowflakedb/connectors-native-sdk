/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ObjectNameTest {

  @ParameterizedTest
  @MethodSource("validObjectNames")
  void validObjectNameTest(String objectName) {
    assertThat(ObjectName.validate(objectName)).isTrue();
    assertThat(ObjectName.fromString(objectName)).isNotNull();
  }

  @ParameterizedTest
  @MethodSource("invalidObjectNames")
  void invalidObjectNameTest(String objectName) {
    assertThat(ObjectName.validate(objectName)).isFalse();
    assertThatExceptionOfType(InvalidObjectNameException.class)
        .isThrownBy(() -> ObjectName.fromString(objectName));
  }

  @Test
  void shouldCreateProperObjectNameFromName() {
    // when
    var objectName = ObjectName.from(Identifier.fromWithAutoQuoting("object-name"));

    // then
    assertThat(objectName.getEscapedName()).isEqualTo("\"object-name\"");
  }

  @Test
  void shouldThrowExceptionOnCreateObjectNameFromInvalidName() {
    // then
    assertThatThrownBy(() -> ObjectName.from("db", "object-name"))
        .isInstanceOf(InvalidIdentifierException.class)
        .hasMessage("object-name is not a valid Snowflake identifier");
  }

  @ParameterizedTest
  @MethodSource("provideThreeParamObjects")
  void shouldCreateProperObjectNameFromSchemaAndName(
      String schemaName, String name, String result) {
    // when
    var objectName = ObjectName.from(schemaName, name);

    // then
    assertThat(objectName.getEscapedName()).isEqualTo(result);
  }

  @ParameterizedTest
  @MethodSource("provideThreeParamObjectsError")
  void shouldThrowExceptionWhenCreatingObjectNameFromSchemaAndName(String schemaName, String name) {
    // then
    assertThatThrownBy(() -> ObjectName.from(schemaName, name))
        .isInstanceOf(InvalidIdentifierException.class)
        .hasMessageContaining("is not a valid Snowflake identifier");
  }

  @ParameterizedTest
  @MethodSource("provideThreeParamObjectsError")
  void shouldCreateObjectNameFromSchemaAndNameWhenAutoQuoting(
      String schemaName, String name, String result) {
    // when
    var objectName =
        ObjectName.from(
            Identifier.fromWithAutoQuoting(schemaName), Identifier.fromWithAutoQuoting(name));

    // then
    assertThat(objectName.getEscapedName()).isEqualTo(result);
  }

  @ParameterizedTest
  @ValueSource(strings = "")
  @NullSource
  void shouldThrowWhenCreatingObjectNameFromNullableName(String name) {
    // then
    assertThatThrownBy(() -> ObjectName.from(Identifier.from(name)))
        .isInstanceOf(EmptyIdentifierException.class)
        .hasMessage("Provided identifier must not be empty");
  }

  private static List<Arguments> provideThreeParamObjects() {
    return Arrays.asList(
        Arguments.of("", "\"object-name\"", "\"object-name\""),
        Arguments.of(null, "\"object-name\"", "\"object-name\""),
        Arguments.of("\"schema-name\"", "\"object-name\"", "\"schema-name\".\"object-name\""));
  }

  private static List<Arguments> provideThreeParamObjectsError() {
    return Arrays.asList(
        Arguments.of("", "object-name", "\"object-name\""),
        Arguments.of(null, "object-name", "\"object-name\""),
        Arguments.of("schema-name", "object-name", "\"schema-name\".\"object-name\""));
  }

  private static Stream<String> validObjectNames() {
    return Stream.of(
        "c..a",
        "b.a",
        "c.b.a",
        "schema.table_name",
        "db.schema.table_name",
        "db..table_name",
        "\"SN_TEST_OBJECT_1364386155.!| @,#$\"",
        "\"schema \".\" table name\"",
        "\"db\".\"schema\".\"table_name\"",
        "\"db\"..\"table_name\"",
        "\"\"\"db\"\"\"..\"\"\"table_name\"\"\"",
        "\"\"\"na.me\"\"\"",
        "\"n\"\"a..m\"\"e\"",
        "\"schema\"\"\".\"n\"\"a..m\"\"e\"",
        "\"\"\"db\".\"schema\"\"\".\"n\"\"a..m\"\"e\"");
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
        "...");
  }
}

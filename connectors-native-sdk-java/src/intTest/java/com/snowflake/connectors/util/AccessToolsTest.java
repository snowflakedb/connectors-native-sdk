/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.SchemaName;
import com.snowflake.connectors.util.snowflake.AccessTools;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AccessToolsTest extends BaseIntegrationTest {

  private static final String SECRET_SCHEMA = "SECRET_SCHEMA";
  private static final Identifier SPECIAL_IDENTIFIER = Identifier.from("\"a'b\"\"c❄d\"");
  private final String SECRET_PATH =
      String.format("%s.%s.TEST_SECRET_%s", DATABASE_NAME, SECRET_SCHEMA, TEST_ID);

  private AccessTools accessTools;

  @BeforeAll
  void setup() {
    accessTools = AccessTools.getInstance(session);

    session.sql(String.format("CREATE SCHEMA %s", SECRET_SCHEMA)).toLocalIterator();
    session
        .sql(
            String.format(
                "CREATE SECRET %s TYPE = GENERIC_STRING SECRET_STRING = 'TEST'", SECRET_PATH))
        .toLocalIterator();
  }

  @Test
  void shouldHaveAccessToXSWarehouse() {
    // expect
    assertThat(accessTools.hasWarehouseAccess(Identifier.from(WAREHOUSE_NAME))).isTrue();
  }

  @Test
  void shouldNotHaveAccessToNonexistentWarehouse() {
    // expect
    assertThat(accessTools.hasWarehouseAccess(Identifier.from("NONEXISTENT_WH"))).isFalse();
  }

  @Test
  void shouldHandleSpecialCharactersInWarehouseName() {
    // expect
    assertThat(accessTools.hasWarehouseAccess(SPECIAL_IDENTIFIER)).isFalse();
  }

  @Test
  void shouldHaveAccessToTheStateSchema() {
    // expect
    assertThat(accessTools.hasSchemaAccess(SchemaName.from(DATABASE_NAME, "STATE"))).isTrue();
  }

  @Test
  void shouldNotHaveAccessToNonexistentSchema() {
    // expect
    assertThat(accessTools.hasSchemaAccess(SchemaName.from(DATABASE_NAME, "NONEXISTENT_SCHEMA")))
        .isFalse();
  }

  @Test
  void shouldHandleSpecialCharactersInSchemaName() {
    // expect
    assertThat(accessTools.hasSchemaAccess(SchemaName.from(SPECIAL_IDENTIFIER))).isFalse();
  }

  @Test
  void shouldNotHaveAccessToTheSchemaInNonexistentDatabase() {
    // expect
    assertThat(accessTools.hasSchemaAccess(SchemaName.from("UNKNOWN", "NONEXISTENT_SCHEMA")))
        .isFalse();
  }

  @Test
  void shouldHaveAccessToTheTestDatabase() {
    // expect
    assertThat(accessTools.hasDatabaseAccess(Identifier.from(DATABASE_NAME))).isTrue();
  }

  @Test
  void shouldNotHaveAccessToTheNonexistentDatabase() {
    // expect
    assertThat(accessTools.hasDatabaseAccess(Identifier.from("UNKNOWN"))).isFalse();
  }

  @Test
  void shouldHandleSpecialCharactersInDatabaseName() {
    // expect
    assertThat(accessTools.hasDatabaseAccess(SPECIAL_IDENTIFIER)).isFalse();
  }

  @Test
  void shouldHaveAccessToTheTestSecret() {
    // expect
    assertThat(accessTools.hasSecretAccess(ObjectName.fromString(SECRET_PATH))).isTrue();
  }

  @Test
  void shouldNotHaveAccessToTheNonexistentSecret() {
    // expect
    assertThat(
            accessTools.hasSecretAccess(ObjectName.from(DATABASE_NAME, SECRET_SCHEMA, "UNKNOWN")))
        .isFalse();
  }

  @Test
  void shouldHandleSpecialCharactersInSecretName() {
    // expect
    assertThat(accessTools.hasSecretAccess(ObjectName.from(SPECIAL_IDENTIFIER))).isFalse();
  }
}

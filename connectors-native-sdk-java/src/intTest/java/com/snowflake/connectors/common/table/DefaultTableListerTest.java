/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.SchemaName;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DefaultTableListerTest extends BaseIntegrationTest {

  private final String TEST_SCHEMA = "TEST_SCHEMA";
  private final String[] referenceTableNames = {"TBL1", "TBL2", "TBL3"};
  private final Identifier SCHEMA = Identifier.from(TEST_SCHEMA);
  private final Identifier DATABASE = Identifier.from(DATABASE_NAME);
  private final SchemaName schemaFqn = SchemaName.from(DATABASE, SCHEMA);
  private final SchemaName schema = SchemaName.from(SCHEMA);

  private TableLister tableLister;

  @BeforeAll
  void beforeAll() {
    tableLister = new DefaultTableLister(session);

    session.sql(format("create or replace schema %s", TEST_SCHEMA)).collect();
    session.sql(format("use schema %s", TEST_SCHEMA)).collect();
    createTestTables();
  }

  private void createTestTables() {
    var createTableTmpl = "create table %s(c1 varchar)";
    Stream.of(referenceTableNames)
        .forEach(tableName -> session.sql(format(createTableTmpl, tableName)).collect());
  }

  @Test
  void shouldShowAllTablesInSchema() {

    // when
    var tables = tableLister.showTables(schemaFqn);
    var fetchedNames = getStringNames(tables);

    // then
    assertThat(tables.size()).isEqualTo(referenceTableNames.length);
    assertThat(fetchedNames).containsExactlyInAnyOrder(referenceTableNames);
  }

  @Test
  void shouldShowAllTablesInDefaultDatabase() {

    // when
    var tables = tableLister.showTables(schema);
    var fetchedNames = getStringNames(tables);

    // then
    assertThat(tables).hasSize(referenceTableNames.length);
    assertThat(fetchedNames).containsExactlyInAnyOrder(referenceTableNames);
  }

  @Test
  void shouldShowOnlyMatchingTablesInSchema() {

    // when
    var tables = tableLister.showTables(schemaFqn, "tbl1");
    var fetchedNames = getStringNames(tables);
    // then
    assertThat(tables.size()).isEqualTo(1);
    assertThat(fetchedNames).containsExactlyInAnyOrder("TBL1");

    // when
    tables = tableLister.showTables(schemaFqn, "%bl%");
    fetchedNames = getStringNames(tables);
    // then
    assertThat(tables.size()).isEqualTo(3);
    assertThat(fetchedNames).containsExactlyInAnyOrder(referenceTableNames);

    // when
    tables = tableLister.showTables(schemaFqn, "tbl%");
    fetchedNames = getStringNames(tables);
    // then
    assertThat(tables.size()).isEqualTo(3);
    assertThat(fetchedNames).containsExactlyInAnyOrder(referenceTableNames);
  }

  @Test
  void shouldHandleTableWithSpecialCharacters() {
    // expect
    assertThatNoException().isThrownBy(() -> tableLister.showTables(schemaFqn, "a'b\"c❄d"));
  }

  private static String[] getStringNames(List<TableProperties> tables) {
    var fetchedNames = tables.stream().map(name -> name.getName()).toArray(String[]::new);
    return fetchedNames;
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.SchemaName;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultTableRepositoryTest extends BaseIntegrationTest {

  private static final String DATABASE =
      format("TEST_DB_%s", new Random().nextInt(Integer.MAX_VALUE));
  private static final String SCHEMA =
      format("TEST_SCHEMA_%s", new Random().nextInt(Integer.MAX_VALUE));
  private static final SchemaName schemaName = SchemaName.from(DATABASE, SCHEMA);
  private static final String TABLE_1 = "TABLE1";
  private static final String TABLE_2 = "TABLE2";
  private static final ObjectName table1ObjectName = ObjectName.from(DATABASE, SCHEMA, TABLE_1);
  private static final ObjectName table2ObjectName = ObjectName.from(DATABASE, SCHEMA, TABLE_2);

  private TableRepository repository;

  @BeforeAll
  void beforeAll() {
    repository = new DefaultTableRepository(session);
  }

  @BeforeEach
  void beforeEach() {
    session.sql(format("create or replace database %s", DATABASE)).collect();
    session.sql(format("create schema %s", SCHEMA)).collect();
    session.sql(format("create table %s(col1 varchar())", table1ObjectName.getValue())).collect();
    session.sql(format("insert into %s values ('test')", table1ObjectName.getValue())).collect();
  }

  @AfterAll
  void tearDown() {
    session.sql(format("drop database %s", DATABASE)).collect();
  }

  @Test
  void shouldDropTableIfExists() {

    // given
    var tables = repository.showTables(schemaName);
    assertThat(tables).hasSize(1);

    // when
    repository.dropTableIfExists(table1ObjectName);
    // second drop should be ok too because of IF EXISTS clause
    repository.dropTableIfExists(table1ObjectName);

    // then
    tables = repository.showTables(schemaName);
    assertThat(tables).hasSize(0);
  }

  @Test
  void tablePropertiesShouldBeMappedCorrectly() {

    // when
    var tables = repository.showTables(schemaName);
    TableProperties table1 = tables.get(0);

    // then
    assertThat(table1.getName()).isEqualTo(TABLE_1);
    assertThat(table1.getObjectName().getDatabase().isPresent());
    assertThat(table1.getObjectName().getDatabase().get().getValue()).isEqualTo(DATABASE);
    assertThat(table1.getObjectName().getSchema().isPresent());
    assertThat(table1.getObjectName().getSchema().get().getValue()).isEqualTo(SCHEMA);
    assertThat(table1.getObjectName().getName().getValue()).isEqualTo(TABLE_1);
    assertThat(table1.getCreatedOn()).isNotNull();
    assertThat(table1.getKind()).isEqualTo("TABLE");
    assertThat(table1.getOwner()).isNotEmpty();
    assertThat(table1.getRows()).isEqualTo(1);
  }

  @Test
  void renameTableShouldWorkCorrectly() {

    // given
    // this table should be created in beforeEach
    String nameBeforeRename = repository.showTables(schemaName).get(0).getName();
    String expectedNameAfterRename = table2ObjectName.getName().getValue();
    assertThat(nameBeforeRename).isNotEqualTo(expectedNameAfterRename);

    // when
    repository.renameTable(table1ObjectName, table2ObjectName);

    // then

    String actualNameAfterRename = repository.showTables(schemaName).get(0).getName();
    assertThat(actualNameAfterRename).isEqualTo(expectedNameAfterRename);
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.SchemaName;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultTableRepositoryTest extends BaseIntegrationTest {

  static final String DATABASE = format("TEST_DB_%s", new Random().nextInt(Integer.MAX_VALUE));
  static final String SCHEMA = format("TEST_SCHEMA_%s", new Random().nextInt(Integer.MAX_VALUE));
  static final SchemaName schemaName = SchemaName.from(DATABASE, SCHEMA);
  final ObjectName table1 = ObjectName.from(DATABASE, SCHEMA, "table1");
  TableRepository repository = new DefaultTableRepository(session);

  @BeforeEach
  void beforeEach() {
    session.sql(format("create or replace database %s", DATABASE)).collect();
    session.sql(format("create schema %s", SCHEMA)).collect();
    session.sql(format("create table %s(col1 varchar())", table1.getValue())).collect();
  }

  @AfterAll
  static void tearDown() {
    session.sql(format("drop database %s", DATABASE)).collect();
  }

  @Test
  void shouldDropTableIfExists() {

    // given
    var tables = repository.showTables(schemaName);
    assertThat(tables).hasSize(1);

    // when
    repository.dropTableIfExists(table1);
    // second drop should be ok too because of IF EXISTS clause
    repository.dropTableIfExists(table1);

    // then
    tables = repository.showTables(schemaName);
    assertThat(tables).hasSize(0);
  }
}

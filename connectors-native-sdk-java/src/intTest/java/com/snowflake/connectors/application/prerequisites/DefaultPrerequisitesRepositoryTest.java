/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.prerequisites;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.application.configuration.prerequisites.PrerequisitesRepository;
import com.snowflake.snowpark_java.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultPrerequisitesRepositoryTest extends BaseIntegrationTest {

  private PrerequisitesRepository repo;

  @BeforeAll
  void beforeAll() {
    repo = PrerequisitesRepository.getInstance(session);
  }

  @BeforeEach
  void beforeEach() {
    session.sql("TRUNCATE TABLE STATE.PREREQUISITES").collect();
    session
        .sql(
            "INSERT INTO STATE.PREREQUISITES (ID, TITLE, DESCRIPTION, POSITION, IS_COMPLETED)"
                + " VALUES  ('1', 'example', 'example', 2, true), ('2', 'example', 'example', 1,"
                + " true) ")
        .collect();
  }

  @Test
  public void shouldMarkAllPrerequisitesAsNotCompleted() {
    // when
    repo.markAllPrerequisitesAsUndone();

    // then
    var result = session.sql("SELECT id, is_completed FROM STATE.PREREQUISITES").collect();
    assertThat(result).containsExactlyInAnyOrder(Row.create("1", false), Row.create("2", false));
  }
}

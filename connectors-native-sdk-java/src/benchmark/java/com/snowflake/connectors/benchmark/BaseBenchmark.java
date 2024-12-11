/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.benchmark;

import static java.lang.String.format;

import com.snowflake.connectors.SnowparkSessionProvider;
import com.snowflake.connectors.application.Application;
import com.snowflake.connectors.application.BenchmarkApplication;
import com.snowflake.connectors.common.object.Reference;
import com.snowflake.snowpark_java.Session;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.Random.class)
public class BaseBenchmark {

  protected static final String WAREHOUSE = "XSMALL";
  protected static final String TASK_REACTOR_INSTANCE_NAME = "TR_BENCHMARK_INSTANCE";
  protected static final Reference WAREHOUSE_REFERENCE = Reference.from("WAREHOUSE_REFERENCE");

  protected Session session;
  protected Application application;

  @BeforeAll
  public void beforeAll() throws IOException {
    session = SnowparkSessionProvider.createSession();
    application = BenchmarkApplication.createNewInstance(session);

    session.sql("USE DATABASE " + application.instanceName).collect();
    session.sql("USE SCHEMA PUBLIC").collect();

    application.grantUsageOnWarehouse(WAREHOUSE);
    application.grantExecuteTaskPrivilege();
  }

  protected void setupWarehouseReference() {
    session
        .sql(
            format(
                "CALL PUBLIC.REGISTER_REFERENCE('%s', 'ADD',"
                    + " SYSTEM$REFERENCE('WAREHOUSE', '%s', 'PERSISTENT', 'USAGE'))",
                WAREHOUSE_REFERENCE.getName(), WAREHOUSE))
        .collect();
  }
}

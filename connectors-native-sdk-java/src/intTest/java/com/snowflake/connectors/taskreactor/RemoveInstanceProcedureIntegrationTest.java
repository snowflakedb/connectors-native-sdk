/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.taskreactor.utils.Constants.INSTANCE_REGISTRY;
import static com.snowflake.connectors.taskreactor.utils.Constants.REMOVE_INSTANCE_PROCEDURE;
import static com.snowflake.connectors.taskreactor.utils.Constants.TASK_REACTOR_API_SCHEMA;
import static com.snowflake.connectors.taskreactor.utils.Constants.TASK_REACTOR_INSTANCES_SCHEMA_NAME;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callProcedure;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.lit;
import static java.lang.String.format;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.utils.TaskReactorTestInstance;
import org.junit.jupiter.api.Test;

public class RemoveInstanceProcedureIntegrationTest extends BaseTaskReactorIntegrationTest {

  @Test
  void shouldRemoveInstance() {
    // given
    String instanceName = "TR_TEST_INSTANCE";
    TaskReactorTestInstance.buildFromScratch(instanceName, session).createInstance();

    // when
    var response =
        callProcedure(
            session, TASK_REACTOR_API_SCHEMA, REMOVE_INSTANCE_PROCEDURE, asVarchar(instanceName));

    // then
    assertThat(response).hasOKResponseCode();
    instanceIsRemovedFromInstanceRegistry(instanceName);
    instanceSchemaIsRemoved(instanceName);
  }

  private void instanceSchemaIsRemoved(String instanceName) {
    var discoveredSchemasNumber =
        session.sql(format("SHOW SCHEMAS LIKE '%s'", instanceName)).count();
    assertThat(discoveredSchemasNumber).isZero();
  }

  @Test
  void shouldExecuteWithoutErrorAndReturnSuccessfulResponseWhenRemovingNonExistentInstance() {
    // given
    String instanceName = "NON_EXISTING_TR_TEST_INSTANCE";

    // when
    var response =
        callProcedure(
            session, TASK_REACTOR_API_SCHEMA, REMOVE_INSTANCE_PROCEDURE, asVarchar(instanceName));

    // then
    assertThat(response).hasOKResponseCode();
  }

  private void instanceIsRemovedFromInstanceRegistry(String instanceName) {
    var instancesInRegistryNumber =
        session
            .table(
                ObjectName.from(TASK_REACTOR_INSTANCES_SCHEMA_NAME, INSTANCE_REGISTRY).getValue())
            .where(col("INSTANCE_NAME").equal_to(lit(instanceName)))
            .count();
    assertThat(instancesInRegistryNumber).isZero();
  }
}

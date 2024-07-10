/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions;

import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinition;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.application.observability.IngestionRun;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.common.assertions.application.status.FullConnectorStatusAssert;
import com.snowflake.connectors.common.assertions.common.VariantAssert;
import com.snowflake.connectors.common.assertions.common.object.IdentifierAssert;
import com.snowflake.connectors.common.assertions.common.object.ObjectNameAssert;
import com.snowflake.connectors.common.assertions.common.object.ReferenceAssert;
import com.snowflake.connectors.common.assertions.common.object.SchemaNameAssert;
import com.snowflake.connectors.common.assertions.common.response.ConnectorResponseAssert;
import com.snowflake.connectors.common.assertions.common.response.ResponseMapAssert;
import com.snowflake.connectors.common.assertions.common.state.TestState;
import com.snowflake.connectors.common.assertions.common.state.TestStateAssert;
import com.snowflake.connectors.common.assertions.configurationRepository.TestConfig;
import com.snowflake.connectors.common.assertions.configurationRepository.TestConfigAssert;
import com.snowflake.connectors.common.assertions.ingestion.IngestionProcessAssert;
import com.snowflake.connectors.common.assertions.ingestion.IngestionRunAssert;
import com.snowflake.connectors.common.assertions.ingestion.definition.ResourceIngestionDefinitionAssert;
import com.snowflake.connectors.common.assertions.task.CommandAssert;
import com.snowflake.connectors.common.assertions.task.TaskPropertiesAssert;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.Reference;
import com.snowflake.connectors.common.object.SchemaName;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;

/** Entry point for custom native-sdk AssertJ based assertions. */
public class NativeSdkAssertions extends Assertions {

  /**
   * Creates a new assertion for the specified {@link TestConfig}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static TestConfigAssert assertThat(TestConfig actual) {
    return new TestConfigAssert(actual, TestConfigAssert.class);
  }

  public static final InstanceOfAssertFactory<IngestionProcess, IngestionProcessAssert>
      INGESTION_PROCESS =
          new InstanceOfAssertFactory<>(IngestionProcess.class, NativeSdkAssertions::assertThat);

  /**
   * Creates a new assertion for the specified {@link IngestionProcess}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static IngestionProcessAssert assertThat(IngestionProcess actual) {
    return new IngestionProcessAssert(actual, IngestionProcessAssert.class);
  }

  /**
   * Creates a new assertion for the specified {@link IngestionRun}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static IngestionRunAssert assertThat(IngestionRun actual) {
    return new IngestionRunAssert(actual, IngestionRunAssert.class);
  }

  /**
   * Creates a new assertion for the specified {@link ConnectorResponse}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static ConnectorResponseAssert assertThat(ConnectorResponse actual) {
    return new ConnectorResponseAssert(actual, ConnectorResponseAssert.class);
  }

  /**
   * Creates a new assertion for the specified {@link FullConnectorStatus}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static FullConnectorStatusAssert assertThat(FullConnectorStatus actual) {
    return new FullConnectorStatusAssert(actual, FullConnectorStatusAssert.class);
  }

  public static final InstanceOfAssertFactory<
          ResourceIngestionDefinition, ResourceIngestionDefinitionAssert>
      RESOURCE_INGESTION_DEFINITION =
          new InstanceOfAssertFactory<>(
              ResourceIngestionDefinition.class, NativeSdkAssertions::assertThat);

  /**
   * Creates a new assertion for the specified {@link ResourceIngestionDefinition}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static ResourceIngestionDefinitionAssert assertThat(
      ResourceIngestionDefinition<?, ?, ?, ?> actual) {
    return new ResourceIngestionDefinitionAssert(actual, ResourceIngestionDefinitionAssert.class);
  }

  /**
   * Creates a new assertion for the specified map representing a variant response.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static ResponseMapAssert assertThatResponseMap(Map<String, Variant> actual) {
    return new ResponseMapAssert(actual, ResponseMapAssert.class);
  }

  /**
   * Creates a new assertion for the specified {@link TestState}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static TestStateAssert assertThat(TestState actual) {
    return new TestStateAssert(actual, TestStateAssert.class);
  }

  /** Definition of {@link InstanceOfAssertFactory} for {@link TaskProperties} assertion classes. */
  public static InstanceOfAssertFactory<TaskProperties, TaskPropertiesAssert> TASK_PROPERTIES =
      new InstanceOfAssertFactory<>(TaskProperties.class, NativeSdkAssertions::assertThat);

  /**
   * Creates a new assertion for the specified {@link TaskProperties}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static TaskPropertiesAssert assertThat(TaskProperties actual) {
    return new TaskPropertiesAssert(actual, TaskPropertiesAssert.class);
  }

  /** Definition of {@link InstanceOfAssertFactory} for {@link Command} assertion classes. */
  public static InstanceOfAssertFactory<Command, CommandAssert> COMMAND =
      new InstanceOfAssertFactory<>(Command.class, NativeSdkAssertions::assertThat);

  /**
   * Creates a new assertion for the specified {@link Command}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static CommandAssert assertThat(Command actual) {
    return new CommandAssert(actual, CommandAssert.class);
  }

  /**
   * Creates a new assertion for the specified {@link Variant}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static VariantAssert assertThat(Variant actual) {
    return new VariantAssert(actual, VariantAssert.class);
  }

  public static final InstanceOfAssertFactory<Identifier, IdentifierAssert> IDENTIFIER =
      new InstanceOfAssertFactory<>(Identifier.class, NativeSdkAssertions::assertThat);

  /**
   * Creates a new assertion for the specified {@link Identifier}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static IdentifierAssert assertThat(Identifier actual) {
    return new IdentifierAssert(actual, IdentifierAssert.class);
  }

  public static final InstanceOfAssertFactory<SchemaName, SchemaNameAssert> SCHEMA_NAME =
      new InstanceOfAssertFactory<>(SchemaName.class, NativeSdkAssertions::assertThat);

  /**
   * Creates a new assertion for the specified {@link SchemaName}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static SchemaNameAssert assertThat(SchemaName actual) {
    return new SchemaNameAssert(actual, SchemaNameAssert.class);
  }

  public static final InstanceOfAssertFactory<ObjectName, ObjectNameAssert> OBJECT_NAME =
      new InstanceOfAssertFactory<>(ObjectName.class, NativeSdkAssertions::assertThat);

  /**
   * Creates a new assertion for the specified {@link ObjectName}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static ObjectNameAssert assertThat(ObjectName actual) {
    return new ObjectNameAssert(actual, ObjectNameAssert.class);
  }

  public static final InstanceOfAssertFactory<Reference, ReferenceAssert> REFERENCE =
      new InstanceOfAssertFactory<>(Reference.class, NativeSdkAssertions::assertThat);

  /**
   * Creates a new assertion for the specified {@link Reference}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static ReferenceAssert assertThat(Reference actual) {
    return new ReferenceAssert(actual, ReferenceAssert.class);
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions;

import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinition;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.application.observability.IngestionRun;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.common.assertions.application.status.FullConnectorStatusAssert;
import com.snowflake.connectors.common.assertions.common.VariantAssert;
import com.snowflake.connectors.common.assertions.common.response.ConnectorResponseAssert;
import com.snowflake.connectors.common.assertions.common.response.ResponseMapAssert;
import com.snowflake.connectors.common.assertions.common.state.TestState;
import com.snowflake.connectors.common.assertions.common.state.TestStateAssert;
import com.snowflake.connectors.common.assertions.configurationRepository.TestConfig;
import com.snowflake.connectors.common.assertions.configurationRepository.TestConfigAssert;
import com.snowflake.connectors.common.assertions.ingestion.IngestionProcessAssert;
import com.snowflake.connectors.common.assertions.ingestion.IngestionRunAssert;
import com.snowflake.connectors.common.assertions.ingestion.definition.ResourceIngestionDefinitionAssert;
import com.snowflake.connectors.common.assertions.task.TaskPropertiesAssert;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.assertj.core.api.Assertions;

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

  /**
   * Creates a new assertion for the specified {@link TaskProperties}.
   *
   * @param actual actual value
   * @return new assertion instance
   */
  public static TaskPropertiesAssert assertThat(TaskProperties actual) {
    return new TaskPropertiesAssert(actual, TaskPropertiesAssert.class);
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
}

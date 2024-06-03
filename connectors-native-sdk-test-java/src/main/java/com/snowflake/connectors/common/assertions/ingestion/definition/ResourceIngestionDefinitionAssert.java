/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.ingestion.definition;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinition;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link ResourceIngestionDefinition}. */
public class ResourceIngestionDefinitionAssert
    extends AbstractAssert<
        ResourceIngestionDefinitionAssert, ResourceIngestionDefinition<?, ?, ?, ?>> {

  public ResourceIngestionDefinitionAssert(
      ResourceIngestionDefinition<?, ?, ?, ?> resourceIngestionDefinition,
      Class<ResourceIngestionDefinitionAssert> selfType) {
    super(resourceIngestionDefinition, selfType);
  }

  /**
   * Asserts that this resource ingestion definition has an id equal to the specified value.
   *
   * @param id expected id
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert hasId(String id) {
    assertThat(actual.getId()).isEqualTo(id);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has a resource id equal to the specified value.
   *
   * @param resourceId expected resource id
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert hasResourceId(Variant resourceId) {
    assertThat(actual.getResourceId()).isEqualTo(resourceId);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has a name equal to the specified value.
   *
   * @param name expected name
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert hasName(String name) {
    assertThat(actual.getName()).isEqualTo(name);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has a resource metadata equal to the specified
   * value.
   *
   * @param resourceMetadata expected resource metadata
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert hasResourceMetadata(Variant resourceMetadata) {
    assertThat(actual.getResourceMetadata()).isEqualTo(resourceMetadata);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has ingestion configurations equal to the
   * specified value.
   *
   * @param ingestionConfigurations expected ingestion configurations
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert hasIngestionConfigurations(
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations) {
    assertThat(actual.getIngestionConfigurations()).isEqualTo(ingestionConfigurations);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has an enabled state equal to the specified
   * value.
   *
   * @param enabled expected enabled state
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert isEnabled(boolean enabled) {
    assertThat(actual.isEnabled()).isEqualTo(enabled);
    return this;
  }
}

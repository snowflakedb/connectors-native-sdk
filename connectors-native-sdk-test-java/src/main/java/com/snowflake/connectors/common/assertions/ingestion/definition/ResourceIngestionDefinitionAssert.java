/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.ingestion.definition;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinition;
import java.util.List;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

/** AssertJ based assertions for {@link ResourceIngestionDefinition}. */
public class ResourceIngestionDefinitionAssert<I, M, C, D>
    extends AbstractAssert<
        ResourceIngestionDefinitionAssert<I, M, C, D>, ResourceIngestionDefinition<I, M, C, D>> {

  /**
   * Creates a new {@link ResourceIngestionDefinitionAssert}.
   *
   * @param resourceIngestionDefinition asserted resource ingestion definition
   * @param selfType self type
   */
  public ResourceIngestionDefinitionAssert(
      ResourceIngestionDefinition<I, M, C, D> resourceIngestionDefinition,
      Class<ResourceIngestionDefinitionAssert> selfType) {
    super(resourceIngestionDefinition, selfType);
  }

  /**
   * Asserts that this resource ingestion definition has an id equal to the specified value.
   *
   * @param id expected id
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert<I, M, C, D> hasId(String id) {
    Assertions.assertThat(this.actual.getId()).isEqualTo(id);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has a resource id equal to the specified value.
   *
   * @param resourceId expected resource id
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert<I, M, C, D> hasResourceId(I resourceId) {
    Assertions.assertThat(this.actual.getResourceId()).isEqualTo(resourceId);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has a name equal to the specified value.
   *
   * @param name expected name
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert<I, M, C, D> hasName(String name) {
    Assertions.assertThat(this.actual.getName()).isEqualTo(name);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has a resource metadata equal to the specified
   * value.
   *
   * @param resourceMetadata expected resource metadata
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert<I, M, C, D> hasResourceMetadata(M resourceMetadata) {
    Assertions.assertThat(this.actual.getResourceMetadata()).isEqualTo(resourceMetadata);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has ingestion configurations equal to the
   * specified value.
   *
   * @param ingestionConfigurations expected ingestion configurations
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert<I, M, C, D> hasIngestionConfigurations(
      List<IngestionConfiguration<C, D>> ingestionConfigurations) {
    Assertions.assertThat(this.actual.getIngestionConfigurations())
        .isEqualTo(ingestionConfigurations);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has an enabled state equal to the specified
   * value.
   *
   * @param enabled expected enabled state
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert<I, M, C, D> isEnabled(boolean enabled) {
    Assertions.assertThat(this.actual.isEnabled()).isEqualTo(enabled);
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has an enabled state equal to true.
   *
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert<I, M, C, D> isEnabled() {
    Assertions.assertThat(this.actual.isEnabled()).isTrue();
    return this;
  }

  /**
   * Asserts that this resource ingestion definition has an enabled state equal to false.
   *
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert<I, M, C, D> isDisabled() {
    Assertions.assertThat(this.actual.isEnabled()).isFalse();
    return this;
  }

  /**
   * Returns an assertion for ingestion configuration of this resource ingestion definition.
   *
   * @return an assertion for ingestion configuration of this resource ingestion definition
   */
  public ListAssert<IngestionConfiguration<C, D>> andIngestionConfigurations() {
    return Assertions.assertThat(this.actual.getIngestionConfigurations());
  }

  /**
   * Asserts that this resource ingestion definition has a metadata satisfying the specified
   * requirement.
   *
   * @param requirement metadata requirement
   * @return this assertion
   */
  public ResourceIngestionDefinitionAssert<I, M, C, D> resourceMetadataSatisfies(
      Consumer<M> requirement) {
    requirement.accept(this.actual.getResourceMetadata());
    return this;
  }
}

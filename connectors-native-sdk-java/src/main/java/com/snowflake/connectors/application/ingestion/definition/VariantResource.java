/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/**
 * An implementation of {@link ResourceIngestionDefinition} using {@link Variant} for all
 * dependencies.
 */
public class VariantResource
    extends ResourceIngestionDefinition<Variant, Variant, Variant, Variant> {

  /**
   * Creates an empty {@link VariantResource}.
   *
   * <p>This constructor is used by the reflection-based mapping process and should not be used for
   * any other purpose.
   */
  public VariantResource() {}

  /**
   * Creates a new {@link VariantResource}, with no parent definition.
   *
   * @param id resource ingestion definition id
   * @param name resource name
   * @param enabled should the ingestion for the resource be enabled
   * @param resourceId properties which identify the resource in the source system
   * @param resourceMetadata resource metadata
   * @param ingestionConfigurations ingestion configurations
   */
  public VariantResource(
      String id,
      String name,
      boolean enabled,
      Variant resourceId,
      Variant resourceMetadata,
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations) {
    super(id, name, enabled, null, resourceId, resourceMetadata, ingestionConfigurations);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}

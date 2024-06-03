/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import com.snowflake.snowpark_java.Session;

/** Factory for instances of {@link ResourceIngestionDefinitionRepository}. */
public class ResourceIngestionDefinitionRepositoryFactory {

  /**
   * Creates a new instance of the default {@link ResourceIngestionDefinitionRepository}
   * implementation, created for the given {@link ResourceIngestionDefinition} implementation.
   *
   * <p>Default implementation of the repository uses:
   *
   * <ul>
   *   <li>default reflection-based resource ingestion definition mapper
   *   <li>default resource ingestion definition validator
   *   <li>{@code STATE.RESOURCE_INGESTION_DEFINITION} table for data storage
   * </ul>
   *
   * @param session Snowpark session object
   * @param resourceClass implementation of the {@link ResourceIngestionDefinition} class
   * @param <R> type of the resource, for which the repository will be created
   * @return new instance of the repository
   * @throws ResourceIngestionDefinitionInstantiationException when instance of {@link
   *     DefaultResourceIngestionDefinitionRepository} cannot be created
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <R extends ResourceIngestionDefinition<?, ?, ?, ?>>
      ResourceIngestionDefinitionRepository<R> create(Session session, Class<R> resourceClass) {
    ResourceIngestionDefinitionMapper<R, ?, ?, ?, ?> mapper =
        new ResourceIngestionDefinitionMapper(resourceClass);
    ResourceIngestionDefinitionValidator validator = new ResourceIngestionDefinitionValidator();
    return new DefaultResourceIngestionDefinitionRepository<>(session, mapper, validator);
  }
}

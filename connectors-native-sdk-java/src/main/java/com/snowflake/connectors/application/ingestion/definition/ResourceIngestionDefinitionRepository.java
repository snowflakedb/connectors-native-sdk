/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;

import com.snowflake.connectors.util.variant.VariantMapperException;
import com.snowflake.snowpark_java.Column;
import java.util.List;
import java.util.Optional;

/**
 * A repository for basic storage of the resource ingestion definitions, created for instances of
 * the specified {@link ResourceIngestionDefinition} implementations.
 *
 * @param <R> implementation of {@link ResourceIngestionDefinition}
 */
public interface ResourceIngestionDefinitionRepository<
    R extends ResourceIngestionDefinition<?, ?, ?, ?>> {

  /**
   * Fetches a resource ingestion definition with the specified id.
   *
   * @param id resource ingestion definition id
   * @return resource ingestion definition with the specified id
   */
  Optional<R> fetch(String id);

  /**
   * Fetches a resource ingestion definition with the specified resource id.
   *
   * @param resourceId properties which identify the resource in the source system
   * @return resource ingestion definition with the specified resource id
   * @throws VariantMapperException if the fetched object cannot be deserialized to the definition
   *     class of this repository
   */
  Optional<R> fetchByResourceId(Object resourceId);

  /**
   * Fetches all resource ingestion definitions with resource ingestion definition id contained
   * within the specified list of ids.
   *
   * @param ids resource ingestion definition ids
   * @return a list containing resource ingestion definitions matching the specified criteria
   * @throws VariantMapperException if the fetched object cannot be deserialized to the definition
   *     class of this repository
   */
  List<R> fetchAllById(List<String> ids);

  /**
   * Fetches all resource ingestion definitions which have the ingestion for the resource enabled.
   *
   * @return a list containing resource ingestion definitions matching the specified criteria
   * @throws VariantMapperException if the fetched object cannot be deserialized to the definition
   *     class of this repository
   */
  List<R> fetchAllEnabled();

  /**
   * Fetches all resource ingestion definitions satisfying the specified condition.
   *
   * @param condition resource ingestion definition condition
   * @return all resource ingestion definitions satisfying the specified condition
   * @throws VariantMapperException if the fetched object cannot be deserialized to the definition
   *     class of this repository
   */
  List<R> fetchAllWhere(Column condition);

  /**
   * Fetches all resource ingestion definitions.
   *
   * @return all resource ingestion definitions
   */
  default List<R> fetchAll() {
    return fetchAllWhere(lit("1").equal_to(lit("1")));
  }

  /**
   * Counts all resource ingestion definitions which have the ingestion for the resource enabled.
   *
   * @return number of resource ingestion definitions which have the ingestion for the resource
   *     enabled
   */
  long countEnabled();

  /**
   * Saves the specified resource ingestion definition.
   *
   * <p>If a definition with a specific resource ingestion definition id does not exist - it will be
   * created. If such definition already exists - it will be updated.
   *
   * @param resource resource ingestion definition
   * @throws ResourceIngestionDefinitionValidationException if validation of the specified resource
   *     ingestion definition failed
   */
  void save(R resource);

  /**
   * Saves the specified resource ingestion definitions.
   *
   * <p>If a definition with a specific resource ingestion definition id does not exist - it will be
   * created. If such definition already exists - it will be updated.
   *
   * @param resources resource ingestion definitions
   * @throws ResourceIngestionDefinitionValidationException if validation of the specified resource
   *     ingestion definitions failed
   */
  void saveMany(List<R> resources);
}

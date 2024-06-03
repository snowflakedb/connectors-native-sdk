/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion;

import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepositoryFactory;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Service for basic creation of ingestion resources. */
public interface CreateResourceService {

  /**
   * Creates an ingestion resource.
   *
   * <p>The resource creation process consists of:
   *
   * <ul>
   *   <li>persisting a new resource ingestion definition
   *   <li>persisting a new ingestion process for each ingestion configuration, if the {@code
   *       enabled} parameter is true
   * </ul>
   *
   * @param id resource ingestion definition id, if null - a random id will be generated
   * @param name resource name, usually displayed in the UI
   * @param enabled should the ingestion for the resource be enabled
   * @param resourceId properties which identify the resource in the source system
   * @param resourceMetadata additional resource metadata
   * @param ingestionConfigurations resource ingestion configurations, consistent with {@link
   *     com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration
   *     IngestionConfiguration}
   * @return a response with the code {@code OK} if the creation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse createResource(
      String id,
      String name,
      boolean enabled,
      Variant resourceId,
      Variant resourceMetadata,
      Variant ingestionConfigurations);

  /**
   * Returns a new instance of the default service implementation.
   *
   * <p>Default implementation of the service uses:
   *
   * <ul>
   *   <li>a default implementation of {@link
   *       com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository
   *       ResourceIngestionDefinitionRepository}
   *   <li>a default implementation of {@link
   *       com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository
   *       IngestionProcessRepository}
   * </ul>
   *
   * @param session Snowpark session object
   * @return a new service instance
   */
  static CreateResourceService getInstance(Session session) {
    var definitionRepository =
        ResourceIngestionDefinitionRepositoryFactory.create(session, VariantResource.class);
    var processRepository = new DefaultIngestionProcessRepository(session);
    return new DefaultCreateResourceService(definitionRepository, processRepository);
  }
}

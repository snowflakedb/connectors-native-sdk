/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion;

import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionValidationException;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.common.exception.InvalidInputException;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.variant.VariantMapperException;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Default implementation of {@link CreateResourceService} */
class DefaultCreateResourceService implements CreateResourceService {

  private final ResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository;
  private final IngestionProcessRepository ingestionProcessRepository;

  DefaultCreateResourceService(
      ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository,
      IngestionProcessRepository ingestionProcessRepository) {
    this.resourceIngestionDefinitionRepository = resourceIngestionDefinitionRepository;
    this.ingestionProcessRepository = ingestionProcessRepository;
  }

  @Override
  public ConnectorResponse createResource(
      String id,
      String name,
      boolean enabled,
      Variant resourceId,
      Variant resourceMetadata,
      Variant ingestionConfigurations) {
    validateIdDoesNotExist(id, resourceId);
    var resource =
        createVariantResource(
            id, name, enabled, resourceId, resourceMetadata, ingestionConfigurations);
    return createResourceBody(resource);
  }

  private ConnectorResponse createResourceBody(VariantResource resource) {
    createResourceIngestionDefinition(resource);
    if (resource.isEnabled()) {
      resource
          .getIngestionConfigurations()
          .forEach(
              ingestionConfiguration ->
                  createProcess(resource.getId(), ingestionConfiguration.getId()));
    }
    return createSuccessResponse(resource);
  }

  private void validateIdDoesNotExist(String id, Variant resourceId) {
    if (id != null && resourceIngestionDefinitionRepository.fetch(id).isPresent()) {
      throw new InvalidInputException(String.format("Resource with id '%s' already exists.", id));
    }
    if (resourceIngestionDefinitionRepository.fetchByResourceId(resourceId).isPresent()) {
      throw new InvalidInputException(
          String.format("Resource with resourceId '%s' already exists.", resourceId));
    }
  }

  private VariantResource createVariantResource(
      String id,
      String name,
      boolean enabled,
      Variant resourceId,
      Variant resourceMetadata,
      Variant ingestionConfigurations) {
    if (id == null) {
      id = UUID.randomUUID().toString();
    }
    List<IngestionConfiguration<Variant, Variant>> mappedIngestionConfigurations =
        mapIngestionConfigurations(ingestionConfigurations);
    return new VariantResource(
        id, name, enabled, resourceId, resourceMetadata, mappedIngestionConfigurations);
  }

  private List<IngestionConfiguration<Variant, Variant>> mapIngestionConfigurations(
      Variant ingestionConfigurations) {
    TypeFactory typeFactory = TypeFactory.defaultInstance();
    JavaType ingestionConfigurationType =
        typeFactory.constructParametricType(
            IngestionConfiguration.class, Variant.class, Variant.class);
    var ingestionConfigurationListType =
        typeFactory.constructParametricType(List.class, ingestionConfigurationType);
    try {
      return mapVariant(ingestionConfigurations, ingestionConfigurationListType);
    } catch (VariantMapperException e) {
      throw new InvalidInputException(
          "Provided ingestion configuration has invalid structure and cannot be processed.");
    }
  }

  private void createResourceIngestionDefinition(VariantResource resource) {
    try {
      resourceIngestionDefinitionRepository.save(resource);
    } catch (ResourceIngestionDefinitionValidationException e) {
      throw new InvalidInputException(e.getMessage());
    }
  }

  private void createProcess(
      String resourceIngestionDefinitionId, String ingestionConfigurationId) {
    ingestionProcessRepository.createProcess(
        resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "SCHEDULED", null);
  }

  private ConnectorResponse createSuccessResponse(VariantResource resource) {
    var additionalPayload = Map.of("id", new Variant(resource.getId()));
    return ConnectorResponse.success("Resource created", additionalPayload);
  }
}

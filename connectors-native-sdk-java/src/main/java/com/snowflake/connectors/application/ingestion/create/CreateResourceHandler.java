/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.create;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfigurationMapper;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionValidationException;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.common.exception.InvalidInputException;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Handler for the ingestion resource creation process. A new instance of the handler must be
 * created using {@link #builder(Session) the builder}.
 *
 * <p>For more information about the resource creation process see {@link #createResource(String,
 * String, boolean, Variant, Variant, Variant) createResource}.
 */
public class CreateResourceHandler {

  private final ConnectorErrorHelper errorHelper;
  private final ResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository;
  private final IngestionProcessRepository ingestionProcessRepository;
  private final PreCreateResourceCallback preCallback;
  private final PostCreateResourceCallback postCallback;
  private final CreateResourceValidator validator;
  private final TransactionManager transactionManager;

  CreateResourceHandler(
      ConnectorErrorHelper errorHelper,
      ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository,
      IngestionProcessRepository ingestionProcessRepository,
      PreCreateResourceCallback preCallback,
      PostCreateResourceCallback postCallback,
      CreateResourceValidator validator,
      TransactionManager transactionManager) {
    this.errorHelper = errorHelper;
    this.resourceIngestionDefinitionRepository = resourceIngestionDefinitionRepository;
    this.ingestionProcessRepository = ingestionProcessRepository;
    this.preCallback = preCallback;
    this.postCallback = postCallback;
    this.validator = validator;
    this.transactionManager = transactionManager;
  }

  /**
   * Default handler method for the {@code PUBLIC.CREATE_RESOURCE} procedure.
   *
   * @param session Snowpark session object
   * @param name resource name
   * @param resourceId properties which identify the resource in the source system
   * @param ingestionConfigurations resource ingestion configurations
   * @param id resource ingestion definition id
   * @param enabled should the ingestion for the resource be enabled
   * @param resourceMetadata additional resource metadata
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #createResource(String, String, boolean, Variant, Variant, Variant) createResource}
   */
  public static Variant createResource(
      Session session,
      String name,
      Variant resourceId,
      Variant ingestionConfigurations,
      String id,
      boolean enabled,
      Variant resourceMetadata) {
    var resourceHandler = CreateResourceHandler.builder(session).build();
    return resourceHandler
        .createResource(id, name, enabled, resourceId, resourceMetadata, ingestionConfigurations)
        .toVariant();
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The resource creation process consists of:
   *
   * <ul>
   *   <li>initial validation - whether a resource with given id already exists and whether the
   *       provided resource has a valid structure
   *   <li>{@link CreateResourceValidator#validate(VariantResource)}
   *   <li>{@link PreCreateResourceCallback#execute(VariantResource)}
   *   <li>persisting a new resource ingestion definition
   *   <li>persisting a new ingestion process for each ingestion configuration, if the {@code
   *       enabled} parameter is true
   *   <li>{@link PostCreateResourceCallback#execute(String)}
   * </ul>
   *
   * @param id resource ingestion definition id
   * @param name resource name
   * @param enabled should the ingestion for the resource be enabled
   * @param resourceId properties which identify the resource in the source system
   * @param resourceMetadata additional resource metadata
   * @param ingestionConfigurations resource ingestion configurations
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse createResource(
      String id,
      String name,
      boolean enabled,
      Variant resourceId,
      Variant resourceMetadata,
      Variant ingestionConfigurations) {
    return errorHelper.withExceptionLoggingAndWrapping(
        () ->
            createResourceBody(
                id, name, enabled, resourceId, resourceMetadata, ingestionConfigurations));
  }

  private ConnectorResponse createResourceBody(
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
    ConnectorResponse validateResponse = validator.validate(resource);
    if (validateResponse.isNotOk()) {
      return validateResponse;
    }

    ConnectorResponse preCallbackResponse = preCallback.execute(resource);
    if (preCallbackResponse.isNotOk()) {
      return preCallbackResponse;
    }

    transactionManager.withTransaction(
        () -> {
          createResourceIngestionDefinition(resource);
          if (resource.isEnabled()) {
            createIngestionProcesses(resource);
          }
        },
        ResourceCreationException::new);

    ConnectorResponse postCallbackResponse = postCallback.execute(resource.getId());
    if (postCallbackResponse.isNotOk()) {
      return postCallbackResponse.withAdditionalPayload("id", new Variant(resource.getId()));
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
        IngestionConfigurationMapper.map(ingestionConfigurations);
    return new VariantResource(
        id, name, enabled, resourceId, resourceMetadata, mappedIngestionConfigurations);
  }

  private void createResourceIngestionDefinition(VariantResource resource) {
    try {
      resourceIngestionDefinitionRepository.save(resource);
    } catch (ResourceIngestionDefinitionValidationException e) {
      throw new InvalidInputException(e.getMessage());
    }
  }

  private void createIngestionProcesses(VariantResource resource) {
    resource
        .getIngestionConfigurations()
        .forEach(
            ingestionConfiguration ->
                createProcess(resource.getId(), ingestionConfiguration.getId()));
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

  /**
   * Returns a new instance of {@link CreateResourceHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static CreateResourceHandlerBuilder builder(Session session) {
    return new CreateResourceHandlerBuilder(session);
  }
}

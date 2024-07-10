/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.update;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfigurationMapper;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.common.exception.InvalidInputException;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Handler for the process of updating a resource. A new instance of the handler must be created
 * using {@link #builder(Session) the builder}.
 *
 * <p>For more information about the update process see {@link #updateResource(String, Variant)}.
 */
public class UpdateResourceHandler {

  private static final String SUCCESS_MSG = "Resource successfully updated.";
  private final ConnectorErrorHelper errorHelper;
  private final IngestionProcessRepository ingestionProcessRepository;
  private final ResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository;
  private final TransactionManager transactionManager;
  private final UpdateResourceValidator validator;
  private final PreUpdateResourceCallback preCallback;
  private final PostUpdateResourceCallback postCallback;

  UpdateResourceHandler(
      ConnectorErrorHelper errorHelper,
      IngestionProcessRepository ingestionProcessRepository,
      ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository,
      TransactionManager transactionManager,
      UpdateResourceValidator validator,
      PreUpdateResourceCallback preCallback,
      PostUpdateResourceCallback postCallback) {
    this.errorHelper = errorHelper;
    this.ingestionProcessRepository = ingestionProcessRepository;
    this.resourceIngestionDefinitionRepository = resourceIngestionDefinitionRepository;
    this.transactionManager = transactionManager;
    this.validator = validator;
    this.preCallback = preCallback;
    this.postCallback = postCallback;
  }

  /**
   * Default handler method for the {@code PUBLIC.UPDATE_RESOURCE} procedure.
   *
   * @param session Snowpark session object
   * @param updatedIngestionConfigurations resource ingestion configurations
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #updateResource(String, Variant) updateResource}.
   */
  public static Variant updateResource(
      Session session,
      String resourceIngestionDefinitionId,
      Variant updatedIngestionConfigurations) {
    return builder(session)
        .build()
        .updateResource(resourceIngestionDefinitionId, updatedIngestionConfigurations)
        .toVariant();
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The resource update process consists of:
   *
   * <ul>
   *   <li>initial validation - whether a resource with given id already exists and whether the
   *       provided ingestion configurations have a valid structures
   *   <li>{@link UpdateResourceValidator#validate(String, List)}
   *   <li>{@link PreUpdateResourceCallback#execute(String, List)}
   *   <li>updating ingestion configurations for a particular resource ingestion definition
   *   <li>finishing all ingestion processes for removed ingestion configurations
   *   <li>scheduling ingestion process for updated and new ingestion configurations
   *   <li>{@link PostUpdateResourceCallback#execute(String, List)}
   * </ul>
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param updatedIngestionConfigurations resource ingestion configurations
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse updateResource(
      String resourceIngestionDefinitionId, Variant updatedIngestionConfigurations) {
    return errorHelper.withExceptionLoggingAndWrapping(
        () ->
            updateResourceBody(
                resourceIngestionDefinitionId,
                IngestionConfigurationMapper.map(updatedIngestionConfigurations)));
  }

  private ConnectorResponse updateResourceBody(
      String resourceIngestionDefinitionId,
      List<IngestionConfiguration<Variant, Variant>> updatedIngestionConfigurations) {
    ConnectorResponse validateResponse =
        validator.validate(resourceIngestionDefinitionId, updatedIngestionConfigurations);
    if (validateResponse.isNotOk()) {
      return validateResponse;
    }

    ConnectorResponse preCallbackResponse =
        preCallback.execute(resourceIngestionDefinitionId, updatedIngestionConfigurations);
    if (preCallbackResponse.isNotOk()) {
      return preCallbackResponse;
    }

    VariantResource resource = fetchResource(resourceIngestionDefinitionId);
    var obsoleteIngestionConfigurations = resource.getIngestionConfigurations();
    transactionManager.withTransaction(
        () -> {
          updateResourceIngestionConfiguration(resource, updatedIngestionConfigurations);
          if (resource.isEnabled()) {
            finishObsoleteIngestionProcesses(resource);
            scheduleNewIngestionProcesses(resource, obsoleteIngestionConfigurations);
          }
        },
        ResourceUpdateException::new);

    ConnectorResponse postCallbackResponse =
        postCallback.execute(resourceIngestionDefinitionId, updatedIngestionConfigurations);
    if (postCallbackResponse.isNotOk()) {
      return postCallbackResponse;
    }
    return ConnectorResponse.success(SUCCESS_MSG);
  }

  private void updateResourceIngestionConfiguration(
      VariantResource currentResource,
      List<IngestionConfiguration<Variant, Variant>> updatedIngestionConfigurations) {
    currentResource.setIngestionConfigurations(updatedIngestionConfigurations);
    resourceIngestionDefinitionRepository.save(currentResource);
  }

  private void scheduleNewIngestionProcesses(
      VariantResource updatedResource,
      List<IngestionConfiguration<Variant, Variant>> obsoleteIngestionConfigurations) {
    List<String> obsoleteIngestionConfigurationsIds =
        obsoleteIngestionConfigurations.stream()
            .map(IngestionConfiguration::getId)
            .collect(Collectors.toList());
    updatedResource.getIngestionConfigurations().stream()
        .filter(it -> !obsoleteIngestionConfigurationsIds.contains(it.getId()))
        .forEach(it -> createIngestionProcess(updatedResource, it));
  }

  private void finishObsoleteIngestionProcesses(VariantResource updatedResource) {
    var updatedIngestionConfigurationsIds =
        updatedResource.getIngestionConfigurations().stream()
            .map(IngestionConfiguration::getId)
            .collect(Collectors.toList());
    ingestionProcessRepository.fetchAllActive(updatedResource.getId()).stream()
        .filter(it -> !updatedIngestionConfigurationsIds.contains(it.getIngestionConfigurationId()))
        .forEach(it -> ingestionProcessRepository.endProcess(it.getId()));
  }

  private VariantResource fetchResource(String resourceIngestionDefinitionId) {
    return resourceIngestionDefinitionRepository
        .fetch(resourceIngestionDefinitionId)
        .orElseThrow(
            () ->
                new InvalidInputException(
                    String.format(
                        "Resource with resourceId '%s' does not exist",
                        resourceIngestionDefinitionId)));
  }

  private void createIngestionProcess(
      VariantResource resource, IngestionConfiguration<Variant, Variant> ingestionConfiguration) {
    Optional<IngestionProcess> lastProcess =
        ingestionProcessRepository.fetchLastFinished(
            resource.getId(), ingestionConfiguration.getId(), "DEFAULT");
    Variant metadata = lastProcess.map(IngestionProcess::getMetadata).orElse(null);

    ingestionProcessRepository.createProcess(
        resource.getId(), ingestionConfiguration.getId(), "DEFAULT", "SCHEDULED", metadata);
  }

  /**
   * Returns a new instance of {@link UpdateResourceHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static UpdateResourceHandlerBuilder builder(Session session) {
    return new UpdateResourceHandlerBuilder(session);
  }
}

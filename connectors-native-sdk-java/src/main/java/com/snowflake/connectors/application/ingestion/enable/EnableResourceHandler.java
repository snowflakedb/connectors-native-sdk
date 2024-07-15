/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
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
import java.util.Optional;

/**
 * Handler for the process of enabling a resource. A new instance of the handler must be created
 * using {@link #builder(Session) the builder}.
 *
 * <p>For more information about the disabling process see {@link #enableResource(String)}.
 */
public class EnableResourceHandler {

  private final ConnectorErrorHelper errorHelper;
  private final ResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository;
  private final IngestionProcessRepository ingestionProcessRepository;
  private final PreEnableResourceCallback preCallback;
  private final PostEnableResourceCallback postCallback;
  private final EnableResourceValidator validator;
  private final TransactionManager transactionManager;

  EnableResourceHandler(
      ConnectorErrorHelper errorHelper,
      ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository,
      IngestionProcessRepository ingestionProcessRepository,
      PreEnableResourceCallback preCallback,
      PostEnableResourceCallback postCallback,
      EnableResourceValidator validator,
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
   * Default handler method for the {@code PUBLIC.ENABLE_RESOURCE} procedure.
   *
   * @param session Snowpark session object
   * @param resourceIngestionDefinitionId id of a resource ingestion definition which should be
   *     enabled
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #enableResource(String)}
   */
  public static Variant enableResource(Session session, String resourceIngestionDefinitionId) {
    return builder(session).build().enableResource(resourceIngestionDefinitionId).toVariant();
  }

  /**
   * Enables a resource with given id.
   *
   * <p>The process of enabling a resource consists of:
   *
   * <ul>
   *   <li>{@link EnableResourceValidator#validate}
   *   <li>{@link PreEnableResourceCallback#execute}
   *   <li>changing 'enabled' flag of the resource ingestion definition to true
   *   <li>creating a new {@link IngestionProcess} for each ingestion configuration of given
   *       resource ingestion definition
   *   <li>{@link PostEnableResourceCallback#execute}
   * </ul>
   *
   * If a resource is already enabled, nothing is done. If a resource ingestion definition with a
   * given id does not exist, {@link InvalidInputException} is thrown.
   *
   * @param resourceIngestionDefinitionId id of resource ingestion definition
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse enableResource(String resourceIngestionDefinitionId) {
    return errorHelper.withExceptionLoggingAndWrapping(
        () -> enableResourceBody(resourceIngestionDefinitionId));
  }

  private ConnectorResponse enableResourceBody(String resourceIngestionDefinitionId) {
    VariantResource resource = fetchResource(resourceIngestionDefinitionId);

    if (resource.isEnabled()) {
      return ConnectorResponse.success();
    }

    ConnectorResponse validateResponse = validator.validate(resourceIngestionDefinitionId);
    if (validateResponse.isNotOk()) {
      return validateResponse;
    }

    ConnectorResponse preCallbackResponse = preCallback.execute(resourceIngestionDefinitionId);
    if (preCallbackResponse.isNotOk()) {
      return preCallbackResponse;
    }

    transactionManager.withTransaction(
        () -> {
          resource.setEnabled();
          resourceIngestionDefinitionRepository.save(resource);
          createIngestionProcesses(resource);
        },
        ResourceEnablingException::new);

    ConnectorResponse postCallbackResponse = postCallback.execute(resourceIngestionDefinitionId);
    if (postCallbackResponse.isNotOk()) {
      return postCallbackResponse;
    }
    return ConnectorResponse.success();
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

  private void createIngestionProcesses(VariantResource resource) {
    resource
        .getIngestionConfigurations()
        .forEach(
            ingestionConfiguration -> createIngestionProcess(resource, ingestionConfiguration));
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
   * Returns a new instance of {@link EnableResourceHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static EnableResourceHandlerBuilder builder(Session session) {
    return new EnableResourceHandlerBuilder(session);
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.disable;

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

/**
 * Handler for the process of disabling a resource. A new instance of the handler must be created
 * using {@link #builder(Session) the builder}.
 *
 * <p>For more information about the disabling process see {@link #disableResource(String)}.
 */
public class DisableResourceHandler {

  private final ConnectorErrorHelper errorHelper;
  private final ResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository;
  private final IngestionProcessRepository ingestionProcessRepository;
  private final PreDisableResourceCallback preCallback;
  private final PostDisableResourceCallback postCallback;
  private final TransactionManager transactionManager;

  DisableResourceHandler(
      ConnectorErrorHelper errorHelper,
      ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository,
      IngestionProcessRepository ingestionProcessRepository,
      PreDisableResourceCallback preCallback,
      PostDisableResourceCallback postCallback,
      TransactionManager transactionManager) {
    this.errorHelper = errorHelper;
    this.resourceIngestionDefinitionRepository = resourceIngestionDefinitionRepository;
    this.ingestionProcessRepository = ingestionProcessRepository;
    this.preCallback = preCallback;
    this.postCallback = postCallback;
    this.transactionManager = transactionManager;
  }

  /**
   * Default handler method for the {@code PUBLIC.DISABLE_RESOURCE} procedure.
   *
   * @param session Snowpark session object
   * @param resourceIngestionDefinitionId id of a resource ingestion definition which should be
   *     disabled
   * @return a variant representing the {@link ConnectorResponse}
   */
  public static Variant disableResource(Session session, String resourceIngestionDefinitionId) {
    return builder(session).build().disableResource(resourceIngestionDefinitionId).toVariant();
  }

  /**
   * Disables a resource with given id
   *
   * <p>The process of disabling a resource consists of:
   *
   * <ul>
   *   <li>calling {@link PreDisableResourceCallback}
   *   <li>changing 'enabled' flag of the resource ingestion definition to false
   *   <li>ending all active ingestion processes associated with the resource ingestion definition
   *   <li>calling {@link PostDisableResourceCallback}
   * </ul>
   *
   * If a resource is already disabled, nothing is done. If a resource ingestion definition with
   * given id does not exist, {@link InvalidInputException} is thrown.
   *
   * @param resourceIngestionDefinitionId id of resource ingestion definition
   */
  public ConnectorResponse disableResource(String resourceIngestionDefinitionId) {
    return errorHelper.withExceptionLoggingAndWrapping(
        () -> disableResourceBody(resourceIngestionDefinitionId));
  }

  private ConnectorResponse disableResourceBody(String resourceIngestionDefinitionId) {
    VariantResource resource = fetchResource(resourceIngestionDefinitionId);

    if (resource.isDisabled()) {
      return ConnectorResponse.success();
    }

    ConnectorResponse preCallbackResponse = preCallback.execute(resourceIngestionDefinitionId);
    if (preCallbackResponse.isNotOk()) {
      return preCallbackResponse;
    }

    transactionManager.withTransaction(
        () -> {
          endActiveIngestionProcesses(resource);
          resource.setDisabled();
          resourceIngestionDefinitionRepository.save(resource);
        },
        ResourceDisablingException::new);

    ConnectorResponse postCallbackResponse = postCallback.execute(resourceIngestionDefinitionId);
    if (postCallbackResponse.isNotOk()) {
      return postCallbackResponse;
    }
    return ConnectorResponse.success();
  }

  private VariantResource fetchResource(String id) {
    return resourceIngestionDefinitionRepository
        .fetch(id)
        .orElseThrow(
            () ->
                new InvalidInputException(
                    String.format("Resource with resourceId '%s' does not exist", id)));
  }

  private void endActiveIngestionProcesses(VariantResource resource) {
    List<IngestionProcess> processes = ingestionProcessRepository.fetchAllActive(resource.getId());
    processes.forEach(process -> ingestionProcessRepository.endProcess(process.getId()));
  }

  /**
   * Returns a new instance of {@link DisableResourceHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static DisableResourceHandlerBuilder builder(Session session) {
    return new DisableResourceHandlerBuilder(session);
  }
}

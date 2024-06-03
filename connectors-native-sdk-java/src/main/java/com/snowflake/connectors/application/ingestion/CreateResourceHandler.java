/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for the ingestion resource creation process. A new instance of the handler must be
 * created using {@link #builder(Session) the builder}.
 *
 * <p>For more information about the resource creation process see {@link
 * CreateResourceService#createResource(String, String, boolean, Variant, Variant, Variant)
 * createResource}.
 */
public class CreateResourceHandler {

  private final ConnectorErrorHelper errorHelper;
  private final CreateResourceService createResourceService;

  CreateResourceHandler(
      ConnectorErrorHelper errorHelper, CreateResourceService createResourceService) {
    this.createResourceService = createResourceService;
    this.errorHelper = errorHelper;
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
   * Returns a new instance of {@link CreateResourceHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static CreateResourceHandlerBuilder builder(Session session) {
    return new CreateResourceHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The handler logic consists of creating the new resource via the {@link
   * CreateResourceService#createResource(String, String, boolean, Variant, Variant, Variant)
   * createResource} method.
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
            createResourceService.createResource(
                id, name, enabled, resourceId, resourceMetadata, ingestionConfigurations));
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.update;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepositoryFactory;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link UpdateResourceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link PreUpdateResourceCallback}
 *   <li>{@link PostUpdateResourceCallback}
 *   <li>{@link UpdateResourceValidator}
 * </ul>
 */
public class UpdateResourceHandlerBuilder {

  ConnectorErrorHelper errorHelper;
  ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository;
  IngestionProcessRepository ingestionProcessRepository;
  PreUpdateResourceCallback preCallback;
  PostUpdateResourceCallback postCallback;
  UpdateResourceValidator validator;
  TransactionManager transactionManager;

  /**
   * Creates a new {@link UpdateResourceHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String)}
   *   <li>{@link TransactionManager} built using {@link TransactionManager#getInstance(Session)}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public UpdateResourceHandlerBuilder(Session session) {
    requireNonNull(session);

    this.errorHelper = ConnectorErrorHelper.buildDefault(session, "UPDATE_RESOURCE");
    this.resourceIngestionDefinitionRepository =
        ResourceIngestionDefinitionRepositoryFactory.create(session, VariantResource.class);
    this.ingestionProcessRepository = new DefaultIngestionProcessRepository(session);
    this.preCallback = new DefaultPreUpdateResourceCallback(session);
    this.postCallback = new DefaultPostUpdateResourceCallback(session);
    this.validator = new DefaultUpdateResourceValidator(session);
    this.transactionManager = TransactionManager.getInstance(session);
  }

  /** Constructor used by the test builder implementation. */
  UpdateResourceHandlerBuilder() {}

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public UpdateResourceHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the UpdateResourceValidator used to build the handler instance. It allows to add
   * connector-specific logic which validates whether a resource can be updated.
   *
   * @param validator custom callback implementation
   * @return this builder
   */
  public UpdateResourceHandlerBuilder withUpdateResourceValidator(
      UpdateResourceValidator validator) {
    this.validator = validator;
    return this;
  }

  /**
   * Sets the PreUpdateResourceCallback used to build the handler instance. It allows to add
   * connector-specific logic which is invoked before a resource is updated.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public UpdateResourceHandlerBuilder withPreUpdateResourceCallback(
      PreUpdateResourceCallback callback) {
    this.preCallback = callback;
    return this;
  }

  /**
   * Sets the PostUpdateResourceCallback used to build the handler instance. It allows to add
   * connector-specific logic which is invoked after a resource is updated.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public UpdateResourceHandlerBuilder withPostUpdateResourceCallback(
      PostUpdateResourceCallback callback) {
    this.postCallback = callback;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public UpdateResourceHandler build() {
    requireNonNull(errorHelper);
    requireNonNull(resourceIngestionDefinitionRepository);
    requireNonNull(ingestionProcessRepository);
    requireNonNull(preCallback);
    requireNonNull(postCallback);
    requireNonNull(validator);
    requireNonNull(transactionManager);

    return new UpdateResourceHandler(
        errorHelper,
        ingestionProcessRepository,
        resourceIngestionDefinitionRepository,
        transactionManager,
        validator,
        preCallback,
        postCallback);
  }
}

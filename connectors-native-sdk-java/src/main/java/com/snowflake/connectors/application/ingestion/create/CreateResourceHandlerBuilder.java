/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.create;

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
 * Builder for the {@link CreateResourceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link PreCreateResourceCallback}
 *   <li>{@link PostCreateResourceCallback}
 *   <li>{@link CreateResourceValidator}
 * </ul>
 */
public class CreateResourceHandlerBuilder {

  ConnectorErrorHelper errorHelper;
  ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository;
  IngestionProcessRepository ingestionProcessRepository;
  PreCreateResourceCallback preCallback;
  PostCreateResourceCallback postCallback;
  CreateResourceValidator validator;
  TransactionManager transactionManager;

  /**
   * Creates a new {@link CreateResourceHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>Default implementation of {@link PreCreateResourceCallback}
   *   <li>Default implementation of {@link PostCreateResourceCallback}
   *   <li>Default implementation of {@link CreateResourceValidator}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public CreateResourceHandlerBuilder(Session session) {
    requireNonNull(session);

    this.errorHelper = ConnectorErrorHelper.buildDefault(session, "CREATE_RESOURCE");
    this.resourceIngestionDefinitionRepository =
        ResourceIngestionDefinitionRepositoryFactory.create(session, VariantResource.class);
    this.ingestionProcessRepository = new DefaultIngestionProcessRepository(session);
    this.preCallback = new DefaultPreCreateResourceCallback(session);
    this.postCallback = new DefaultPostCreateResourceCallback(session);
    this.validator = new DefaultCreateResourceValidator(session);
    this.transactionManager = TransactionManager.getInstance(session);
  }

  /** Constructor used by the test builder implementation. */
  CreateResourceHandlerBuilder() {}

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public CreateResourceHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the CreateResourceValidator used to build the handler instance. It allows to add
   * connector-specific logic which validates whether a resource can be created.
   *
   * @param validator custom callback implementation
   * @return this builder
   */
  public CreateResourceHandlerBuilder withCreateResourceValidator(
      CreateResourceValidator validator) {
    this.validator = validator;
    return this;
  }

  /**
   * Sets the PreCreateResourceCallback used to build the handler instance. It allows to add
   * connector-specific logic which is invoked before a resource is created.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public CreateResourceHandlerBuilder withPreCreateResourceCallback(
      PreCreateResourceCallback callback) {
    this.preCallback = callback;
    return this;
  }

  /**
   * Sets the PostCreateResourceCallback used to build the handler instance. It allows to add
   * connector-specific logic which is invoked after a resource is created.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public CreateResourceHandlerBuilder withPostCreateResourceCallback(
      PostCreateResourceCallback callback) {
    this.postCallback = callback;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public CreateResourceHandler build() {
    requireNonNull(errorHelper);
    requireNonNull(resourceIngestionDefinitionRepository);
    requireNonNull(ingestionProcessRepository);
    requireNonNull(preCallback);
    requireNonNull(postCallback);
    requireNonNull(validator);
    requireNonNull(transactionManager);

    return new CreateResourceHandler(
        errorHelper,
        resourceIngestionDefinitionRepository,
        ingestionProcessRepository,
        preCallback,
        postCallback,
        validator,
        transactionManager);
  }
}

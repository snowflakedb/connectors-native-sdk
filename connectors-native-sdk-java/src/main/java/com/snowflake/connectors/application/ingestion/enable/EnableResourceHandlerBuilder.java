/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

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
 * Builder for the {@link EnableResourceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link PreEnableResourceCallback}
 *   <li>{@link PostEnableResourceCallback}
 *   <li>{@link EnableResourceValidator}
 * </ul>
 */
public class EnableResourceHandlerBuilder {

  ConnectorErrorHelper errorHelper;
  ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository;
  IngestionProcessRepository ingestionProcessRepository;
  PreEnableResourceCallback preCallback;
  PostEnableResourceCallback postCallback;
  EnableResourceValidator validator;
  TransactionManager transactionManager;

  /**
   * Creates a new {@link EnableResourceHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public EnableResourceHandlerBuilder(Session session) {
    requireNonNull(session);

    this.errorHelper = ConnectorErrorHelper.buildDefault(session, "ENABLE_RESOURCE");
    this.resourceIngestionDefinitionRepository =
        ResourceIngestionDefinitionRepositoryFactory.create(session, VariantResource.class);
    this.ingestionProcessRepository = new DefaultIngestionProcessRepository(session);
    this.preCallback = new DefaultPreEnableResourceCallback(session);
    this.postCallback = new DefaultPostEnableResourceCallback(session);
    this.validator = new DefaultEnableResourceValidator(session);
    this.transactionManager = TransactionManager.getInstance(session);
  }

  /** Constructor used by the test builder implementation. */
  EnableResourceHandlerBuilder() {}

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public EnableResourceHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the EnableResourceValidator used to build the handler instance. It allows to add
   * connector-specific logic which validates whether a resource can be enabled.
   *
   * @param validator custom callback implementation
   * @return this builder
   */
  public EnableResourceHandlerBuilder withEnableResourceValidator(
      EnableResourceValidator validator) {
    this.validator = validator;
    return this;
  }

  /**
   * Sets the PreEnableResourceCallback used to build the handler instance. It allows to add
   * connector-specific logic which is invoked before a resource is enabled.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public EnableResourceHandlerBuilder withPreEnableResourceCallback(
      PreEnableResourceCallback callback) {
    this.preCallback = callback;
    return this;
  }

  /**
   * Sets the PostEnableResourceCallback used to build the handler instance. It allows to add
   * connector-specific logic which is invoked after a resource is enabled.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public EnableResourceHandlerBuilder withPostEnableResourceCallback(
      PostEnableResourceCallback callback) {
    this.postCallback = callback;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public EnableResourceHandler build() {
    requireNonNull(errorHelper);
    requireNonNull(resourceIngestionDefinitionRepository);
    requireNonNull(ingestionProcessRepository);
    requireNonNull(preCallback);
    requireNonNull(postCallback);
    requireNonNull(validator);
    requireNonNull(transactionManager);

    return new EnableResourceHandler(
        errorHelper,
        resourceIngestionDefinitionRepository,
        ingestionProcessRepository,
        preCallback,
        postCallback,
        validator,
        transactionManager);
  }
}

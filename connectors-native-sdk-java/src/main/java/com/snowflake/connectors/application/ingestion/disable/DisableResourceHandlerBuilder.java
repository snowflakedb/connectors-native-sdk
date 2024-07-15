/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.disable;

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
 * Builder for the {@link DisableResourceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link PreDisableResourceCallback}
 *   <li>{@link PostDisableResourceCallback}
 * </ul>
 */
public class DisableResourceHandlerBuilder {

  ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository;
  IngestionProcessRepository ingestionProcessRepository;
  PreDisableResourceCallback preCallback;
  PostDisableResourceCallback postCallback;
  ConnectorErrorHelper errorHelper;
  TransactionManager transactionManager;

  /**
   * Creates a new {@link DisableResourceHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>default implementation of {@link PreDisableResourceCallback}
   *   <li>default implementation of {@link PostDisableResourceCallback}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public DisableResourceHandlerBuilder(Session session) {
    requireNonNull(session);

    this.errorHelper = ConnectorErrorHelper.buildDefault(session, "DISABLE_RESOURCE");
    this.resourceIngestionDefinitionRepository =
        ResourceIngestionDefinitionRepositoryFactory.create(session, VariantResource.class);
    this.ingestionProcessRepository = new DefaultIngestionProcessRepository(session);
    this.preCallback = new DefaultPreDisableResourceCallback(session);
    this.postCallback = new DefaultPostDisableResourceCallback(session);
    this.transactionManager = TransactionManager.getInstance(session);
  }

  /** Constructor used by the test builder implementation. */
  DisableResourceHandlerBuilder() {}

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public DisableResourceHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the PreDisableResourceCallback used to build the handler instance. It allows to add
   * connector-specific logic which is invoked before a resource is disabled.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public DisableResourceHandlerBuilder withPreDisableResourceCallback(
      PreDisableResourceCallback callback) {
    this.preCallback = callback;
    return this;
  }

  /**
   * Sets the PostDisableResourceCallback used to build the handler instance. It allows to add
   * connector-specific logic which is invoked after a resource is disabled.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public DisableResourceHandlerBuilder withPostDisableResourceCallback(
      PostDisableResourceCallback callback) {
    this.postCallback = callback;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public DisableResourceHandler build() {
    requireNonNull(errorHelper);
    requireNonNull(resourceIngestionDefinitionRepository);
    requireNonNull(ingestionProcessRepository);
    requireNonNull(preCallback);
    requireNonNull(postCallback);
    requireNonNull(transactionManager);

    return new DisableResourceHandler(
        errorHelper,
        resourceIngestionDefinitionRepository,
        ingestionProcessRepository,
        preCallback,
        postCallback,
        transactionManager);
  }
}

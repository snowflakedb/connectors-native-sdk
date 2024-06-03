/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepositoryFactory;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link CreateResourceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class CreateResourceHandlerBuilder {

  ConnectorErrorHelper errorHelper;
  ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository;
  IngestionProcessRepository ingestionProcessRepository;

  /**
   * Creates a new {@link CreateResourceHandlerBuilder}.
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
  public CreateResourceHandlerBuilder(Session session) {
    requireNonNull(session);

    this.errorHelper = ConnectorErrorHelper.buildDefault(session, "CREATE_RESOURCE");
    this.resourceIngestionDefinitionRepository =
        ResourceIngestionDefinitionRepositoryFactory.create(session, VariantResource.class);
    this.ingestionProcessRepository = new DefaultIngestionProcessRepository(session);
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
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public CreateResourceHandler build() {
    requireNonNull(errorHelper);
    requireNonNull(resourceIngestionDefinitionRepository);
    requireNonNull(ingestionProcessRepository);

    var createResourceService =
        new DefaultCreateResourceService(
            resourceIngestionDefinitionRepository, ingestionProcessRepository);
    return new CreateResourceHandler(errorHelper, createResourceService);
  }
}

/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion;

import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link CreateResourceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link ResourceIngestionDefinitionRepository}
 *   <li>{@link IngestionProcessRepository}
 * </ul>
 */
public class CreateResourceHandlerTestBuilder extends CreateResourceHandlerBuilder {

  /**
   * Creates a new, empty {@link CreateResourceHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public CreateResourceHandlerTestBuilder() {}

  /**
   * Creates a new {@link CreateResourceHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link ResourceIngestionDefinitionRepository}, created for
   *       the {@link VariantResource}
   *   <li>a default implementation of {@link IngestionProcessRepository}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public CreateResourceHandlerTestBuilder(Session session) {
    super(session);
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public CreateResourceHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    super.withErrorHelper(errorHelper);
    return this;
  }

  /**
   * Sets the resource ingestion definition repository used to build the handler instance.
   *
   * @param resourceIngestionDefinitionRepository resource ingestion definition repository
   * @return this builder
   */
  public CreateResourceHandlerTestBuilder withResourceIngestionDefinitionRepository(
      ResourceIngestionDefinitionRepository<VariantResource>
          resourceIngestionDefinitionRepository) {
    this.resourceIngestionDefinitionRepository = resourceIngestionDefinitionRepository;
    return this;
  }

  /**
   * Sets the ingestion process repository used to build the handler instance.
   *
   * @param ingestionProcessRepository ingestion process repository
   * @return this builder
   */
  public CreateResourceHandlerTestBuilder withIngestionProcessRepository(
      IngestionProcessRepository ingestionProcessRepository) {
    this.ingestionProcessRepository = ingestionProcessRepository;
    return this;
  }
}

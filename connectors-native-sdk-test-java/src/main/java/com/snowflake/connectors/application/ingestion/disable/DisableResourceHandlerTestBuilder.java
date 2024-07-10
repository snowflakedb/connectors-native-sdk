/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.disable;

import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link DisableResourceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link ResourceIngestionDefinitionRepository}
 *   <li>{@link IngestionProcessRepository}
 *   <li>{@link TransactionManager}
 * </ul>
 */
public class DisableResourceHandlerTestBuilder extends DisableResourceHandlerBuilder {

  /**
   * Creates a new, empty {@link DisableResourceHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public DisableResourceHandlerTestBuilder() {}

  /**
   * Creates a new {@link DisableResourceHandlerTestBuilder}.
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
  public DisableResourceHandlerTestBuilder(Session session) {
    super(session);
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public DisableResourceHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    super.withErrorHelper(errorHelper);
    return this;
  }

  /**
   * Sets the resource ingestion definition repository used to build the handler instance.
   *
   * @param resourceIngestionDefinitionRepository resource ingestion definition repository
   * @return this builder
   */
  public DisableResourceHandlerTestBuilder withResourceIngestionDefinitionRepository(
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
  public DisableResourceHandlerTestBuilder withIngestionProcessRepository(
      IngestionProcessRepository ingestionProcessRepository) {
    this.ingestionProcessRepository = ingestionProcessRepository;
    return this;
  }

  /**
   * Sets the transaction manager used to build the handler instance.
   *
   * @param transactionManager transaction manager
   * @return this builder
   */
  public DisableResourceHandlerTestBuilder withTransactionManager(
      TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
    return this;
  }
}

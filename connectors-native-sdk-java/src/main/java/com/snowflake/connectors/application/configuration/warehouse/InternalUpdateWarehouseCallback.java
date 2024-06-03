/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;
import static com.snowflake.connectors.util.sql.SqlTools.varcharArgument;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link UpdateWarehouseCallback}. */
public class InternalUpdateWarehouseCallback implements UpdateWarehouseCallback {

  private final Session session;

  InternalUpdateWarehouseCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(Identifier warehouse) {
    return callPublicProcedure(
        session, "UPDATE_WAREHOUSE_INTERNAL", varcharArgument(warehouse.toSqlString()));
  }
}

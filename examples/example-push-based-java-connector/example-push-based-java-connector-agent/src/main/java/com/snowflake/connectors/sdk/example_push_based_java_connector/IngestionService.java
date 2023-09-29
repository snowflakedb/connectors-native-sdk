package com.snowflake.connectors.sdk.example_push_based_java_connector;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

class IngestionService {

  private final SnowflakeStreamingIngestClient client;
  private final String databaseName;
  private final String schemaName;

  IngestionService(Properties snowflakeIngestProperties, String databaseName, String schemaName) {
    this.client = buildIngestionClient(snowflakeIngestProperties);
    this.databaseName = databaseName;
    this.schemaName = schemaName;
  }

  private SnowflakeStreamingIngestClient buildIngestionClient(
      Properties snowflakeIngestProperties) {
    return SnowflakeStreamingIngestClientFactory.builder("CLIENT")
        .setProperties(snowflakeIngestProperties)
        .build();
  }

  CompletableFuture<Void> uploadRecords(String tableName, List<Map<String, Object>> records) {
    SnowflakeStreamingIngestChannel channel = openChannel(client, tableName);
    uploadRecords(channel, records);
    return channel.close();
  }

  private SnowflakeStreamingIngestChannel openChannel(
      SnowflakeStreamingIngestClient client, String tableName) {
    OpenChannelRequest request =
        OpenChannelRequest.builder(tableName + UUID.randomUUID())
            .setDBName(databaseName)
            .setSchemaName(schemaName)
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();
    return client.openChannel(request);
  }

  private void uploadRecords(
      SnowflakeStreamingIngestChannel channel, List<Map<String, Object>> records) {
    for (var record : records) {
      InsertValidationResponse response = channel.insertRow(record, null);
      if (response.hasErrors()) {
        throw response.getInsertErrors().get(0).getException();
      }
    }
  }

  void close() throws Exception {
    client.close();
  }
}

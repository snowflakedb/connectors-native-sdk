# Snowflake Native SDK for Connectors GitHub Example

This example shows how to implement a simple connector application based on the [template][template path]. This connector
uses GitHub API to retrieve the data about issues from a repository. This example includes all the basic steps of the Native SDK for Connectors flow.

Consult the step by step [tutorial][Example tutorial] to learn more about the GitHub example connector.

You can find the [template here][template path] and the implementation tutorial in the [documentation][Template tutorial]

## Developer flow

This example is ready to be deployed after minimal configurations from the user. The suggested flow is:

1. Clone the example
2. Build and deploy ([Connector lifecycle](CONNECTOR_LIFECYCLE.md) for more information)
3. Configure in Snowflake
4. Ingest the data!

[Example tutorial]: https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/tutorials/native_sdk_example_connector_tutorial
[Template tutorial]: https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/tutorials/native_sdk_template_connector_tutorial

[template path]: ../../templates/connectors-native-sdk-template

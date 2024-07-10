# Snowflake Native SDK for Connectors Template

Snowflake Native SDK for Connectors Template is a simple connector native application using Snowflake Native SDK for Connectors.
The purpose of this template is to provide a simple connector that can be used as a starting point when developing a new connector.
It can be deployed to a Snowflake environment out of the box and then each of its features can be customized according to more specific requirements.
It is recommended to get familiar with the [Snowflake Native SDK for Connectors tutorial][Tutorial docs]
and [Snowflake Native SDK for Connectors documentation][Native SDK official docs].

## How to use
The template is designed to act as deployment ready application, that mocks various functionalities of the actual connector,
starting with configuration and ending with the ingestion of data. This design allows the developer to implement features step by step
and be able to deploy and test it in Snowflake at any time.

To start a new project simply clone this directory and start implementation. Provided scripts will handle build and deployment process (more details below).

During implementation look for `TODO` comments present in the code. Some of them mark crucial parts of the connectors that need to be implemented,
while some of them only include hints or are needed in very specific scenarios.
The most important parts are described in the [tutorial][Tutorial docs].

Example connector using the template as a base can be found [here][Example Connector path].

## Developer flow
We recommend the following flow when using the tutorial, especially for the first time users:

1. Clone the template
2. Build and deploy the template ([Connector lifecycle](CONNECTOR_LIFECYCLE.md) for more information)
3. Customize features one by one using the [tutorial][Tutorial docs]
4. Redeploy and test your implementation in Snowflake
5. Ingest the data!

[Native SDK official docs]: https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/about-connector-sdk
[Tutorial docs]: https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/tutorials/native_sdk_tutorial

[Example Connector path]: ../../examples/connectors-native-sdk-example-java-github-connector

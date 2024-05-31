# Example connectors

The Native SDK for Connectors provides a couple of examples that show a general pattern, recommended by Snowflake, of 
creating new connectors as a [Native Applications][Native Apps docs]. 
From these examples, you will learn:
* how to create pull-based connectors ([learn more about pull-based pattern][Pull-based pattern])
* how to create push-based connectors ([learn more about push-based pattern][Push-based pattern])
* how to implement the connector configuration mechanism
* how to implement the ingestion mechanism
* how to create an instance of the application in the Snowflake environment
* how to use the [SnowSQL][SnowSQL guide] tool in connector development process

## Prerequisites

- Basic knowledge of [Snowflake Native Apps][Native Apps docs]
- Basic knowledge of SQL/SnowflakeScripting
- Snowflake user with `accountadmin` role

## Prepare your local environment

Before approaching example connectors, please take up the following steps or make sure that they are currently done:
- Install Java 11
- Install [SnowSQL][SnowSQL guide]
- Configure [SnowSQL][SnowSQL guide] to allow using [variables][SnowSQL variables] (`variable_substitution = True`)
- Configure [SnowSQL][SnowSQL guide] to [exit on first error][SnowSQL exit on error] (`exit_on_error = True`)

## Files structure

```text
├── connectors-native-sdk-example-java-github-connector
└── examples-basic
     ├── example-github-java-connector
     ├── example-github-python-connector
     └── example-push-based-java-connector
```

In the [connectors-native-sdk-example-java-github-connector][connectors-native-sdk-example-java-github-connector dir]
you can find an example that shows how to implement a simple connector application based on the [template][connectors-native-sdk template]
which uses the [Native SDK for Connectors Java library][connectors-native-sdk-java]. This connector uses GitHub API to 
retrieve the data about issues from a repository. This example includes all the basic steps of the Native SDK for 
Connectors flow.

In the [examples-basic][examples-basic dir] you can find example connectors that had been introduced before the [Native SDK 
for Connectors Java library][connectors-native-sdk-java] was created, so that's why you need keep in mind that these examples
do not use the SDK library. The goal of these connectors is to show the general concept of the connector application. 
You can find there the following examples:
* [example-github-java-connector][example-github-java-connector dir]
* [example-github-python-connector][example-github-python-connector dir]
* [example-push-based-java-connector][example-push-based-java-connector dir]



[Native Apps docs]: https://docs.snowflake.com/en/developer-guide/native-apps/native-apps-about
[SnowSQL guide]: https://docs.snowflake.com/en/user-guide/snowsql
[SnowSQL variables]: https://docs.snowflake.com/en/user-guide/snowsql-use#enabling-variable-substitution
[SnowSQL exit on error]: https://docs.snowflake.com/en/user-guide/snowsql-config#exit-on-error
[Pull-based pattern]: TODO:add_url_to_docs
[Push-based pattern]: TODO:add_url_to_docs

[connectors-native-sdk-example-java-github-connector dir]: ./connectors-native-sdk-example-java-github-connector
[example-github-java-connector dir]: ./examples-basic/example-github-java-connector
[example-github-python-connector dir]: ./examples-basic/example-github-python-connector
[example-push-based-java-connector dir]: ./examples-basic/example-push-based-java-connector

[examples-basic dir]: ./examples-basic
[connectors-native-sdk template]: ../templates/connectors-native-sdk-template
[connectors-native-sdk-java]: ../connectors-native-sdk-java

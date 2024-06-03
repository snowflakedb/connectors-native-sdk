APP_NAME=EXAMPLE_PUSH_BASED_CONNECTOR
CONNECTION=<YOUR CONNECTION>
APP_VERSION=V_1_0_0
STAGE_DB=${APP_NAME}_STAGE
STAGE_NAME=${STAGE_DB}.public.artifacts
WAREHOUSE=<YOUR WAREHOUSE>

.PHONY: test
test:
	./gradlew build cleanTest test

.PHONY: build_java_module
build_java_module:
	./gradlew build -x test

.PHONY: sf_build
sf_build:
	bash sf_build.sh

.PHONY: build
build: build_java_module sf_build

.PHONY: deploy
deploy:
	snowsql -f example-push-based-java-connector-native-app/scripts/deploy.sql -c $(CONNECTION) -D APP_NAME=${APP_NAME} -D STAGE_DB=${STAGE_DB} -D STAGE_NAME=${STAGE_NAME} -D APP_VERSION=${APP_VERSION}

.PHONY: install
install:
	snowsql -f example-push-based-java-connector-native-app/scripts/install.sql -c $(CONNECTION) -D APP_NAME=${APP_NAME} -D STAGE_NAME=${STAGE_NAME} -D APP_VERSION=${APP_VERSION}

.PHONY: run_agent
run_agent:
	java -jar sf_build_java/Agent.jar

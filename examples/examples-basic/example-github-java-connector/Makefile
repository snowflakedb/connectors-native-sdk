APP_NAME=EXAMPLE_GITHUB_JAVA_CONNECTOR
APP_VERSION=V_1_0_0
STAGE_DB=${APP_NAME}_STAGE
STAGE_NAME=${STAGE_DB}.public.artifacts

.PHONY: build_java_module
build_java_module:
	./gradlew build -x test

.PHONY: sf_build
sf_build:
	bash sf_build.sh

.PHONY: build
build: build_java_module sf_build

.PHONY: dev_deploy
deploy:
		snowsql -f scripts/install.sql -c $(CONNECTION) -D APP_NAME=${APP_NAME} -D STAGE_DB=${STAGE_DB} -D STAGE_NAME=${STAGE_NAME} -D APP_VERSION=${APP_VERSION}

install:
		snowsql -f scripts/setup.sql -c $(CONNECTION) -D token=${GITHUB_TOKEN}  -D APP_NAME=${APP_NAME} -D APP_VERSION=${APP_VERSION}

test:
		./gradlew test

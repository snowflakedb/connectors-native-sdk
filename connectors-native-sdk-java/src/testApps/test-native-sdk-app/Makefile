CONNECTION=native_sdk_connection

APP_PACKAGE_NAME=NATIVE_SDK_INTEGRATION_TEST_APP
SCHEMA_NAME=TEST_SCHEMA
STAGE_NAME=TEST_STAGE
VERSION=1_0
VERSION_DIR_PATH=@$(APP_PACKAGE_NAME).$(SCHEMA_NAME).$(STAGE_NAME)/$(VERSION)


.PHONY: publish_sdk_to_maven_local
publish_sdk_to_maven_local:
	bash scripts/publish-sdk-locally.sh

.PHONY: get_latest_released_sdk_version
get_latest_released_sdk_version:
	bash scripts/latest_released_sdk_version.sh

.PHONY: copy_internal_components
copy_internal_components:
	./gradlew copyInternalComponents

.PHONY: copy_sdk_components
copy_sdk_components:
	SDK_VERSION=$(CONNECTORS_NATIVE_SDK_VERSION) ./gradlew copySdkComponents

.PHONY: prepare_app_package
prepare_app_package:
	./gradlew prepareAppPackage \
		-Pconnection=$(CONNECTION) \
		-PappPackage=$(APP_PACKAGE_NAME) \
		-Pschema=$(SCHEMA_NAME) \
		-Pstage=$(STAGE_NAME)

.PHONY: deploy_connector
deploy_connector:
	./gradlew deployConnector \
		-Pconnection=$(CONNECTION) \
		-PappPackage=$(APP_PACKAGE_NAME) \
		-Pschema=$(SCHEMA_NAME) \
		-Pstage=$(STAGE_NAME) \
		-PappVersion=$(VERSION)

.PHONY: create_new_version
create_new_version:
	./gradlew createNewVersion \
		-Pconnection=$(CONNECTION) \
		-PappPackage=$(APP_PACKAGE_NAME) \
		-PversionDirPath=$(VERSION_DIR_PATH) \
		-PappVersion=$(VERSION)

.PHONY: complex_create_app_instance_from_app_version
build_and_create_app_version:
	make copy_internal_components
	make copy_sdk_components
	make prepare_app_package
	make deploy_connector
	make create_new_version

.PHONY: drop_application
drop_application:
	snowsql -c $(CONNECTION) \
		-q "DROP APPLICATION PACKAGE IF EXISTS $(APP_PACKAGE_NAME)"

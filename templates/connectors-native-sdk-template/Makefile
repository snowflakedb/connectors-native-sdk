CONNECTION=native_sdk_connection
USERNAME=$(shell whoami)
APP_PACKAGE_NAME=NATIVE_SDK_CONNECTOR_TEMPLATE_$(USERNAME)
INSTANCE_NAME=$(APP_PACKAGE_NAME)_INSTANCE
SCHEMA_NAME=TEST_SCHEMA
STAGE_NAME=TEST_STAGE
VERSION=1_0
VERSION_DIR_PATH=@$(APP_PACKAGE_NAME).$(SCHEMA_NAME).$(STAGE_NAME)/$(VERSION)

# ***********
#  CORE FLOW
# ***********

.PHONY: copy_internal_components
copy_internal_components:
	./gradlew copyInternalComponents

.PHONY: copy_sdk_components
copy_sdk_components:
	./gradlew copySdkComponents

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

# ****************************************
#  CREATE INSTANCE FROM VERSION DIRECTORY
# ****************************************

.PHONY: create_app_instance_from_version_dir
create_app_instance_from_version_dir:
	./gradlew createAppInstance \
		-Pconnection=$(CONNECTION) \
		-PappPackage=$(APP_PACKAGE_NAME) \
		-PversionDirPath=$(VERSION_DIR_PATH)

.PHONY: complex_create_app_instance_from_version_dir
complex_create_app_instance_from_version_dir:
	make copy_internal_components
	make copy_sdk_components
	make prepare_app_package
	make deploy_connector
	make create_app_instance_from_version_dir

# **********************************
#  CREATE INSTANCE FROM APP VERSION
# **********************************

.PHONY: create_new_version
create_new_version:
	./gradlew createNewVersion \
		-Pconnection=$(CONNECTION) \
		-PappPackage=$(APP_PACKAGE_NAME) \
		-PversionDirPath=$(VERSION_DIR_PATH) \
		-PappVersion=$(VERSION)

.PHONY: create_app_instance_from_app_version
create_app_instance_from_app_version:
	./gradlew createAppInstance \
		-Pconnection=$(CONNECTION) \
		-PappPackage=$(APP_PACKAGE_NAME) \
		-PappVersion=$(VERSION)

.PHONY: complex_create_app_instance_from_app_version
complex_create_app_instance_from_app_version:
	make copy_internal_components
	make copy_sdk_components
	make prepare_app_package
	make deploy_connector
	make create_new_version
	make create_app_instance_from_app_version

# ******************
#  ADDITIONAL TASKS
# ******************

.PHONY: drop_application_package
drop_application_package:
	snowsql -c $(CONNECTION) \
		-q "DROP APPLICATION PACKAGE IF EXISTS $(APP_PACKAGE_NAME)"

.PHONY: drop_application_instance
drop_application_instance:
	snowsql -c $(CONNECTION) \
		-q "DROP APPLICATION IF EXISTS $(INSTANCE_NAME) CASCADE"

.PHONY: drop_application
drop_application:
	snowsql -c $(CONNECTION) \
		-q "DROP APPLICATION IF EXISTS $(INSTANCE_NAME) CASCADE; DROP APPLICATION PACKAGE IF EXISTS $(APP_PACKAGE_NAME)"

.PHONY: reinstall_application_from_version_dir
reinstall_application_from_version_dir:
	make drop_application
	make complex_create_app_instance_from_version_dir

.PHONY: reinstall_application_from_app_version
reinstall_application_from_app_version:
	make drop_application
	make complex_create_app_instance_from_app_version


.PHONY: publish_sdk_to_maven_local
publish_sdk_to_maven_local:
	bash scripts/publish-sdk-locally.sh

.PHONY: get_latest_released_sdk_version
get_latest_released_sdk_version:
	bash scripts/latest_released_sdk_version.sh

# ***********
#  CORE FLOW
# ***********

.PHONY: copy_internal_components
copy_internal_components:
	./gradlew copyInternalComponents

.PHONY: copy_sdk_components
copy_sdk_components:
	SDK_VERSION=$(CONNECTORS_NATIVE_SDK_VERSION) ./gradlew copySdkComponents

# ****************************************
#  CREATE INSTANCE FROM VERSION DIRECTORY
# ****************************************

.PHONY: create_app_instance_from_version_dir
create_app_instance_from_version_dir:
	snow --config-file $(CONNECTION_FILE) app run --env APP_PACKAGE_NAME=$(APP_PACKAGE_NAME) --APP_NAME=$(APP_NAME)

.PHONY: complex_create_app_instance_from_version_dir
complex_create_app_instance_from_version_dir:
	make copy_internal_components
	make copy_sdk_components
	make create_app_instance_from_version_dir

# **********************************
#  CREATE INSTANCE FROM APP VERSION
# **********************************

.PHONY: deploy_app_package
deploy_app_package:
	snow --config-file $(CONNECTION_FILE) app deploy --env APP_PACKAGE_NAME=$(APP_PACKAGE_NAME) --env APP_NAME=''

.PHONY: create_new_version
create_new_version:
	snow --config-file $(CONNECTION_FILE) app version create '"$(VERSION)"' --env APP_PACKAGE_NAME=$(APP_PACKAGE_NAME) --env APP_NAME='' --force --skip-git-check

.PHONY: build_and_create_app_version
build_and_create_app_version:
	make copy_internal_components
	make copy_sdk_components
	make deploy_app_package
	make create_new_version

.PHONY: create_app_instance_from_app_version
create_app_instance_from_app_version:
	snow --config-file $(CONNECTION_FILE) app run --version '"$(VERSION)"' --env APP_PACKAGE_NAME=$(APP_PACKAGE_NAME) --env APP_NAME=$(APP_NAME)

.PHONY: complex_create_app_instance_from_app_version
complex_create_app_instance_from_app_version:
	make build_and_create_app_version
	make create_app_instance_from_app_version

# ******************
#  ADDITIONAL TASKS
# ******************

.PHONY: drop_application
drop_application:
	snow --config-file $(CONNECTION_FILE) app teardown --env APP_PACKAGE_NAME=$(APP_PACKAGE_NAME) --env APP_NAME='' --cascade --force

.PHONY: reinstall_application_from_version_dir
reinstall_application_from_version_dir:
	make drop_application
	make complex_create_app_instance_from_version_dir

.PHONY: reinstall_application_from_app_version
reinstall_application_from_app_version:
	make drop_application
	make complex_create_app_instance_from_app_version

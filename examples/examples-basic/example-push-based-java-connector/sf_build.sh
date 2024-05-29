# Copyright (c) 2024 Snowflake Inc.
export BUILD_DIR="sf_build"
export BUILD_DIR_JAVA="sf_build_java"

echo "Cleanup"
rm -rf "${BUILD_DIR}"
rm -rf "${BUILD_DIR_JAVA}"
mkdir "${BUILD_DIR}"
mkdir "${BUILD_DIR_JAVA}"

echo "Copying Java Module artifacts"
cp example-push-based-java-connector-agent/build/libs/*.jar ""${BUILD_DIR_JAVA}/Agent.jar""

echo "Copying Native App artifacts"
cp example-push-based-java-connector-native-app/manifest.yml "${BUILD_DIR}/"
cp example-push-based-java-connector-native-app/setup.sql "${BUILD_DIR}/"
cp example-push-based-java-connector-native-app/streamlit_app.py "${BUILD_DIR}/"
cp example-push-based-java-connector-native-app/environment.yml "${BUILD_DIR}/"

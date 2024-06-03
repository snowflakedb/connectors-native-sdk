# Copyright (c) 2024 Snowflake Inc.
export BUILD_DIR="sf_build"

echo "Cleanup"
rm -rf "sf_build"
mkdir "sf_build"

echo "Java module"
cp example-github-connector-java-module/build/libs/*.jar ""${BUILD_DIR}/MyJavaApp.jar""

echo "Snowflake artifacts"
cp manifest.yml "${BUILD_DIR}/"
cp setup.sql "${BUILD_DIR}/"
cp streamlit_app.py "${BUILD_DIR}/"

cp environment.yml "${BUILD_DIR}/"

export BUILD_DIR="sf_build"

echo "Cleanup"
rm -rf "sf_build"
mkdir "sf_build"

echo "Java module"
cp example-github-connector-java-module/build/libs/*.jar ""${BUILD_DIR}/MyJavaApp.jar""

echo "Snowflake artifacts"
cp manifest.yml "${BUILD_DIR}/"
snow render template setup.sql -o "${BUILD_DIR}/setup.sql"

cp streamlit_app.py "${BUILD_DIR}/"

cp environment.yml "${BUILD_DIR}/"

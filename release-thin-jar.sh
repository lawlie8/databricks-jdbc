#!/bin/bash
#
# Script to handle thin JAR release as part of the main release process
# This integrates with the existing GitHub Actions release workflow
#

set -e

echo "======================================"
echo "Databricks JDBC Thin JAR Release"
echo "======================================"

# Get version from pom.xml
VERSION=$(grep -m1 '<version>' pom.xml | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
echo "Version: $VERSION"

# Step 1: Generate thin-jar-pom.xml from main pom.xml
echo "Step 1: Generating thin-jar-pom.xml..."
./generate-thin-pom.sh

# Step 2: Verify thin JAR exists
THIN_JAR="target/databricks-jdbc-${VERSION}-thin.jar"
if [ ! -f "$THIN_JAR" ]; then
    echo "Error: Thin JAR not found at $THIN_JAR"
    echo "The main build should have created this file"
    exit 1
fi
echo "Found thin JAR: $THIN_JAR"

# Step 3: Deploy thin JAR with signing
echo "Step 3: Deploying thin JAR to Maven Central..."
mvn gpg:sign-and-deploy-file \
    -Dfile="$THIN_JAR" \
    -DpomFile=thin-jar-pom.xml \
    -DrepositoryId=central \
    -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ \
    -DgroupId=com.databricks \
    -DartifactId=databricks-jdbc-thin \
    -Dversion="$VERSION" \
    -Dpackaging=jar \
    -DgeneratePom=false \
    -DupdateReleaseInfo=true

echo "======================================"
echo "Thin JAR Release Complete!"
echo "======================================"
echo ""
echo "The thin JAR has been deployed to the Maven staging repository."
echo "It will be released along with the main artifact when you:"
echo "  1. Close the staging repository in Sonatype UI"
echo "  2. Release the staging repository"
echo ""
echo "Artifact coordinates:"
echo "  com.databricks:databricks-jdbc-thin:${VERSION}"
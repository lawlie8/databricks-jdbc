#!/bin/bash

# Script to generate thin-jar-pom.xml from pom.xml
# This ensures version consistency between main POM and thin JAR POM
# Following Snowflake's pattern: separate artifact with full dependencies

set -e

echo "======================================"
echo "Generating thin-jar-pom.xml"
echo "======================================"

# Extract version from pom.xml
VERSION=$(grep -m1 '<version>' pom.xml | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
echo "Version: $VERSION"

# Create thin-jar-pom.xml with proper artifactId
cat > thin-jar-pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.databricks</groupId>
  <artifactId>databricks-jdbc-thin</artifactId>
  <version>VERSION_PLACEHOLDER</version>
  <packaging>jar</packaging>
  <name>Databricks JDBC Driver Thin</name>
  <description>Databricks JDBC Driver Thin JAR - requires external dependencies.</description>
  <url>https://github.com/databricks/databricks-jdbc</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>https://github.com/databricks/databricks-jdbc/blob/main/LICENSE</url>
    </license>
  </licenses>
  <developers>
    <developer>
      <name>Databricks JDBC Team</name>
      <email>eng-oss-sql-driver@databricks.com</email>
      <organization>Databricks</organization>
      <organizationUrl>https://www.databricks.com</organizationUrl>
    </developer>
  </developers>
  <scm>
    <connection>scm:git:https://github.com/databricks/databricks-jdbc.git</connection>
    <developerConnection>scm:git:https://github.com/databricks/databricks-jdbc.git</developerConnection>
    <url>https://github.com/databricks/databricks-jdbc</url>
  </scm>
  <issueManagement>
    <system>GitHub Issues</system>
    <url>https://github.com/databricks/databricks-jdbc/issues</url>
  </issueManagement>
  
  <properties>
PROPERTIES_PLACEHOLDER
  </properties>
  
  <dependencyManagement>
    <dependencies>
      <!-- Force safe version of commons-lang3 https://nvd.nist.gov/vuln/detail/CVE-2025-48924 -->
      <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-lang3</artifactId>
        <version>${commons-lang3.version}</version>
      </dependency>
      <!-- Jackson BOM for version management -->
      <dependency>
        <groupId>com.fasterxml.jackson</groupId>
        <artifactId>jackson-bom</artifactId>
        <version>${jackson.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
  
  <dependencies>
DEPENDENCIES_PLACEHOLDER
  </dependencies>
</project>
EOF

# Replace version
sed -i "s/VERSION_PLACEHOLDER/$VERSION/g" thin-jar-pom.xml

# Extract properties (version management)
echo "Extracting version properties..."
PROPERTIES=$(awk '/<properties>/,/<\/properties>/' pom.xml | \
  grep -E '<[a-z.-]+\.version>' | \
  sed 's/^/    /')

# Replace properties placeholder
sed -i "/PROPERTIES_PLACEHOLDER/r /dev/stdin" thin-jar-pom.xml <<< "$PROPERTIES"
sed -i "/PROPERTIES_PLACEHOLDER/d" thin-jar-pom.xml

# Extract dependencies (excluding test scope and provided scope)
echo "Extracting runtime dependencies..."
DEPENDENCIES=$(awk '/<dependencies>/,/<\/dependencies>/' pom.xml | \
  awk '/<dependency>/,/<\/dependency>/' | \
  grep -v '<scope>test</scope>' | \
  grep -v '<scope>provided</scope>' | \
  sed 's/^/    /')

# Format dependencies properly
FORMATTED_DEPS=""
IN_DEP=0
CURRENT_DEP=""
while IFS= read -r line; do
  if [[ $line == *"<dependency>"* ]]; then
    IN_DEP=1
    CURRENT_DEP="    <dependency>\n"
  elif [[ $line == *"</dependency>"* ]]; then
    CURRENT_DEP="${CURRENT_DEP}    </dependency>\n"
    # Check if this dependency should be included (not test or provided)
    if ! echo "$CURRENT_DEP" | grep -q '<scope>test</scope>' && \
       ! echo "$CURRENT_DEP" | grep -q '<scope>provided</scope>'; then
      FORMATTED_DEPS="${FORMATTED_DEPS}${CURRENT_DEP}"
    fi
    IN_DEP=0
    CURRENT_DEP=""
  elif [[ $IN_DEP -eq 1 ]]; then
    # Properly indent dependency content
    TRIMMED=$(echo "$line" | sed 's/^[[:space:]]*//')
    if [[ ! -z "$TRIMMED" ]]; then
      CURRENT_DEP="${CURRENT_DEP}      ${TRIMMED}\n"
    fi
  fi
done <<< "$DEPENDENCIES"

# Write dependencies to temporary file
echo -e "$FORMATTED_DEPS" > /tmp/deps.tmp

# Replace dependencies placeholder
sed -i "/DEPENDENCIES_PLACEHOLDER/r /tmp/deps.tmp" thin-jar-pom.xml
sed -i "/DEPENDENCIES_PLACEHOLDER/d" thin-jar-pom.xml

# Clean up
rm -f /tmp/deps.tmp

echo "Generated thin-jar-pom.xml successfully!"
echo ""
echo "To deploy the thin JAR as a separate artifact:"
echo "1. Build: mvn clean package"
echo "2. Deploy locally: mvn deploy:deploy-file -Dfile=target/databricks-jdbc-*-thin.jar -DpomFile=thin-jar-pom.xml -DrepositoryId=local-test-repo -Durl=file://\${PWD}/target/local-repo"
echo "3. Deploy to Maven Central: Use the release process with proper signing"
#!/usr/bin/env bash
set -euo pipefail

JAR_DIR="$(cd "$(dirname "$0")/../flink-job/jars" && pwd)"
FLINK_VERSION="1.18.1"
SCALA_VERSION="2.12"
KAFKA_CONNECTOR_VERSION="3.2.0-1.18"

mkdir -p "${JAR_DIR}"

download() {
  local url="$1"
  local dest="$2"
  if [[ -f "${dest}" ]]; then
    echo "Already present: $(basename "${dest}")"
    return
  fi
  echo "Downloading $(basename "${dest}")..."
  curl -fsSL "${url}" -o "${dest}"
}

download \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
  "${JAR_DIR}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar" \
  "${JAR_DIR}/kafka-clients-3.4.0.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar" \
  "${JAR_DIR}/flink-json-${FLINK_VERSION}.jar"

echo "Flink connector JARs ready in ${JAR_DIR}"

#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

if [[ -z "${RANGER_VERSION:-}" ]]; then
  # shellcheck source=/dev/null
  source "${COMPOSE_DIR}/.env"
fi

# shellcheck source=/dev/null
source "${COMPOSE_DIR}/../testlib.sh"

: "${POLARIS_IMAGE:=apache/polaris:1.4.1}"
: "${SPARK_SQL_IMAGE:=apache/spark:3.5.7-scala2.12-java17-ubuntu}"
: "${POLARIS_CATALOG_NAME:=quickstart_catalog}"
: "${POLARIS_STORAGE_LOCATION:=s3://iceberg-obs/polaris-smoke}"
: "${POLARIS_ICEBERG_SPARK_RUNTIME_VERSION:=1.10.1}"
: "${ICEBERG_SVC_CATALOG_USER:=svc-iceberg-rest-catalog}"
: "${ICEBERG_SVC_CATALOG_PRINCIPAL:=${ICEBERG_SVC_CATALOG_USER}/s3g@EXAMPLE.COM}"
: "${ICEBERG_SVC_CATALOG_KEYTAB:=/etc/security/keytabs/${ICEBERG_SVC_CATALOG_USER}.keytab}"

export POLARIS_IMAGE SPARK_SQL_IMAGE POLARIS_CATALOG_NAME POLARIS_STORAGE_LOCATION

if [[ "${COMPOSE_FILE:-}" != *polaris.yaml* ]]; then
  export COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yaml:ranger.yaml:../common/ranger.yaml}:polaris.yaml"
fi

echo "Fetching permanent S3 credentials for ${ICEBERG_SVC_CATALOG_USER}..."
s3_secret_output="$(
  docker-compose exec -T s3g bash -lc \
    "kinit -kt ${ICEBERG_SVC_CATALOG_KEYTAB} ${ICEBERG_SVC_CATALOG_PRINCIPAL} && ozone sh volume info s3v && ozone s3 getsecret"
)"

POLARIS_AWS_ACCESS_KEY_ID="$(
  echo "${s3_secret_output}" | grep -o 'awsAccessKey=[^[:space:]]*' | head -1 | cut -d= -f2
)"
POLARIS_AWS_SECRET_ACCESS_KEY="$(
  echo "${s3_secret_output}" | grep -o 'awsSecret=[^[:space:]]*' | head -1 | cut -d= -f2
)"

if [[ -z "${POLARIS_AWS_ACCESS_KEY_ID}" || -z "${POLARIS_AWS_SECRET_ACCESS_KEY}" ]]; then
  echo "ERROR: Failed to parse S3 credentials from ozone s3 getsecret output:"
  echo "${s3_secret_output}"
  exit 1
fi

export POLARIS_AWS_ACCESS_KEY_ID POLARIS_AWS_SECRET_ACCESS_KEY

echo "Starting Polaris (${POLARIS_IMAGE})..."
docker-compose --ansi never up -d polaris

wait_for_port polaris 8181 120

echo "Provisioning Polaris catalog (${POLARIS_CATALOG_NAME})..."
docker-compose --ansi never run --rm polaris-setup

spark_packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${POLARIS_ICEBERG_SPARK_RUNTIME_VERSION},org.apache.iceberg:iceberg-aws-bundle:${POLARIS_ICEBERG_SPARK_RUNTIME_VERSION}"
sql_file="/opt/hadoop/smoketest/security/ozone-secure-sts-polaris.sql"

echo "Running Spark SQL against Polaris with STS vended credentials..."
set +e
spark_output="$(
  docker-compose --ansi never run --rm spark-sql \
    /opt/spark/bin/spark-sql \
      --packages "${spark_packages}" \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.polaris.type=rest \
      --conf spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog \
      --conf spark.sql.catalog.polaris.rest.auth.type=oauth2 \
      --conf spark.sql.catalog.polaris.oauth2-server-uri=http://polaris:8181/api/catalog/v1/oauth/tokens \
      --conf spark.sql.catalog.polaris.token-refresh-enabled=false \
      --conf spark.sql.catalog.polaris.warehouse="${POLARIS_CATALOG_NAME}" \
      --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
      --conf spark.sql.catalog.polaris.credential=root:s3cr3t \
      --conf spark.sql.catalog.polaris.client.region=us-west-2 \
      --conf spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials \
      -f "${sql_file}" 2>&1
)"
spark_exit_code=$?
set -e

echo "${spark_output}"

if [[ "${spark_exit_code}" -ne 0 ]]; then
  echo "ERROR: spark-sql exited with status ${spark_exit_code}"
  exit "${spark_exit_code}"
fi

if ! echo "${spark_output}" | grep -Fq "testing STS"; then
  echo "ERROR: Expected Spark output to contain inserted row value 'testing STS'"
  exit 1
fi

echo "Polaris STS smoke test passed."

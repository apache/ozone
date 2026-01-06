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

#suite:misc

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

: "${RANGER_VERSION:=2.8.0-SNAPSHOT}"
: "${DOWNLOAD_DIR:=${TEMP_DIR:-/tmp}}"

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export COMPOSE_FILE=docker-compose.yaml:ranger.yaml:../common/ranger.yaml
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export SECURITY_ENABLED=true

# Check if we are using a snapshot version
if [[ "${RANGER_VERSION}" =~ [0-9]+\.[0-9]+\.[0-9]+-[0-9]{8}\.[0-9]{6}-[0-9]+ ]] || [[ "${RANGER_VERSION}" == *"SNAPSHOT"* ]]; then
  IS_SNAPSHOT=true
else
  IS_SNAPSHOT=false
fi

if [[ "${SKIP_APACHE_VERIFY_DOWNLOAD}" != "true" ]] && [[ "${IS_SNAPSHOT}" == "false" ]]; then
  curl -LO https://downloads.apache.org/ranger/KEYS
  gpg --import KEYS
fi

if [[ "${IS_SNAPSHOT}" == "true" ]]; then
  # Snapshot download logic
  RANGER_BASE_VERSION=$(echo "${RANGER_VERSION}" | sed -E 's/-[0-9]{8}\.[0-9]{6}-[0-9]+//')
  if [[ "${RANGER_BASE_VERSION}" == "${RANGER_VERSION}" ]]; then
      RANGER_BASE_VERSION="${RANGER_VERSION}"
  else
      RANGER_BASE_VERSION="${RANGER_BASE_VERSION}-SNAPSHOT"
  fi
  SNAPSHOT_REPO="https://repository.apache.org/content/groups/snapshots/org/apache/ranger/ranger-distro/${RANGER_BASE_VERSION}"

  if [[ "${RANGER_VERSION}" == *"SNAPSHOT"* ]]; then
      # If RANGER_VERSION is a snapshot (e.g. 2.8.0-SNAPSHOT), resolve it to the latest timestamped version
      download_if_not_exists "${SNAPSHOT_REPO}/maven-metadata.xml" "${DOWNLOAD_DIR}/maven-metadata.xml"
      TIMESTAMP=$(grep "<timestamp>" "${DOWNLOAD_DIR}/maven-metadata.xml" | head -1 | sed -e 's/.*<timestamp>\(.*\)<\/timestamp>.*/\1/')
      BUILDNUM=$(grep "<buildNumber>" "${DOWNLOAD_DIR}/maven-metadata.xml" | head -1 | sed -e 's/.*<buildNumber>\(.*\)<\/buildNumber>.*/\1/')
      if [[ -n "${TIMESTAMP}" ]] && [[ -n "${BUILDNUM}" ]]; then
          RANGER_VERSION="${RANGER_BASE_VERSION%-SNAPSHOT}-${TIMESTAMP}-${BUILDNUM}"
          echo "Resolved RANGER_VERSION to ${RANGER_VERSION}"
      fi
  fi

  SRC_TAR="ranger-distro-${RANGER_VERSION}-src.tar.gz"
  download_if_not_exists "${SNAPSHOT_REPO}/${SRC_TAR}" "${DOWNLOAD_DIR}/${SRC_TAR}"
  tar -C "${DOWNLOAD_DIR}" -x -z -f "${DOWNLOAD_DIR}/${SRC_TAR}"

  # Find the extracted directory name
  EXTRACTED_DIR=$(tar -tf "${DOWNLOAD_DIR}/${SRC_TAR}" | grep -o '^[^/]*' | sort | uniq | head -1)
  export RANGER_SOURCE_DIR="${DOWNLOAD_DIR}/${EXTRACTED_DIR}"
else
  # Release download logic
  download_and_verify_apache_release "ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz"
  tar -C "${DOWNLOAD_DIR}" -x -z -f "${DOWNLOAD_DIR}/apache-ranger-${RANGER_VERSION}.tar.gz"
  export RANGER_SOURCE_DIR="${DOWNLOAD_DIR}/apache-ranger-${RANGER_VERSION}"
fi

chmod -R a+rX "${RANGER_SOURCE_DIR}"

# Ranger docker support scripts moved between releases (eg: from config/*.sh to scripts/**).
# Ensure we don't fail if a glob doesn't match, but still make init scripts executable when present.
shopt -s nullglob
chmod_targets=(
  "${RANGER_SOURCE_DIR}"/dev-support/ranger-docker/config/*.sh
  "${RANGER_SOURCE_DIR}"/dev-support/ranger-docker/scripts/rdbms/*.sh
)
shopt -u nullglob
if (( ${#chmod_targets[@]} > 0 )); then
  chmod a+x "${chmod_targets[@]}"
fi

if [[ "${IS_SNAPSHOT}" == "true" ]]; then
  PLUGIN_TAR="ranger-distro-${RANGER_VERSION}-ozone-plugin.tar.gz"
  download_if_not_exists "${SNAPSHOT_REPO}/${PLUGIN_TAR}" "${DOWNLOAD_DIR}/${PLUGIN_TAR}"
  tar -C "${DOWNLOAD_DIR}" -x -z -f "${DOWNLOAD_DIR}/${PLUGIN_TAR}"
  EXTRACTED_PLUGIN_DIR=$(tar -tf "${DOWNLOAD_DIR}/${PLUGIN_TAR}" | grep -o '^[^/]*' | sort | uniq | head -1)
  export RANGER_OZONE_PLUGIN_DIR="${DOWNLOAD_DIR}/${EXTRACTED_PLUGIN_DIR}"
else
  download_and_verify_apache_release "ranger/${RANGER_VERSION}/plugins/ozone/ranger-${RANGER_VERSION}-ozone-plugin.tar.gz"
  tar -C "${DOWNLOAD_DIR}" -x -z -f "${DOWNLOAD_DIR}/ranger-${RANGER_VERSION}-ozone-plugin.tar.gz"
  export RANGER_OZONE_PLUGIN_DIR="${DOWNLOAD_DIR}/ranger-${RANGER_VERSION}-ozone-plugin"
fi

chmod -R a+rX "${RANGER_OZONE_PLUGIN_DIR}"
chmod a+x "${RANGER_OZONE_PLUGIN_DIR}"/*.sh

# customizations before install
perl -wpl -i \
  -e 's@^POLICY_MGR_URL=.*@POLICY_MGR_URL=http://ranger:6080@;' \
  -e 's@^REPOSITORY_NAME=.*@REPOSITORY_NAME=dev_ozone@;' \
  -e 's@^CUSTOM_USER=ozone@CUSTOM_USER=hadoop@;' \
  -e 's@^XAAUDIT.LOG4J.ENABLE=.*@XAAUDIT.LOG4J.ENABLE=false@;' \
  -e 's@^XAAUDIT.LOG4J.DESTINATION.LOG4J=.*@XAAUDIT.LOG4J.DESTINATION.LOG4J=false@;' \
  "${RANGER_OZONE_PLUGIN_DIR}/install.properties"

echo 'machine ranger login admin password rangerR0cks!' > ../../.netrc

start_docker_env
wait_for_port ranger 6080 120

execute_robot_test s3g -v USER:hdfs kinit.robot
execute_robot_test s3g freon/generate.robot
execute_robot_test s3g freon/validate.robot

execute_robot_test s3g -v RANGER_ENDPOINT_URL:"http://ranger:6080" -v USER:hdfs security/ozone-secure-tenant.robot
execute_robot_test s3g -v RANGER_ENDPOINT_URL:"http://ranger:6080" -v USER:hdfs security/ozone-secure-sts.robot

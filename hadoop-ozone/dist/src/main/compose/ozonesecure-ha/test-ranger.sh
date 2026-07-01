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

if [[ -z "${RANGER_VERSION:-}" ]]; then
  source "${COMPOSE_DIR}/.env"
fi

: "${DOWNLOAD_DIR:=${TEMP_DIR:-/tmp}}"

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export COMPOSE_FILE=docker-compose.yaml:ranger.yaml:../common/ranger.yaml
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export SECURITY_ENABLED=true

if [[ "${SKIP_APACHE_VERIFY_DOWNLOAD}" != "true" ]]; then
  curl -LO https://downloads.apache.org/ranger/KEYS
  gpg --import KEYS
fi

download_and_verify_apache_release "ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz"
tar -C "${DOWNLOAD_DIR}" -x -z -f "${DOWNLOAD_DIR}/apache-ranger-${RANGER_VERSION}.tar.gz"
export RANGER_SOURCE_DIR="${DOWNLOAD_DIR}/apache-ranger-${RANGER_VERSION}"
chmod -R a+rX "${RANGER_SOURCE_DIR}"

# Ranger docker support scripts moved between releases (eg: from config/*.sh to scripts/**).
# Ensure we don't fail if a glob doesn't match, but still make init scripts executable when present.
if [[ -d "${RANGER_SOURCE_DIR}/dev-support/ranger-docker" ]]; then
  find "${RANGER_SOURCE_DIR}/dev-support/ranger-docker" -type f -name '*.sh' -exec chmod a+x {} +
fi
download_and_verify_apache_release "ranger/${RANGER_VERSION}/plugins/ozone/ranger-${RANGER_VERSION}-ozone-plugin.tar.gz"
tar -C "${DOWNLOAD_DIR}" -x -z -f "${DOWNLOAD_DIR}/ranger-${RANGER_VERSION}-ozone-plugin.tar.gz"
export RANGER_OZONE_PLUGIN_DIR="${DOWNLOAD_DIR}/ranger-${RANGER_VERSION}-ozone-plugin"
chmod -R a+rX "${RANGER_OZONE_PLUGIN_DIR}"
chmod a+x "${RANGER_OZONE_PLUGIN_DIR}"/*.sh

# customizations before install
perl -wpl -i \
  -e 's@^POLICY_MGR_URL=.*@POLICY_MGR_URL=http://ranger:6080@;' \
  -e 's@^REPOSITORY_NAME=.*@REPOSITORY_NAME=dev_ozone@;' \
  -e 's@^CUSTOM_USER=ozone@CUSTOM_USER=hadoop@;' \
  -e 's@^XAAUDIT.LOG4J.ENABLE=true@XAAUDIT.LOG4J.ENABLE=false@;' \
  -e 's@^XAAUDIT.LOG4J.DESTINATION.LOG4J=true@XAAUDIT.LOG4J.DESTINATION.LOG4J=false@;' \
  "${RANGER_OZONE_PLUGIN_DIR}/install.properties"

echo 'machine ranger login admin password rangerR0cks!' > ../../.netrc

start_docker_env
wait_for_port ranger 6080 120

execute_robot_test s3g -v USER:hdfs kinit.robot
execute_robot_test s3g freon/generate.robot
execute_robot_test s3g freon/validate.robot

execute_robot_test s3g -v RANGER_ENDPOINT_URL:"http://ranger:6080" -v USER:hdfs security/ozone-secure-tenant.robot
execute_robot_test s3g -v RANGER_ENDPOINT_URL:"http://ranger:6080" -v USER:hdfs security/ozone-secure-sts.robot
execute_robot_test s3g -v RANGER_ENDPOINT_URL:"http://ranger:6080" -v USER:hdfs security/ozone-secure-sts-multitenant.robot

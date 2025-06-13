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

#suite:failing

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

: "${RANGER_VERSION:=2.6.0}"
: "${DOWNLOAD_DIR:=${TEMP_DIR:-/tmp}}"

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export SECURITY_ENABLED=false
export COMPOSE_FILE=docker-compose.yaml:docker-compose.ranger.yaml

curl -LO https://downloads.apache.org/ranger/KEYS
gpg --import KEYS

download_if_not_exists() {
  local url="$1"
  local f="$2"

  if [[ -e "${f}" ]]; then
    echo "${f} already downloaded"
  else
    echo "Downloading ${f} from ${url}"
    curl --fail --location --output "${f}" --show-error --silent "${url}" || rm -fv "${f}"
  fi
}

download_and_verify_apache_release() {
  local remote_path="$1"

  local f="$(basename "${remote_path}")"
  local base_url="${APACHE_MIRROR_URL:-https://www.apache.org/dyn/closer.lua?action=download&filename=}"
  local checksum_base_url="${APACHE_OFFICIAL_URL:-https://downloads.apache.org/}"
  local download_dir="${DOWNLOAD_DIR:-/tmp}"

  download_if_not_exists "${base_url}${remote_path}" "${download_dir}/${f}"
  download_if_not_exists "${checksum_base_url}${remote_path}.asc"  "${download_dir}/${f}.asc"
  gpg --verify "${download_dir}/${f}.asc" "${download_dir}/${f}" || exit 1
}

download_and_verify_apache_release "ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz"
tar -C "${DOWNLOAD_DIR}" -x -z -f "${DOWNLOAD_DIR}/apache-ranger-${RANGER_VERSION}.tar.gz"
export RANGER_SOURCE_DIR="${DOWNLOAD_DIR}/apache-ranger-${RANGER_VERSION}"
chmod -R go+rX "${RANGER_SOURCE_DIR}"

download_and_verify_apache_release "ranger/${RANGER_VERSION}/plugins/ozone/ranger-${RANGER_VERSION}-ozone-plugin.tar.gz"
tar -C "${DOWNLOAD_DIR}" -x -z -f "${DOWNLOAD_DIR}/ranger-${RANGER_VERSION}-ozone-plugin.tar.gz"
export RANGER_OZONE_PLUGIN_DIR="${DOWNLOAD_DIR}/ranger-${RANGER_VERSION}-ozone-plugin"
chmod -R go+rX "${RANGER_OZONE_PLUGIN_DIR}"

# customizations before install
sed -i \
  -e 's@^POLICY_MGR_URL=.*@POLICY_MGR_URL=http://ranger:6080@' \
  -e 's@^REPOSITORY_NAME=.*@REPOSITORY_NAME=dev_ozone@' \
  -e 's@^CUSTOM_USER=ozone@CUSTOM_USER=hadoop@' \
  "${RANGER_OZONE_PLUGIN_DIR}/install.properties"

start_docker_env
wait_for_port ranger 6080 120

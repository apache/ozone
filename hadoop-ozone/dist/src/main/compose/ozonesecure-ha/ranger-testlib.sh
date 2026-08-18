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

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

setup_ranger_acceptance_env() {
  # Load FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION from .env without overriding other env.
  # Ranger reads this value from install.properties (not process env), but we allow
  # controlling the mounted install.properties via .env.
  if [[ -z "${FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION:-}" ]] && [[ -f "${COMPOSE_DIR}/.env" ]]; then
    local ff_from_dotenv
    ff_from_dotenv="$(
      (
        set -a
        # shellcheck source=/dev/null
        source "${COMPOSE_DIR}/.env"
        echo "${FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION:-}"
      ) 2>/dev/null
    )"
    if [[ -n "${ff_from_dotenv}" ]]; then
      export FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION="${ff_from_dotenv}"
    fi
  fi

  if [[ -z "${RANGER_VERSION:-}" ]]; then
    export RANGER_VERSION="${ranger.version}"
  fi

  : "${DOWNLOAD_DIR:=${TEMP_DIR:-/tmp}}"

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
  export RANGER_INIT_POSTGRES_SH="${RANGER_SOURCE_DIR}/dev-support/ranger-docker/scripts/rdbms/init_postgres.sh"

  # Create a temp install.properties so we can override feature flags from .env.
  local ranger_admin_install_properties_src
  ranger_admin_install_properties_src="${RANGER_SOURCE_DIR}/dev-support/ranger-docker/scripts/admin/ranger-admin-install-postgres.properties"
  RANGER_ADMIN_INSTALL_PROPERTIES="$(mktemp "${DOWNLOAD_DIR%/}/ranger-admin-install-postgres.XXXXXX")"
  cp -f "${ranger_admin_install_properties_src}" "${RANGER_ADMIN_INSTALL_PROPERTIES}"
  chmod a+r "${RANGER_ADMIN_INSTALL_PROPERTIES}"

  local ff
  ff="$(echo "${FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION:-false}" | tr '[:upper:]' '[:lower:]')"
  if [[ "${ff}" != "true" ]]; then
    ff="false"
  fi
  if grep -Eq '^[[:space:]]*#?[[:space:]]*FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION=' "${RANGER_ADMIN_INSTALL_PROPERTIES}"; then
    perl -pi -e "s@^[[:space:]]*#?[[:space:]]*FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION=.*@FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION=${ff}@g" \
      "${RANGER_ADMIN_INSTALL_PROPERTIES}"
  else
    printf '\nFF_ENABLE_OZONE_ACTION_MATCHES_CONDITION=%s\n' "${ff}" >> "${RANGER_ADMIN_INSTALL_PROPERTIES}"
  fi

  export RANGER_ADMIN_INSTALL_PROPERTIES

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
}

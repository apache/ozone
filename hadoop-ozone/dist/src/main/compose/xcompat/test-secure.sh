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

#suite:compat

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR
basename=$(basename ${COMPOSE_DIR})

current_version="${ozone.version}"
old_versions="1.0.0 1.1.0 1.2.1 1.3.0 1.4.0"

# shellcheck source=hadoop-ozone/dist/src/main/compose/testlib.sh
source "${COMPOSE_DIR}/../testlib.sh"

export SECURITY_ENABLED=true
: ${OZONE_BUCKET_KEY_NAME:=key1}

old_client() {
  OZONE_DIR=/opt/ozone
  container=${client}
  "$@"
}

new_client() {
  OZONE_DIR=/opt/hadoop
  container=new_client
  client_version=${current_version}
  "$@"
}


test_bucket_encryption() {

  execute_command_in_container ${container} kinit -k -t /etc/security/keytabs/testuser.keytab testuser/scm@EXAMPLE.COM

  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}" -v SUFFIX:${client_version} security/bucket-encryption.robot
}

test_encryption_cross_compatibility() {

  echo "Running client connection to multiple KMS backward compatibility tests."

  echo "Starting secure cluster with COMPOSE_FILE=${COMPOSE_FILE}"
  OZONE_KEEP_RESULTS=true start_docker_env

  execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

  new_client test_bucket_encryption

  for client_version in "$@"; do
    client="old_client_${client_version//./_}"
    old_client test_bucket_encryption
  done

  KEEP_RUNNING=false stop_docker_env
}

create_results_dir

encryption_old_versions="1.2.1 1.3.0 1.4.0"
# current cluster with various clients
COMPOSE_FILE=secure-new-cluster.yaml:secure-clients.yaml cluster_version=${current_version} test_encryption_cross_compatibility ${encryption_old_versions}

# old cluster with clients: same version and current version
for cluster_version in ${encryption_old_versions}; do
  export OZONE_VERSION=${cluster_version}
  COMPOSE_FILE=secure-old-cluster.yaml:secure-clients.yaml test_encryption_cross_compatibility ${cluster_version}
done

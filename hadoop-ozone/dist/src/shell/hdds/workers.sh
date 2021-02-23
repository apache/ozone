#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Run a shell command on all worker hosts.
#
# Environment Variables
#
#   OZONE_WORKERS    File naming remote hosts.
#     Default is ${OZONE_CONF_DIR}/workers.
#   OZONE_CONF_DIR  Alternate conf dir. Default is ${OZONE_HOME}/conf.
#   OZONE_WORKER_SLEEP Seconds to sleep between spawning remote commands.
#   OZONE_SSH_OPTS Options passed to ssh when running remote commands.
##

function ozone_usage
{
  echo "Usage: workers.sh [--config confdir] command..."
}

# load functions
for dir in "${OZONE_LIBEXEC_DIR}" "${OZONE_HOME}/libexec" "${HADOOP_LIBEXEC_DIR}" "${HADOOP_HOME}/libexec" "${bin}/../libexec"; do
  if [[ -e "${dir}/ozone-functions.sh" ]]; then
    . "${dir}/ozone-functions.sh"
    if declare -F ozone_bootstrap >& /dev/null; then
      break
    fi
  fi
done

if ! declare -F ozone_bootstrap >& /dev/null; then
  echo "ERROR: Cannot find ozone-functions.sh." 2>&1
  exit 1
fi

ozone_bootstrap
. "${OZONE_LIBEXEC_DIR}/ozone-config.sh"

# if no args specified, show usage
if [[ $# -le 0 ]]; then
  ozone_exit_with_usage 1
fi

ozone_connect_to_hosts "$@"

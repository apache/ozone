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

# Start ozone daemons.
# Run this on master node.
## @description  usage info
## @audience     private
## @stability    evolving
## @replaceable  no
function ozone_usage
{
  echo "Usage: start-ozone.sh"
}

this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)

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

# get arguments
if [[ $# -ge 1 ]]; then
  startOpt="$1"
  shift
  case "$startOpt" in
    -upgrade)
      nameStartOpt="$startOpt"
    ;;
    -rollback)
      dataStartOpt="$startOpt"
    ;;
    *)
      ozone_exit_with_usage 1
    ;;
  esac
fi

#Add other possible options
nameStartOpt="$nameStartOpt $*"

SECURITY_ENABLED=$("${OZONE_HOME}/bin/ozone" getconf -confKey hadoop.security.authentication | tr '[:upper:]' '[:lower:]' 2>&-)
SECURITY_AUTHORIZATION_ENABLED=$("${OZONE_HOME}/bin/ozone" getconf -confKey hadoop.security.authorization | tr '[:upper:]' '[:lower:]' 2>&-)

# datanodes (using default workers file)

echo "Starting datanodes"
ozone_uservar_su hdfs datanode "${OZONE_HOME}/bin/ozone" \
    --workers \
    --config "${OZONE_CONF_DIR}" \
    --daemon start \
    datanode ${dataStartOpt}
OZONE_JUMBO_RETCOUNTER=$?

#---------------------------------------------------------
# Ozone ozonemanager nodes
OM_NODES=$("${OZONE_HOME}/bin/ozone" getconf -ozonemanagers 2>/dev/null)
echo "Starting Ozone Manager nodes [${OM_NODES}]"
if [[ "${OM_NODES}" == "0.0.0.0" ]]; then
  OM_NODES=$(hostname)
fi

ozone_uservar_su hdfs om "${OZONE_HOME}/bin/ozone" \
  --workers \
  --config "${OZONE_CONF_DIR}" \
  --hostnames "${OM_NODES}" \
  --daemon start \
  om
(( OZONE_JUMBO_RETCOUNTER=OZONE_JUMBO_RETCOUNTER + $? ))

#---------------------------------------------------------
# Ozone storagecontainermanager nodes
SCM_NODES=$("${OZONE_HOME}/bin/ozone" getconf -storagecontainermanagers 2>/dev/null)
echo "Starting storage container manager nodes [${SCM_NODES}]"
ozone_uservar_su hdfs scm "${OZONE_HOME}/bin/ozone" \
  --workers \
  --config "${OZONE_CONF_DIR}" \
  --hostnames "${SCM_NODES}" \
  --daemon start \
  scm
(( OZONE_JUMBO_RETCOUNTER=OZONE_JUMBO_RETCOUNTER + $? ))

exit ${OZONE_JUMBO_RETCOUNTER}

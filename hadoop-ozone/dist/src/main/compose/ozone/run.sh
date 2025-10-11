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

source ../compose_v2_compatibility.sh

declare -i OZONE_DATANODES OZONE_REPLICATION_FACTOR OZONE_SAFEMODE_MIN_DATANODES

ORIG_DATANODES="${OZONE_DATANODES:-}"
ORIG_REPLICATION_FACTOR="${OZONE_REPLICATION_FACTOR:-}"

# only support replication factor of 1 or 3
if [[ -n ${OZONE_REPLICATION_FACTOR} ]] && [[ ${OZONE_REPLICATION_FACTOR} -ne 1 ]] && [[ ${OZONE_REPLICATION_FACTOR} -ne 3 ]]; then
  # assume invalid replication factor was intended as "number of datanodes"
  if [[ -z ${ORIG_DATANODES} ]]; then
    OZONE_DATANODES=${OZONE_REPLICATION_FACTOR}
  fi
  unset OZONE_REPLICATION_FACTOR
fi

# at least 1 datanode
if [[ -n ${OZONE_DATANODES} ]] && [[ ${OZONE_DATANODES} -lt 1 ]]; then
  unset OZONE_DATANODES
fi

if [[ -n ${OZONE_DATANODES} ]] && [[ -n ${OZONE_REPLICATION_FACTOR} ]]; then
  # ensure enough datanodes for replication factor
  if [[ ${OZONE_DATANODES} -lt ${OZONE_REPLICATION_FACTOR} ]]; then
    OZONE_DATANODES=${OZONE_REPLICATION_FACTOR}
  fi
elif [[ -n ${OZONE_DATANODES} ]]; then
  if [[ ${OZONE_DATANODES} -ge 3 ]]; then
    OZONE_REPLICATION_FACTOR=3
  else
    OZONE_REPLICATION_FACTOR=1
  fi
elif [[ -n ${OZONE_REPLICATION_FACTOR} ]]; then
  OZONE_DATANODES=${OZONE_REPLICATION_FACTOR}
else
  OZONE_DATANODES=1
  OZONE_REPLICATION_FACTOR=1
fi

: ${OZONE_SAFEMODE_MIN_DATANODES:=${OZONE_REPLICATION_FACTOR}}

export OZONE_DATANODES OZONE_REPLICATION_FACTOR OZONE_SAFEMODE_MIN_DATANODES

docker-compose up --scale datanode=${OZONE_DATANODES} --no-recreate "$@"

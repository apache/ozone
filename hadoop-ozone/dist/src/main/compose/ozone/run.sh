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

declare -ix OZONE_REPLICATION_FACTOR OZONE_SAFEMODE_MIN_DATANODES

# Uncomment the following two lines if FSO should be enabled by default
# export OZONE_OM_METADATA_LAYOUT=PREFIX
# export OZONE_OM_ENABLE_FILESYSTEM_PATHS=true

: ${OZONE_REPLICATION_FACTOR:=1}
: ${OZONE_SAFEMODE_MIN_DATANODES:=${OZONE_REPLICATION_FACTOR}}

docker-compose up --scale datanode=${OZONE_REPLICATION_FACTOR} --no-recreate "$@"

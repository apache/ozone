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

# Fail if required variables are not set.
set -u
: "${OZONE_VOLUME}"
: "${TEST_DIR}"
set +u

source "$TEST_DIR/testlib.sh"

export COMPOSE_FILE="$TEST_DIR/compose/ha/docker-compose.yaml"
export OM_SERVICE_ID=omservice
create_data_dirs "${OZONE_VOLUME}"/{om1,om2,om3,dn1,dn2,dn3,recon,s3g,scm}

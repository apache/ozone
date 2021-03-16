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

# This script can be run only if the cluster is already started 
# one, and initialized (but not data is written, yet).

set -e
COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "${COMPOSE_DIR}"

# shellcheck source=/dev/null
source "${COMPOSE_DIR}/../testlib.sh"

#read OZONE_VOLUME from here
# shellcheck source=/dev/null
source "$COMPOSE_DIR"/.env

rm -rf "${OZONE_VOLUME}"/{dn1,dn2,dn3,om,recon,s3g,scm}
mkdir -p "${OZONE_VOLUME}"/{dn1,dn2,dn3,om,recon,s3g,scm}



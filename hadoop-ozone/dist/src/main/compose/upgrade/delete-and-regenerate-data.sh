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


#During the first start, all the required VERSION and metadata files will be created
start_docker_env

#data generation requires offline cluster
docker-compose stop

#generate metadadata (-n1 means: only one container is generated)
docker-compose run scm ozone freon cgscm -u hadoop -n 1
docker-compose run om ozone freon cgom -u hadoop -n 1

#generate real data (and metadata) on datanodes.
docker-compose run dn1 ozone freon cgdn -u hadoop -n 1 --datanodes=3 --index=1
docker-compose run dn2 ozone freon cgdn -u hadoop -n 1 --datanodes=3 --index=2
docker-compose run dn3 ozone freon cgdn -u hadoop -n 1 --datanodes=3 --index=3

#start docker env with the generated data
start_docker_env

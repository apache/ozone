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

docker-compose run scm ozone freon cgscm -n 1
docker-compose run om ozone freon cgom -n 1
docker-compose run dn1 ozone freon cgdn -n 1
docker-compose run dn2 ozone freon cgdn -n 1
docker-compose run dn2 ozone freon cgdn -n 1

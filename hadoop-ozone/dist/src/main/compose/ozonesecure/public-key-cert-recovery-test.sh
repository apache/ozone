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

#suite:secure

#test recon
wait_for_port recon 9888 120
wait_for_execute_command recon 60 "find /data/metadata/recon/keys/public.pem -delete && find /data/metadata/recon/certs/*.crt -delete"
docker-compose stop recon
docker-compose start recon
wait_for_port recon 9888 120
wait_for_execute_command recon 60 "find /data/metadata/recon/keys/public.pem && find /data/metadata/recon/certs/ROOTCA*.crt"

#test DN
wait_for_port datanode 19864 120
wait_for_execute_command datanode 60 "find /data/metadata/dn/keys/public.pem -delete && find /data/metadata/dn/certs/*.crt -delete"
docker-compose stop datanode
docker-compose start datanode
wait_for_port datanode 19864 120
wait_for_execute_command datanode 60 "find /data/metadata/dn/keys/public.pem && find /data/metadata/dn/certs/ROOTCA*.crt"


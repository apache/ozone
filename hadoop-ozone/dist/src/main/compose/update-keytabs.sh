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

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )

rm -rf "$SCRIPT_DIR/keytabs"
docker run -it --entrypoint=/data/export-keytabs.sh -v "$SCRIPT_DIR":/data -v "$SCRIPT_DIR/keytabs":/etc/security/keytabs elek/ozone-devkrb5:latest

sudo chown -R "$(id -u)" "$SCRIPT_DIR/keytabs"
chmor 755 "$SCRIPT_DIR/keytabs/*.keytab"

SECURE_ENVS=( ozonesecure ozonesecure-mr ozonesecure-om-ha )

for e in "${SECURE_ENVS[@]}"
do
   rm -rf "$SCRIPT_DIR/$e/keytabs"
   cp -r "$SCRIPT_DIR/keytabs" "$SCRIPT_DIR/$e/keytabs"
done



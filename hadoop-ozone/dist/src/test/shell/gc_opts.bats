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



#
# Can be executed with bats (https://github.com/bats-core/bats-core)
# bats gc_opts.bats (FROM THE CURRENT DIRECTORY)
#

source ../../shell/hdds/hadoop-functions.sh
@test "Setting Hadoop GC parameters: add GC params for server" {
  export HADOOP_SUBCMD_SUPPORTDAEMONIZATION=true
  export HADOOP_OPTS="Test"
  hadoop_add_default_gc_opts
  [[ "$HADOOP_OPTS" =~ "UseConcMarkSweepGC" ]]
}

@test "Setting Hadoop GC parameters: disabled for client" {
  export HADOOP_SUBCMD_SUPPORTDAEMONIZATION=false
  export HADOOP_OPTS="Test"
  hadoop_add_default_gc_opts
  [[ ! "$HADOOP_OPTS" =~ "UseConcMarkSweepGC" ]]
}

@test "Setting Hadoop GC parameters: disabled if GC params are customized" {
  export HADOOP_SUBCMD_SUPPORTDAEMONIZATION=true
  export HADOOP_OPTS="-XX:++UseG1GC -Xmx512"
  hadoop_add_default_gc_opts
  [[ ! "$HADOOP_OPTS" =~ "UseConcMarkSweepGC" ]]
}

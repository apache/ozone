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

export COUNTER=1

pass_after_three_failures() {
   if [ $COUNTER -eq 3 ]; then
      return 0
   fi 
   COUNTER=$(( COUNTER + 1))
   return 255
}

pass_first() {
   echo "pass"
}

pass_never() {
   return 255
}

load ../../main/k8s/examples/testlib.sh

@test "Test retry with passing function" {
   retry pass_first
}

@test "Test retry with 3 failures" {
   export RETRY_SLEEP=0 
   retry pass_after_three_failures
}

@test "Test retry always failure" {
   export RETRY_SLEEP=0 
   run retry pass_never
   [ "$status" -eq 255 ]
}






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
# shellcheck disable=SC2086

set -e

# This script helps to execute S3 robot test against real AWS s3 endpoint
# To make sure that all of our defined tests cases copies the behavior of AWS

: ${OZONE_TEST_S3_BUCKET1:?Please define test bucket}
: ${OZONE_TEST_S3_BUCKET2:?Please define second test bucket}
: ${OZONE_TEST_S3_REGION:?Please define the S3 region for test buckets}

run_robot_test() {
   TEST_NAME=$1
   robot \
       --nostatusrc \
       -v ENDPOINT_URL:https://s3.$OZONE_TEST_S3_REGION.amazonaws.com \
       -v BUCKET:$OZONE_TEST_S3_BUCKET1 \
       -v DESTBUCKET:$OZONE_TEST_S3_BUCKET2 \
       -v OZONE_S3_SET_CREDENTIALS:false \
       -o results/$TEST_NAME.xml \
       $TEST_NAME.robot
}

mkdir -p results

run_robot_test objectputget
run_robot_test objectdelete
run_robot_test objectcopy
run_robot_test objectmultidelete
run_robot_test MultipartUpload

rebot --outputdir results/ results/*.xml

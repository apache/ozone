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


# To run this script
#    mvn clean install -DskipShade -DskipTests
#    cd hadoop-ozone/dist/target/ozone-*/smoketest/s3/
#    python3 -m venv s3env
#    source s3env/bin/activate
#    pip3 install awscli==1.18.69
#    pip3 install boto3==1.13.5

# set up your aws credentials:
#    export AWS_ACCESS_KEY_ID=<your access key>
#    export AWS_SECRET_ACCESS_KEY=<your secret access key>
#    export AWS_DEFAULT_REGION=us-east-1

# add env vars:
#    export OZONE_TEST_S3_BUCKET1=ozone-test-`openssl rand -hex 5`
#    export OZONE_TEST_S3_BUCKET2=ozone-test-`openssl rand -hex 5`
#    export OZONE_TEST_S3_REGION=us-east-1


# create dummy buckets in aws:
#    aws s3api create-bucket --bucket $OZONE_TEST_S3_BUCKET1
#    aws s3api create-bucket --bucket $OZONE_TEST_S3_BUCKET2


# finally to run the test:
#    ./s3_compatbility_check.sh

# when done, run 'deactivate' to turn off the python virtualenv.

: ${OZONE_TEST_S3_BUCKET1:?Please define test bucket}
: ${OZONE_TEST_S3_BUCKET2:?Please define second test bucket}
: ${OZONE_TEST_S3_REGION:?Please define the S3 region for test buckets}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

run_robot_test() {
   TEST_NAME=$1
   robot \
       --nostatusrc \
       --exclude aws-skip \
       -v ENDPOINT_URL:https://s3.$OZONE_TEST_S3_REGION.amazonaws.com \
       -v BUCKET:$OZONE_TEST_S3_BUCKET1 \
       -v DESTBUCKET:$OZONE_TEST_S3_BUCKET2 \
       -v OZONE_S3_SET_CREDENTIALS:false \
       -v S3_SMOKETEST_DIR:${SCRIPT_DIR} \
       -o results/$TEST_NAME.xml \
       $TEST_NAME.robot
}

mkdir -p results

run_robot_test awss3
run_robot_test boto3
run_robot_test bucketcreate
run_robot_test bucketdelete
run_robot_test buckethead
run_robot_test bucketlist
run_robot_test objectputget
run_robot_test objectdelete
run_robot_test objectcopy
run_robot_test objectmultidelete
run_robot_test objecthead
run_robot_test MultipartUpload
run_robot_test objecttagging
run_robot_test objectlist

rebot --outputdir results/ results/*.xml

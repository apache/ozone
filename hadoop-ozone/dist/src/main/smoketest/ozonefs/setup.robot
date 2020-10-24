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

*** Settings ***
Documentation       Setup for Ozone FS tests
Resource            ../commonlib.robot
Resource            ../lib/fs.robot

*** Variables ***
${SCHEME}            ofs
${BUCKET_TYPE}       bucket
${VOLUME}            fstest1
${VOL2}              fstest2
${BUCKET}            ${BUCKET_TYPE}1-${SCHEME}
${BUCKET2}           ${BUCKET_TYPE}2-${SCHEME}
${BUCKET_IN_VOL2}    ${BUCKET_TYPE}3-${SCHEME}
${DEEP_DIR}          test/${SCHEME}/dir

*** Keywords ***
Setup for FS test
    Create volumes for FS test
    Run Keyword    Create ${BUCKET_TYPE}s for FS test
    Sanity check for FS test
    Assign suite vars for FS test
    Log    Completed setup for ${SCHEME} tests with ${BUCKET_TYPE}s in ${VOLUME}/${BUCKET} using FS base URL: ${BASE_URL}

Create volumes for FS test
    Execute And Ignore Error    ozone sh volume create ${VOLUME} --space-quota 100TB
    Execute And Ignore Error    ozone sh volume create ${VOL2} --space-quota 100TB

Create buckets for FS test
    Execute                     ozone sh bucket create ${VOLUME}/${BUCKET}
    Execute                     ozone sh bucket create ${VOLUME}/${BUCKET2}
    Execute                     ozone sh bucket create ${VOL2}/${BUCKET_IN_VOL2}

Create links for FS test
    Execute And Ignore Error    ozone sh volume create ${VOLUME}-src --space-quota 100TB
    Execute And Ignore Error    ozone sh volume create ${VOL2}-src --space-quota 100TB
    Execute                     ozone sh bucket create ${VOLUME}-src/${BUCKET}-src
    Execute                     ozone sh bucket create ${VOLUME}-src/${BUCKET2}-src
    Execute                     ozone sh bucket create ${VOL2}-src/${BUCKET_IN_VOL2}-src
    Execute                     ozone sh bucket link ${VOLUME}-src/${BUCKET}-src ${VOLUME}/${BUCKET}
    Execute                     ozone sh bucket link ${VOLUME}-src/${BUCKET2}-src ${VOLUME}/${BUCKET2}
    Execute                     ozone sh bucket link ${VOL2}-src/${BUCKET_IN_VOL2}-src ${VOL2}/${BUCKET_IN_VOL2}

Sanity check for FS test
    ${result} =         Execute               ozone sh volume list
                        Should contain        ${result}               ${VOLUME}
                        Should contain        ${result}               ${VOL2}
                        Should Match Regexp   ${result}               "admin" : "(hadoop|testuser\/scm@EXAMPLE\.COM)"
    ${result} =         Execute               ozone sh bucket list ${VOLUME}
                        Should contain        ${result}               ${BUCKET}
                        Should contain        ${result}               ${BUCKET2}

Assign suite vars for FS test
    ${BASE_URL} =       Format FS URL         ${SCHEME}     ${VOLUME}    ${BUCKET}    /
    Set Suite Variable  ${BASE_URL}
    Set Suite Variable  ${DEEP_URL}           ${BASE_URL}${DEEP_DIR}

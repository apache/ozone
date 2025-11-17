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
Documentation       Test freon ommg command
Resource            ../ozone-lib/freon.robot
Test Timeout        5 minutes

*** Variables ***
${PREFIX}    ${EMPTY}
${n}    100
${VOLUME}       volume1
${BUCKET_FSO}       bucket-fso
${BUCKET_OBJ}       bucket-obj

*** Test Cases ***
[Setup] Create Volume and Buckets
    ${result} =     Execute             ozone sh volume create /${VOLUME}
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket create /${VOLUME}/${BUCKET_FSO} -l FILE_SYSTEM_OPTIMIZED
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket create /${VOLUME}/${BUCKET_OBJ} -l OBJECT_STORE
                    Should not contain  ${result}       Failed

[Read] Bucket Information
    ${result} =        Execute          ozone freon ommg --operation INFO_BUCKET -n ${n} --bucket ${BUCKET_FSO}
                       Should contain   ${result}   Successful executions: ${n}

[Create] File in FILE_SYSTEM_OPTIMIZED Bucket
    ${result} =        Execute          ozone freon ommg --operation CREATE_FILE -n ${n} --size 4096 --volume ${VOLUME} --bucket ${BUCKET_FSO}
                       Should contain   ${result}   Successful executions: ${n}

[Read] File in FILE_SYSTEM_OPTIMIZED Bucket
    ${result} =        Execute          ozone freon ommg --operation READ_FILE -n ${n} --volume ${VOLUME} --bucket ${BUCKET_FSO} --size 4096
                       Should contain   ${result}   Successful executions: ${n}

[List] File Status in FILE_SYSTEM_OPTIMIZED Bucket
    ${result} =        Execute          ozone freon ommg --operation LIST_STATUS -n 1 -t 1 --volume ${VOLUME} --bucket ${BUCKET_FSO} --batch-size ${n}
                       Should contain   ${result}   Successful executions: 1

[List] light File status in FILE_SYSTEM_OPTIMIZED Bucket
    ${result} =        Execute          ozone freon ommg --operation LIST_STATUS_LIGHT -n 1 -t 1 --volume ${VOLUME} --bucket ${BUCKET_FSO} --batch-size ${n}
                       Should contain   ${result}   Successful executions: 1

[Create] Key in OBJECT_STORE Bucket
    ${result} =        Execute          ozone freon ommg --operation CREATE_KEY -n ${n} --size 4096 --volume ${VOLUME} --bucket ${BUCKET_OBJ}
                       Should contain   ${result}   Successful executions: ${n}

[Read] Key in OBJECT_STORE Bucket
    ${result} =        Execute          ozone freon ommg --operation READ_KEY -n ${n} --volume ${VOLUME} --bucket ${BUCKET_OBJ} --size 4096
                       Should contain   ${result}   Successful executions: ${n}

[List] Keys in OBJECT_STORE Bucket
    ${result} =        Execute          ozone freon ommg --operation LIST_KEYS -n 1 -t 1 --volume ${VOLUME} --bucket ${BUCKET_OBJ} --batch-size ${n}
                       Should contain   ${result}   Successful executions: 1

[List] Light Keys in OBJECT_STORE Bucket
    ${result} =        Execute          ozone freon ommg --operation LIST_KEYS_LIGHT -n 1 -t 1 --volume ${VOLUME} --bucket ${BUCKET_OBJ} --batch-size ${n}
                       Should contain   ${result}   Successful executions: 1

[Get] Key Information in OBJECT_STORE Bucket
    ${result} =        Execute          ozone freon ommg --operation GET_KEYINFO -n ${n} --volume ${VOLUME} --bucket ${BUCKET_OBJ}
                       Should contain   ${result}   Successful executions: ${n}

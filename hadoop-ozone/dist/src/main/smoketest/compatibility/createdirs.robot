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
Documentation       Create dirs and files
Resource            ../ozone-lib/shell.robot
Test Timeout        5 minutes

*** Variables ***
${SUFFIX}    ${EMPTY}
${VOLUME}    volume1
${BUCKET}    bucketfso1

*** Test Cases ***
Key Can Be Written
    Create Key    /${VOLUME}/${BUCKET}/key-${SUFFIX}    /etc/passwd

Dir Can Be Created
    Execute    ozone fs -mkdir o3fs://${BUCKET}.${VOLUME}/dir-${SUFFIX}
    Execute    ozone fs -mkdir o3fs://${BUCKET}.${VOLUME}/newdir-${SUFFIX}
    FOR     ${INDEX}    IN RANGE    5
        Execute    ozone fs -mkdir o3fs://${BUCKET}.${VOLUME}/dir-${SUFFIX}/subdir-0${INDEX}-${SUFFIX}
    END

Put Multiple Files
    FOR     ${INDEX}    IN RANGE    5
        Execute    ozone fs -put /etc/passwd o3fs://${BUCKET}.${VOLUME}/dir-${SUFFIX}/file-${INDEX}
    END

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
Documentation       Test ozone fs with hadoopfs
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            ../lib/fs.robot
Test Timeout        5 minutes

*** Variables ***
${SCHEME}           o3fs
${volume}           volume1
${bucket}           bucket1
${PREFIX}           ozone

*** Test cases ***

Test hadoop dfs
    ${dir} =          Format FS URL         ${SCHEME}     ${volume}    ${bucket}
    ${random} =        Generate Random String  5  [NUMBERS]
    ${result} =        Execute                    hdfs dfs -put /opt/hadoop/NOTICE.txt ${dir}/${PREFIX}-${random}
    ${result} =        Execute                    hdfs dfs -ls ${dir}
                       Should contain             ${result}   ${PREFIX}-${random}

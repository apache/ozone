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
Documentation       Ozonefs test
Library             OperatingSystem
Resource            ../commonlib.robot

*** Variables ***


*** Test Cases ***
Create volume and bucket
    Execute             ozone sh volume create http://ozoneManager/fstest --user bilbo --quota 100TB --root
    Execute             ozone sh volume create http://ozoneManager/fstest2 --user bilbo --quota 100TB --root
    Execute             ozone sh bucket create http://ozoneManager/fstest/bucket1
    Execute             ozone sh bucket create http://ozoneManager/fstest/bucket2
    Execute             ozone sh bucket create http://ozoneManager/fstest2/bucket3

Check volume from ozonefs
    ${result} =         Execute               ozone fs -ls o3://bucket1.fstest/

Run ozoneFS tests
                        Execute               ozone fs -mkdir -p o3://bucket1.fstest/testdir/deep
    ${result} =         Execute               ozone sh key list o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                        Should contain    ${result}         testdir/deep
                        Execute               ozone fs -copyFromLocal NOTICE.txt o3://bucket1.fstest/testdir/deep/
    ${result} =         Execute               ozone sh key list o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                        Should contain    ${result}         NOTICE.txt

                        Execute               ozone fs -put NOTICE.txt o3://bucket1.fstest/testdir/deep/PUTFILE.txt
    ${result} =         Execute               ozone sh key list o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                        Should contain    ${result}         PUTFILE.txt

    ${result} =         Execute               ozone fs -ls o3://bucket1.fstest/testdir/deep/
                        Should contain    ${result}         NOTICE.txt
                        Should contain    ${result}         PUTFILE.txt

                        Execute               ozone fs -mv o3://bucket1.fstest/testdir/deep/NOTICE.txt o3://bucket1.fstest/testdir/deep/MOVED.TXT
    ${result} =         Execute               ozone sh key list o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                        Should contain    ${result}         MOVED.TXT
                        Should not contain  ${result}       NOTICE.txt

                        Execute               ozone fs -mkdir -p o3://bucket1.fstest/testdir/deep/subdir1
                        Execute               ozone fs -cp o3://bucket1.fstest/testdir/deep/MOVED.TXT o3://bucket1.fstest/testdir/deep/subdir1/NOTICE.txt
    ${result} =         Execute               ozone sh key list o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                        Should contain    ${result}         subdir1/NOTICE.txt

    ${result} =         Execute               ozone fs -ls o3://bucket1.fstest/testdir/deep/subdir1/
                        Should contain    ${result}         NOTICE.txt

                        Execute               ozone fs -cat o3://bucket1.fstest/testdir/deep/subdir1/NOTICE.txt
                        Should not contain  ${result}       Failed

                        Execute               ozone fs -rm o3://bucket1.fstest/testdir/deep/subdir1/NOTICE.txt
    ${result} =         Execute               ozone sh key list o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                        Should not contain  ${result}       NOTICE.txt

    ${result} =         Execute               ozone fs -rmdir o3://bucket1.fstest/testdir/deep/subdir1/
    ${result} =         Execute               ozone sh key list o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                        Should not contain  ${result}       subdir1

                        Execute               ozone fs -touch o3://bucket1.fstest/testdir/TOUCHFILE.txt
    ${result} =         Execute               ozone sh key list o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                        Should contain  ${result}       TOUCHFILE.txt

                        Execute               ozone fs -rm -r o3://bucket1.fstest/testdir/
    ${result} =         Execute               ozone sh key list o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                        Should not contain  ${result}       testdir

                        Execute               mkdir localdir1
                        Execute               cp NOTICE.txt localdir1/LOCAL.txt
                        Execute               ozone fs -mkdir -p o3://bucket1.fstest/testdir1
                        Execute               ozone fs -copyFromLocal localdir1 o3://bucket1.fstest/testdir1/
                        Execute               ozone fs -put NOTICE.txt o3://bucket1.fstest/testdir1/NOTICE.txt

    ${result} =         Execute               ozone fs -ls -R o3://bucket1.fstest/testdir1/
                        Should contain    ${result}         localdir1/LOCAL.txt
                        Should contain    ${result}         testdir1/NOTICE.txt

                        Execute               ozone fs -mkdir -p o3://bucket2.fstest/testdir2
                        Execute               ozone fs -mkdir -p o3://bucket3.fstest2/testdir3

                        Execute               ozone fs -cp o3://bucket1.fstest/testdir1/localdir1 o3://bucket2.fstest/testdir2/

                        Execute               ozone fs -cp o3://bucket1.fstest/testdir1/localdir1 o3://bucket3.fstest1/testdir3/

                        Execute               ozone sh key put o3://ozoneManager/fstest/bucket1/KEY.txt NOTICE.txt
    ${result} =         Execute               ozone fs -ls o3://bucket1.fstest/KEY.txt
                        Should contain    ${result}         KEY.txt
    ${result} =         Execute               ozone fs -copyFromLocal NOTICE.txt o3://bucket1.fstest/KEY.txt
                        Should contain    ${result}         'File exists'
                        Execute               ozone fs -get o3://bucket1.fstest/KEY.txt GET.txt
                        Execute               ls -l GET.txt
    ${result} =         Execute               ozone fs -ls o3://abcde.pqrs/
                        Should contain    ${result}         VOLUME_NOT_FOUND

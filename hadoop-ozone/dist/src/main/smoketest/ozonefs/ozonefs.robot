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
Documentation       Ozonefs test covering both o3fs and ofs
Library             OperatingSystem
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${OfsBucket1}          om:9862/fstest/bucket1
${O3fsBucket1}         bucket1.fstest/
${O3Bucket1}           o3://om/fstest/bucket1
${O3Bucket2}           o3://om/fstest/bucket2
${O3Bucket3}           o3://om/fstest2/bucket3

${OfsNonExistBucket}      om:9862/abc/def
${O3fsNonExistBucket}     def.abc/
${NonExistVolume}         abc

*** Test Cases ***
Create volume and bucket for Ozone file System test
    Execute             ozone sh volume create o3://om/fstest --quota 100TB
    Execute             ozone sh bucket create ${O3Bucket1}
    Execute             ozone sh bucket create ${O3Bucket2}

    Execute             ozone sh volume create o3://om/fstest2 --quota 100TB
    Execute             ozone sh bucket create ${O3Bucket3}


Check volume from o3fs
    ${result} =         Execute               ozone sh volume list
                        Should contain    ${result}         fstest
                        Should contain    ${result}         fstest2
                        Should Match Regexp  ${result}      "admin" : "(hadoop|testuser\/scm@EXAMPLE\.COM)"
    ${result} =         Execute               ozone fs -ls o3fs://${O3fsBucket1}

Test ozone shell with ofs
   Test ozone shell with scheme  ofs    om:9862/fstest/bucket1      om:9862/fstest/bucket2      om:9862/fstest2/bucket3      om:9862/abc/def

Test ozone shell with o3fs
   Test ozone shell with scheme  o3fs   bucket1.fstest/             bucket2.fstest/             bucket3.fstest2/             def.abc/

*** Keywords ***
Test ozone shell with scheme
    [arguments]         ${scheme}             ${testBucket1}        ${testBucket2}      ${testBucket3}      ${nonExistBucket}

    ${result} =         Execute               ozone fs -ls ${scheme}://${testBucket1}
                        Execute               ozone fs -mkdir -p ${scheme}://${testBucket1}/testdir/deep
    ${result} =         Execute               ozone sh key list ${O3Bucket1} | jq -r '.name'
                        Should contain    ${result}         testdir/deep

                        Execute               ozone fs -copyFromLocal NOTICE.txt ${scheme}://${testBucket1}/testdir/deep/
    ${result} =         Execute               ozone sh key list ${O3Bucket1} | jq -r '.name'
                        Should contain    ${result}         NOTICE.txt
    ${result} =         Execute               ozone sh key info o3://om/fstest/bucket1/testdir/deep/NOTICE.txt | jq -r '.replicationFactor'
                        Should Be Equal   ${result}         3

                        Execute               ozone fs -put NOTICE.txt ${scheme}://${testBucket1}/testdir/deep/PUTFILE.txt
    ${result} =         Execute               ozone sh key list ${O3Bucket1} | jq -r '.name'
                        Should contain    ${result}         PUTFILE.txt

    ${result} =         Execute               ozone fs -ls ${scheme}://${testBucket1}/testdir/deep/
                        Should contain    ${result}         NOTICE.txt
                        Should contain    ${result}         PUTFILE.txt

                        Execute               ozone fs -mv ${scheme}://${testBucket1}/testdir/deep/NOTICE.txt ${scheme}://${testBucket1}/testdir/deep/MOVED.TXT
    ${result} =         Execute               ozone sh key list ${O3Bucket1} | jq -r '.name'
                        Should contain    ${result}         MOVED.TXT
                        Should not contain  ${result}       NOTICE.txt

                        Execute               ozone fs -mkdir -p ${scheme}://${testBucket1}/testdir/deep/subdir1
                        Execute               ozone fs -cp ${scheme}://${testBucket1}/testdir/deep/MOVED.TXT ${scheme}://${testBucket1}/testdir/deep/subdir1/NOTICE.txt
    ${result} =         Execute               ozone sh key list ${O3Bucket1} | jq -r '.name'
                        Should contain    ${result}         subdir1/NOTICE.txt

    ${result} =         Execute               ozone fs -ls ${scheme}://${testBucket1}/testdir/deep/subdir1/
                        Should contain    ${result}         NOTICE.txt

                        Execute               ozone fs -cat ${scheme}://${testBucket1}/testdir/deep/subdir1/NOTICE.txt
                        Should not contain  ${result}       Failed

                        Execute               ozone fs -rm ${scheme}://${testBucket1}/testdir/deep/subdir1/NOTICE.txt
    ${result} =         Execute               ozone sh key list ${O3Bucket1} | jq -r '.name'
                        Should not contain  ${result}       NOTICE.txt

    ${result} =         Execute               ozone fs -rmdir ${scheme}://${testBucket1}/testdir/deep/subdir1/
    ${result} =         Execute               ozone sh key list ${O3Bucket1} | jq -r '.name'
                        Should not contain  ${result}       subdir1

                        Execute               ozone fs -touch ${scheme}://${testBucket1}/testdir/TOUCHFILE.txt
    ${result} =         Execute               ozone sh key list ${O3Bucket1} | jq -r '.name'
                        Should contain  ${result}       TOUCHFILE.txt

                        Execute               ozone fs -rm -r ${scheme}://${testBucket1}/testdir/
    ${result} =         Execute               ozone sh key list ${O3Bucket1} | jq -r '.name'
                        Should not contain  ${result}       testdir

                        Execute               rm -Rf /tmp/localdir1
                        Execute               mkdir /tmp/localdir1
                        Execute               cp NOTICE.txt /tmp/localdir1/LOCAL.txt
                        Execute               ozone fs -mkdir -p ${scheme}://${testBucket1}/testdir1
                        Execute               ozone fs -copyFromLocal /tmp/localdir1 ${scheme}://${testBucket1}/testdir1/
                        Execute               ozone fs -put NOTICE.txt ${scheme}://${testBucket1}/testdir1/NOTICE.txt

    ${result} =         Execute               ozone fs -ls -R ${scheme}://${testBucket1}/testdir1/
                        Should contain    ${result}         localdir1/LOCAL.txt
                        Should contain    ${result}         testdir1/NOTICE.txt

                        Execute               ozone fs -mkdir -p ${scheme}://${testBucket2}/testdir2
                        Execute               ozone fs -mkdir -p ${scheme}://${testBucket3}/testdir3

                        Execute               ozone fs -cp ${scheme}://${testBucket1}/testdir1/localdir1 ${scheme}://${testBucket2}/testdir2/
                        Execute               ozone fs -cp ${scheme}://${testBucket1}/testdir1/localdir1 ${scheme}://${testBucket3}/testdir3/

                        Execute               ozone sh key put ${O3Bucket1}/KEY.txt NOTICE.txt
    ${result} =         Execute               ozone fs -ls ${scheme}://${testBucket1}/KEY.txt
                        Should contain    ${result}         KEY.txt

    ${rc}  ${result} =  Run And Return Rc And Output        ozone fs -copyFromLocal NOTICE.txt ${scheme}://${testBucket1}/KEY.txt
                        Should Be Equal As Integers     ${rc}                1
                        Should contain    ${result}         File exists

                        Execute               rm -Rf /tmp/GET.txt
                        Execute               ozone fs -get ${scheme}://${testBucket1}/KEY.txt /tmp/GET.txt
                        Execute               ls -l /tmp/GET.txt

    ${rc}  ${result} =  Run And Return Rc And Output        ozone fs -ls ${scheme}://${nonExistBucket}
                        Should Be Equal As Integers     ${rc}                1
                        Should Match Regexp    ${result}         (Check access operation failed)|(Volume ${nonExistVolume} is not found)|(No such file or directory)

# Final clean up before next run
                        Execute               ozone fs -rm -r ${scheme}://${testBucket1}/testdir1/
                        Execute               ozone fs -rm -r ${scheme}://${testBucket2}/testdir2/
                        Execute               ozone fs -rm -r ${scheme}://${testBucket3}/testdir3/
                        Execute               ozone fs -rm -r ${scheme}://${testBucket1}/*
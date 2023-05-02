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
Documentation       S3 gateway test with aws cli
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${OZONE_TEST}         true
${BUCKET}             generated

*** Test Cases ***

Put object to s3
                        Execute                    echo "Randomtext" > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --body /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/putobject/key=value/
                        Should contain             ${result}         f1

                        Execute                    touch -f /tmp/zerobyte
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/zerobyte --body /tmp/zerobyte
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/putobject/key=value/
                        Should contain             ${result}         zerobyte

#This test depends on the previous test case. Can't be executes alone
Get object from s3
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 /tmp/testfile.result
    Compare files               /tmp/testfile              /tmp/testfile.result

#This test depends on the previous test case. Can't be executed alone
Get object with wrong signature
    Pass Execution If          '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    ${result} =                 Execute and Ignore Error   curl -i -H 'Authorization: AWS scm/scm@EXAMPLE.COM:asdfqwerty' ${ENDPOINT_URL}/${BUCKET}/${PREFIX}/putobject/key=value/f1
                                Should contain             ${result}        403 Forbidden

Get Partial object from s3 with both start and endoffset
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=0-4 /tmp/testfile1.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-4/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=/tmp/testfile skip=0 bs=1 count=5 2>/dev/null
    ${actualData} =             Execute                    cat /tmp/testfile1.result
                                Should Be Equal            ${expectedData}            ${actualData}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=2-4 /tmp/testfile1.result1
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 2-4/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=/tmp/testfile skip=2 bs=1 count=3 2>/dev/null
    ${actualData} =             Execute                    cat /tmp/testfile1.result1
                                Should Be Equal            ${expectedData}            ${actualData}

# end offset greater than file size and start with in file length
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=2-1000 /tmp/testfile1.result2
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 2-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=/tmp/testfile skip=2 bs=1 count=9 2>/dev/null
    ${actualData} =             Execute                    cat /tmp/testfile1.result2
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 with both start and endoffset(start offset and endoffset is greater than file size)
    ${result} =                 Execute AWSS3APICli and checkrc        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=10000-10000 /tmp/testfile2.result   255
                                Should contain             ${result}        InvalidRange


Get Partial object from s3 with both start and endoffset(end offset is greater than file size)
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=0-10000 /tmp/testfile2.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat /tmp/testfile
    ${actualData} =             Execute                    cat /tmp/testfile2.result
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 with only start offset
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=0- /tmp/testfile3.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat /tmp/testfile
    ${actualData} =             Execute                    cat /tmp/testfile3.result
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 with both start and endoffset which are equal
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=0-0 /tmp/testfile4.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-0/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=/tmp/testfile skip=0 bs=1 count=1 2>/dev/null
    ${actualData} =             Execute                    cat /tmp/testfile4.result
                                Should Be Equal            ${expectedData}            ${actualData}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=4-4 /tmp/testfile5.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 4-4/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=/tmp/testfile skip=4 bs=1 count=1 2>/dev/null
    ${actualData} =             Execute                    cat /tmp/testfile5.result
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 to get last n bytes
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=-4 /tmp/testfile6.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 7-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=/tmp/testfile skip=7 bs=1 count=4 2>/dev/null
    ${actualData} =             Execute                    cat /tmp/testfile6.result
                                Should Be Equal            ${expectedData}            ${actualData}

# if end is greater than file length, returns whole file
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=-10000 /tmp/testfile7.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat /tmp/testfile
    ${actualData} =             Execute                    cat /tmp/testfile7.result
                                Should Be Equal            ${expectedData}            ${actualData}

Incorrect values for end and start offset
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=-11-10000 /tmp/testfile8.result
                                Should not contain         ${result}        ContentRange
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat /tmp/testfile
    ${actualData} =             Execute                    cat /tmp/testfile8.result
                                Should Be Equal            ${expectedData}            ${actualData}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=11-8 /tmp/testfile9.result
                                Should not contain         ${result}        ContentRange
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat /tmp/testfile
    ${actualData} =             Execute                    cat /tmp/testfile8.result
                                Should Be Equal            ${expectedData}            ${actualData}

Zero byte file
    ${result} =			        Execute                    ozone sh bucket info /s3v/${BUCKET}
    ${linked} = 		        Execute				       echo '${result}' | jq -j '.sourceVolume,"/",.sourceBucket'
    ${eval} = 			        Evaluate			       "source" in """${linked}"""
    	      			        IF	${eval} == ${True}
    ${result} =                 Execute				       ozone sh bucket info ${linked}
				                END
    ${fsolayout} =    		    Evaluate    	   	       "OPTIMIZED" in """${result}"""

    ${result} =                 Execute AWSS3APICli and checkrc     get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/zerobyte --range bytes=0-0 /tmp/testfile2.result   255
    	      			        IF 	${fsolayout} == ${True}
                                Should contain              ${result}       NoSuchKey
				                ELSE
                                Should contain              ${result}       InvalidRange
				                END

    ${result} =                 Execute AWSS3APICli and checkrc     get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/zerobyte --range bytes=0-1 /tmp/testfile2.result   255
                                IF 	${fsolayout} == ${True}
                                Should contain              ${result}       NoSuchKey
                                ELSE
                                Should contain              ${result}       InvalidRange
				                END

    ${result} =                 Execute AWSS3APICli and checkrc     get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/zerobyte --range bytes=0-10000 /tmp/testfile2.result   255
    	      			        IF 	${fsolayout} == ${True}
                                Should contain              ${result}       NoSuchKey
				                ELSE
                                Should contain              ${result}       InvalidRange
				                END

Create file with user defined metadata
                                Execute                   echo "Randomtext" > /tmp/testfile2
                                Execute AWSS3ApiCli       put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key1 --body /tmp/testfile2 --metadata="custom-key1=custom-value1,custom-key2=custom-value2"

    ${result} =                 Execute AWSS3APICli       head-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key1
                                Should contain            ${result}    \"custom-key1\": \"custom-value1\"
                                Should contain            ${result}    \"custom-key2\": \"custom-value2\"

    ${result} =                 Execute                   ozone sh key info /s3v/${BUCKET}/${PREFIX}/putobject/custom-metadata/key1
                                Should contain            ${result}   \"custom-key1\" : \"custom-value1\"
                                Should contain            ${result}   \"custom-key2\" : \"custom-value2\"

Create file with user defined metadata with gdpr enabled value in request
                                Execute                    echo "Randomtext" > /tmp/testfile2
                                Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key2 --body /tmp/testfile2 --metadata="gdprEnabled=true,custom-key2=custom-value2"
    ${result} =                 Execute AWSS3ApiCli        head-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key2
                                Should contain             ${result}   \"custom-key2\": \"custom-value2\"
                                Should not contain         ${result}   \"gdprEnabled\": \"true\"


Create file with user defined metadata size larger than 2 KB
                                Execute                    echo "Randomtext" > /tmp/testfile2
    ${custom_metadata_value} =  Execute                    printf 'v%.0s' {1..3000}
    ${result} =                 Execute AWSS3APICli and ignore error        put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key2 --body /tmp/testfile2 --metadata="custom-key1=${custom_metadata_value}"
                                Should not contain                          ${result}   custom-key1: ${custom_metadata_value}

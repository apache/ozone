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
${BUCKET1}            generated

*** Keywords ***
Put object to s3
    [Arguments]         ${BUCKET}
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
    [Arguments]         ${BUCKET}
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 /tmp/testfile.result
    Compare files               /tmp/testfile              /tmp/testfile.result
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/zerobyte /tmp/zerobyte.result
    Compare files               /tmp/zerobyte              /tmp/zerobyte.result

#This test depends on the previous test case. Can't be executed alone
Get object with wrong signature
    [Arguments]         ${BUCKET}
    Pass Execution If          '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    ${result} =                 Execute and Ignore Error   curl -i -H 'Authorization: AWS scm/scm@EXAMPLE.COM:asdfqwerty' ${ENDPOINT_URL}/${BUCKET}/${PREFIX}/putobject/key=value/f1
                                Should contain             ${result}        403 Forbidden

Get Partial object from s3 with both start and endoffset
    [Arguments]         ${BUCKET}
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
    [Arguments]         ${BUCKET}
    ${result} =                 Execute AWSS3APICli and checkrc        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=10000-10000 /tmp/testfile2.result   255
                                Should contain             ${result}        InvalidRange


Get Partial object from s3 with both start and endoffset(end offset is greater than file size)
    [Arguments]         ${BUCKET}
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=0-10000 /tmp/testfile2.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat /tmp/testfile
    ${actualData} =             Execute                    cat /tmp/testfile2.result
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 with only start offset
    [Arguments]         ${BUCKET}
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --range bytes=0- /tmp/testfile3.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat /tmp/testfile
    ${actualData} =             Execute                    cat /tmp/testfile3.result
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 with both start and endoffset which are equal
    [Arguments]         ${BUCKET}
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
    [Arguments]         ${BUCKET}
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
    [Arguments]         ${BUCKET}
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
    [Arguments]         ${BUCKET}
    ${result} =                 Execute AWSS3APICli and checkrc     get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/zerobyte --range bytes=0-0 /tmp/testfile2.result   255
                                Should contain              ${result}       InvalidRange

    ${result} =                 Execute AWSS3APICli and checkrc     get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/zerobyte --range bytes=0-1 /tmp/testfile2.result   255
                                Should contain              ${result}       InvalidRange

    ${result} =                 Execute AWSS3APICli and checkrc     get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/zerobyte --range bytes=0-10000 /tmp/testfile2.result   255
                                Should contain              ${result}       InvalidRange

Create file with user defined metadata
    [Arguments]         ${BUCKET}
                                Execute                   echo "Randomtext" > /tmp/testfile2
                                Execute AWSS3ApiCli       put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key1 --body /tmp/testfile2 --metadata="custom-key1=custom-value1,custom-key2=custom-value2"

    ${result} =                 Execute AWSS3APICli       head-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key1
                                Should contain            ${result}    \"custom-key1\": \"custom-value1\"
                                Should contain            ${result}    \"custom-key2\": \"custom-value2\"

    ${result} =                 Execute                   ozone sh key info /s3v/${BUCKET}/${PREFIX}/putobject/custom-metadata/key1
                                Should contain            ${result}   \"custom-key1\" : \"custom-value1\"
                                Should contain            ${result}   \"custom-key2\" : \"custom-value2\"

Create file with user defined metadata with gdpr enabled value in request
    [Arguments]         ${BUCKET}
                                Execute                    echo "Randomtext" > /tmp/testfile2
                                Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key2 --body /tmp/testfile2 --metadata="gdprEnabled=true,custom-key2=custom-value2"
    ${result} =                 Execute AWSS3ApiCli        head-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key2
                                Should contain             ${result}   \"custom-key2\": \"custom-value2\"
                                Should not contain         ${result}   \"gdprEnabled\": \"true\"


Create file with user defined metadata size larger than 2 KB
    [Arguments]         ${BUCKET}
                                Execute                    echo "Randomtext" > /tmp/testfile2
    ${custom_metadata_value} =  Execute                    printf 'v%.0s' {1..3000}
    ${result} =                 Execute AWSS3APICli and checkrc       put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/custom-metadata/key2 --body /tmp/testfile2 --metadata="custom-key1=${custom_metadata_value}"    255
                                Should contain                        ${result}   MetadataTooLarge
                                Should not contain                    ${result}   custom-key1: ${custom_metadata_value}

Create small file and expect ETag (MD5) in a reponse header
    [Arguments]         ${BUCKET}
                                Execute                    head -c 1MB </dev/urandom > /tmp/small_file
    ${file_md5_checksum} =      Execute                    md5sum /tmp/small_file | awk '{print $1}'
    ${result} =                 Execute AWSS3CliDebug      cp /tmp/small_file s3://${BUCKET}
                                Should Match Regexp        ${result}    (?i)Response header.*ETag':\ '"${file_md5_checksum}"'

# The next two test cases depends on the previous one
Download small file end expect ETag (MD5) in a response header
    [Arguments]         ${BUCKET}
    ${file_md5_checksum} =      Execute                    md5sum /tmp/small_file | awk '{print $1}'
    ${result} =                 Execute AWSS3CliDebug      cp s3://${BUCKET}/small_file /tmp/small_file_downloaded
                                Should Match Regexp        ${result}    (?is)HEAD /${BUCKET}/small_file.*?Response headers.*?ETag': '"${file_md5_checksum}"'
                                Should Match Regexp        ${result}    (?is)GET /${BUCKET}/small_file.*?Response headers.*?ETag':\ '"${file_md5_checksum}"'
                                # clean up
                                Execute AWSS3Cli           rm s3://${BUCKET}/small_file
                                Execute                    rm /tmp/small_file_downloaded

Create key with custom etag metadata and expect it won't conflict with ETag response header of HEAD request
    [Arguments]         ${BUCKET}
    ${file_md5_checksum}                    Execute                             md5sum /tmp/small_file | awk '{print $1}'
                                            Execute AWSS3CliDebug               cp --metadata "ETag=custom-etag-value" /tmp/small_file s3://${BUCKET}/test_file
    ${result}                               Execute AWSS3CliDebug               cp s3://${BUCKET}/test_file /tmp/test_file_downloaded
    ${match}    ${ETag}     ${etagCustom}   Should Match Regexp                 ${result}    HEAD /${BUCKET}/test_file\ .*?Response headers.*?ETag':\ '"(.*?)"'.*?x-amz-meta-etag':\ '(.*?)'     flags=DOTALL
                                            Should Be Equal As Strings          ${ETag}     ${file_md5_checksum}
                                            Should BE Equal As Strings          ${etagCustom}       custom-etag-value
                                            Should Not Be Equal As Strings      ${ETag}     ${etagCustom}
                                            # clean up
                                            Execute AWSS3Cli                    rm s3://${BUCKET}/test_file
                                            Execute                             rm -rf /tmp/small_file
                                            Execute                             rm -rf /tmp/test_file_downloaded

Create&Download big file by multipart upload and expect ETag in a file download response
    [Arguments]         ${BUCKET}
                                Execute                    head -c 10MB </dev/urandom > /tmp/big_file
    ${result}                   Execute AWSS3CliDebug      cp /tmp/big_file s3://${BUCKET}/
    ${match}    ${etag1}        Should Match Regexp        ${result}    (?is)POST /${BUCKET}/big_file\\?uploadId=.*?Response body.*?ETag>"(.*?-2)"
    ${file_download_result}     Execute AWSS3CliDebug      cp s3://${BUCKET}/big_file /tmp/big_file_downloaded
    ${match}    ${etag2}        Should Match Regexp        ${file_download_result}    (?is)GET /${BUCKET}/big_file.*?Response headers.*?ETag':\ '"(.*?-2)"'
                                Should Be Equal As Strings    ${etag1}     ${etag2}
                                # clean up
                                Execute AWSS3Cli           rm s3://${BUCKET}/big_file
                                Execute                    rm -rf /tmp/big_file

Create key twice with different content and expect different ETags
    [Arguments]         ${BUCKET}
                                Execute                    head -c 1MiB </dev/urandom > /tmp/file1
                                Execute                    head -c 1MiB </dev/urandom > /tmp/file2
    ${file1UploadResult}        Execute AWSS3CliDebug      cp /tmp/file1 s3://${BUCKET}/test_key_to_check_etag_differences
    ${match}    ${etag1}        Should Match Regexp        ${file1UploadResult}     PUT /${BUCKET}/test_key_to_check_etag_differences\ .*?Response headers.*?ETag':\ '"(.*?)"'    flags=DOTALL
    ${file2UploadResult}        Execute AWSS3CliDebug      cp /tmp/file2 s3://${BUCKET}/test_key_to_check_etag_differences
    ${match}    ${etag2}        Should Match Regexp        ${file2UploadResult}     PUT /${BUCKET}/test_key_to_check_etag_differences\ .*?Response headers.*?ETag':\ '"(.*?)"'    flags=DOTALL
                                Should Not Be Equal As Strings  ${etag1}    ${etag2}
                                # clean up
                                Execute AWSS3Cli           rm s3://${BUCKET}/test_key_to_check_etag_differences
                                Execute                    rm -rf /tmp/file1
                                Execute                    rm -rf /tmp/file2

*** Test Cases ***
Put object to s3 with OBS
    Put object to s3    ${BUCKET}
Put object to s3 with FSO
    Put object to s3    ${BUCKET1}

Get object from s3 with OBS
    Get object from s3    ${BUCKET}
Get object from s3 with FSO
    Get object from s3    ${BUCKET1}

Get object with wrong signature with OBS
    Get object with wrong signature    ${BUCKET}
Get object with wrong signature with FSO
    Get object with wrong signature    ${BUCKET1}

Get Partial object from s3 with both start and endoffset with OBS
    Get Partial object from s3 with both start and endoffset    ${BUCKET}
Get Partial object from s3 with both start and endoffset with FSO
    Get Partial object from s3 with both start and endoffset    ${BUCKET1}

Get Partial object from s3 with both start and endoffset(start offset and endoffset is greater than file size) with OBS
    Get Partial object from s3 with both start and endoffset(start offset and endoffset is greater than file size)    ${BUCKET}
Get Partial object from s3 with both start and endoffset(start offset and endoffset is greater than file size) with FSO
    Get Partial object from s3 with both start and endoffset(start offset and endoffset is greater than file size)    ${BUCKET1}

Get Partial object from s3 with both start and endoffset(end offset is greater than file size) with OBS
    Get Partial object from s3 with both start and endoffset(end offset is greater than file size)    ${BUCKET}
Get Partial object from s3 with both start and endoffset(end offset is greater than file size) with FSO
    Get Partial object from s3 with both start and endoffset(end offset is greater than file size)    ${BUCKET1}

Get Partial object from s3 with only start offset with OBS
    Get Partial object from s3 with only start offset    ${BUCKET}
Get Partial object from s3 with only start offset with FSO
    Get Partial object from s3 with only start offset    ${BUCKET1}

Get Partial object from s3 with both start and endoffset which are equal with OBS
    Get Partial object from s3 with both start and endoffset which are equal    ${BUCKET}
Get Partial object from s3 with both start and endoffset which are equal with FSO
    Get Partial object from s3 with both start and endoffset which are equal    ${BUCKET1}

Get Partial object from s3 to get last n bytes with OBS
    Get Partial object from s3 to get last n bytes    ${BUCKET}
Get Partial object from s3 to get last n bytes with FSO
    Get Partial object from s3 to get last n bytes    ${BUCKET1}

Incorrect values for end and start offset with OBS
    Incorrect values for end and start offset    ${BUCKET}
Incorrect values for end and start offset with FSO
    Incorrect values for end and start offset    ${BUCKET1}

Zero byte file with OBS
    Zero byte file    ${BUCKET}
Zero byte file with FSO
    Zero byte file    ${BUCKET1}

Create file with user defined metadata with OBS
    Create file with user defined metadata    ${BUCKET}
Create file with user defined metadata with FSO
    Create file with user defined metadata    ${BUCKET1}

Create file with user defined metadata with gdpr enabled value in request with OBS
    Create file with user defined metadata with gdpr enabled value in request    ${BUCKET}
Create file with user defined metadata with gdpr enabled value in request with FSO
    Create file with user defined metadata with gdpr enabled value in request    ${BUCKET1}

Create file with user defined metadata size larger than 2 KB with OBS
    Create file with user defined metadata size larger than 2 KB    ${BUCKET}
Create file with user defined metadata size larger than 2 KB with FSO
    Create file with user defined metadata size larger than 2 KB    ${BUCKET1}

Create small file and expect ETag (MD5) in a reponse header with OBS
    Create small file and expect ETag (MD5) in a reponse header    ${BUCKET}
Download small file end expect ETag (MD5) in a response header with OBS
    Download small file end expect ETag (MD5) in a response header    ${BUCKET}
Create key with custom etag metadata and expect it won't conflict with ETag response header of HEAD request with OBS
    Create key with custom etag metadata and expect it won't conflict with ETag response header of HEAD request    ${BUCKET}

Create small file and expect ETag (MD5) in a reponse header with FSO
    Create small file and expect ETag (MD5) in a reponse header    ${BUCKET1}
Download small file end expect ETag (MD5) in a response header with FSO
    Download small file end expect ETag (MD5) in a response header    ${BUCKET1}
Create key with custom etag metadata and expect it won't conflict with ETag response header of HEAD request with FSO
    Create key with custom etag metadata and expect it won't conflict with ETag response header of HEAD request    ${BUCKET1}

Create&Download big file by multipart upload and expect ETag in a file download response with OBS
    Create&Download big file by multipart upload and expect ETag in a file download response    ${BUCKET}
Create&Download big file by multipart upload and expect ETag in a file download response with FSO
    Create&Download big file by multipart upload and expect ETag in a file download response    ${BUCKET1}

Create key twice with different content and expect different ETags with OBS
    Create key twice with different content and expect different ETags    ${BUCKET}
Create key twice with different content and expect different ETags with FSO
    Create key twice with different content and expect different ETags    ${BUCKET1}

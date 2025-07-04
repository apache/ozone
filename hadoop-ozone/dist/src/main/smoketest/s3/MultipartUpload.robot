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
Library             DateTime
Resource            ../commonlib.robot
Resource            commonawslib.robot
Resource            mpu_lib.robot
Test Timeout        5 minutes
Suite Setup         Setup Multipart Tests
Suite Teardown      Teardown Multipart Tests
Test Setup          Generate random prefix

*** Keywords ***
Setup Multipart Tests
    Setup s3 tests

    # 5MB + a bit
    Create Random File KB    5121    /tmp/part1

    # 1MB - a bit
    Create Random File KB    1023    /tmp/part2

    Create Random File MB    10      /tmp/10mb
    Create Random File MB    22      /tmp/22mb
    Create Random File KB    10      /tmp/10kb


Teardown Multipart Tests
    Remove Files    /tmp/part1 /tmp/part2 /tmp/10mb /tmp/22mb /tmp/10kb


Wait Til Date Past
    [arguments]         ${date}
    ${latestDate} =     Get Current Date         UTC
    ${sleepSeconds} =   Subtract Date From Date  ${date}  ${latestDate}
    Run Keyword If      ${sleepSeconds} > 0      Sleep  ${sleepSeconds}

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

Test Multipart Upload With Adjusted Length
    Perform Multipart Upload    ${BUCKET}    multipart/adjusted_length_${PREFIX}    /tmp/part1    /tmp/part2
    Verify Multipart Upload     ${BUCKET}    multipart/adjusted_length_${PREFIX}    /tmp/part1    /tmp/part2

Overwrite Empty File
    Execute                     touch ${TEMP_DIR}/empty
    Execute AWSS3Cli            cp ${TEMP_DIR}/empty s3://${BUCKET}/empty_file_${PREFIX}
    Perform Multipart Upload    ${BUCKET}    empty_file_${PREFIX}    /tmp/part1    /tmp/part2
    Verify Multipart Upload     ${BUCKET}    empty_file_${PREFIX}    /tmp/part1    /tmp/part2

Test Multipart Upload
    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/multipartKey
    ${nextUploadID} =   Initiate MPU    ${BUCKET}    ${PREFIX}/multipartKey
                        Should Not Be Equal     ${uploadID}  ${nextUploadID}

# upload part
# each part should be minimum 5mb, other wise during complete multipart
# upload we get error entity too small. So, considering further complete
# multipart upload, uploading each part as 5MB file, exception is for last part

                        Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey    ${nextUploadID}    1    /tmp/part1
                        Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey    ${nextUploadID}    1    /tmp/part1


Test Multipart Upload Complete
    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/multipartKey1    0     --metadata="custom-key1=custom-value1,custom-key2=custom-value2,gdprEnabled=true" --tagging="tag-key1=tag-value1&tag-key2=tag-value2"

    ${eTag1} =          Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey1    ${uploadID}    1    /tmp/part1
    ${eTag2} =          Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey1    ${uploadID}    2    /tmp/part2

#complete multipart upload without any parts
    ${result} =         Execute AWSS3APICli and checkrc    complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey1    255
                        Should contain    ${result}    InvalidRequest
                        Should contain    ${result}    must specify at least one part

#complete multipart upload
    ${resultETag} =     Complete MPU    ${BUCKET}    ${PREFIX}/multipartKey1    ${uploadID}    {ETag=${eTag1},PartNumber=1},{ETag=${eTag2},PartNumber=2}
    ${expectedResultETag} =     Execute                       echo -n ${eTag1}${eTag2} | md5sum | awk '{print $1}'
                                Should Be Equal As Strings    ${resultETag}     "${expectedResultETag}-2"

#check whether the user defined metadata and parts count can be retrieved
    ${result} =                 Execute AWSS3ApiCli           head-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey1
                                Should contain                ${result}    \"custom-key1\": \"custom-value1\"
                                Should contain                ${result}    \"custom-key2\": \"custom-value2\"

    ${partsCount}               Execute and checkrc           echo '${result}' | jq -r '.PartsCount'    0
                                Should Be Equal               ${partsCount}    2

    ${result} =                 Execute                       ozone sh key info /s3v/${BUCKET}/${PREFIX}/multipartKey1
                                Should contain                ${result}    \"custom-key1\" : \"custom-value1\"
                                Should contain                ${result}    \"custom-key2\" : \"custom-value2\"
                                Should not contain            ${result}    \"gdprEnabled\": \"true\"
                                Should contain                ${result}    \"tag-key1\" : \"tag-value1\"
                                Should contain                ${result}    \"tag-key2\" : \"tag-value2\"

#read file and check the key, tag count and parts count
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 /tmp/${PREFIX}-multipartKey1.result
                                Should contain             ${result}      TagCount

    ${tagCount} =               Execute and checkrc        echo '${result}' | jq -r '.TagCount'    0
                                Should Be Equal            ${tagCount}    2

    ${partsCount}               Execute and checkrc        echo '${result}' | jq -r '.PartsCount'    0
                                Should Be Equal            ${partsCount}    2

                                Execute                    cat /tmp/part1 /tmp/part2 > /tmp/${PREFIX}-multipartKey1
    Compare files               /tmp/${PREFIX}-multipartKey1         /tmp/${PREFIX}-multipartKey1.result

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --part-number 1 /tmp/${PREFIX}-multipartKey1-part1.result
    Compare files               /tmp/part1        /tmp/${PREFIX}-multipartKey1-part1.result

    ${tagCount} =               Execute and checkrc        echo '${result}' | jq -r '.TagCount'    0
                                Should Be Equal            ${tagCount}    2

    ${partsCount}               Execute and checkrc        echo '${result}' | jq -r '.PartsCount'    0
                                Should Be Equal            ${partsCount}    2

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --part-number 2 /tmp/${PREFIX}-multipartKey1-part2.result
    Compare files               /tmp/part2        /tmp/${PREFIX}-multipartKey1-part2.result

Test Multipart Upload with user defined metadata size larger than 2 KB
    ${custom_metadata_value} =  Generate Random String   3000
    ${result} =    Initiate MPU    ${BUCKET}    ${PREFIX}/mpuWithLargeMetadata    255     --metadata="custom-key1=${custom_metadata_value}"
                                Should contain                        ${result}   MetadataTooLarge
                                Should not contain                    ${result}   custom-key1: ${custom_metadata_value}

Test Multipart Upload Complete Entity too small
    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/multipartKey2
    ${parts} =          Upload MPU parts    ${BUCKET}    ${PREFIX}/multipartKey2    ${uploadID}    /tmp/10kb    /tmp/10kb
    ${result} =         Complete MPU    ${BUCKET}    ${PREFIX}/multipartKey2    ${uploadID}    ${parts}    255
                        Should contain          ${result}    EntityTooSmall


Test Multipart Upload Complete Invalid part errors and complete mpu with few parts
    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/multipartKey3

#complete multipart upload when no parts uploaded
    ${result} =         Complete MPU    ${BUCKET}    ${PREFIX}/multipartKey3    ${uploadID}    {ETag=etag1,PartNumber=1},{ETag=etag2,PartNumber=2}    255
                        Should contain          ${result}    InvalidPart
    ${result} =         Complete MPU    ${BUCKET}    ${PREFIX}/multipartKey3    ${uploadID}    {ETag=etag1,PartNumber=2},{ETag=etag2,PartNumber=1}    255
                        Should contain          ${result}    InvalidPart
#upload parts
    ${eTag1} =          Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey3    ${uploadID}    1    /tmp/part1
    ${eTag2} =          Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey3    ${uploadID}    2    /tmp/part1
    ${eTag3} =          Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey3    ${uploadID}    3    /tmp/part2

#complete multipart upload
    ${result} =         Complete MPU    ${BUCKET}    ${PREFIX}/multipartKey3    ${uploadID}   {ETag=etag1,PartNumber=1},{ETag=etag2,PartNumber=2}    255
                        Should contain          ${result}    InvalidPart
    ${result} =         Complete MPU    ${BUCKET}    ${PREFIX}/multipartKey3    ${uploadID}   {ETag=${eTag1},PartNumber=1},{ETag=etag2,PartNumber=2}    255
                        Should contain          ${result}    InvalidPart
    ${result} =         Complete MPU    ${BUCKET}    ${PREFIX}/multipartKey3    ${uploadID}   {ETag=${eTag1},PartNumber=4},{ETag=etag2,PartNumber=2}    255
                        Should contain          ${result}    InvalidPartOrder
#complete multipart upload(merge with few parts)
    ${result} =         Complete MPU    ${BUCKET}    ${PREFIX}/multipartKey3    ${uploadID}   {ETag=${eTag1},PartNumber=1},{ETag=${eTag3},PartNumber=3}

    ${result} =         Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 /tmp/${PREFIX}-multipartKey3.result
                        Execute                    cat /tmp/part1 /tmp/part2 > /tmp/${PREFIX}-multipartKey3
    Compare files       /tmp/${PREFIX}-multipartKey3         /tmp/${PREFIX}-multipartKey3.result

    ${result} =         Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --part-number 1 /tmp/${PREFIX}-multipartKey3-part1.result
    Compare files       /tmp/part1         /tmp/${PREFIX}-multipartKey3-part1.result

    ${result} =         Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --part-number 3 /tmp/${PREFIX}-multipartKey3-part2.result
    Compare files       /tmp/part2         /tmp/${PREFIX}-multipartKey3-part2.result

Test abort Multipart upload
    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/multipartKey4    0     --storage-class REDUCED_REDUNDANCY
    ${result} =         Abort MPU    ${BUCKET}    ${PREFIX}/multipartKey4    ${uploadID}    0

Test abort Multipart upload with invalid uploadId
    ${result} =         Abort MPU    ${BUCKET}    ${PREFIX}/multipartKey5    "random"    255

Upload part with Incorrect uploadID
    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/multipartKey
    ${result} =         Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey    "no-such-upload-id"    1    /tmp/10kb    255
                        Should contain          ${result}    NoSuchUpload

Test list parts
#initiate multipart upload
    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/multipartKey5

#upload parts
    ${eTag1} =          Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey5    ${uploadID}    1    /tmp/part1
    ${eTag2} =          Upload MPU part    ${BUCKET}    ${PREFIX}/multipartKey5    ${uploadID}    2    /tmp/part2

#list parts
    ${result} =         Execute AWSS3APICli   list-parts --bucket ${BUCKET} --key ${PREFIX}/multipartKey5 --upload-id ${uploadID}
    ${part1} =          Execute and checkrc    echo '${result}' | jq -r '.Parts[0].ETag'  0
    ${part2} =          Execute and checkrc    echo '${result}' | jq -r '.Parts[1].ETag'  0
                        Should Be equal       ${part1}    ${eTag1}
                        Should contain        ${part2}    ${eTag2}
                        Should contain        ${result}    STANDARD

#list parts with max-items and next token
    ${result} =         Execute AWSS3APICli   list-parts --bucket ${BUCKET} --key ${PREFIX}/multipartKey5 --upload-id ${uploadID} --max-items 1
    ${part1} =          Execute and checkrc    echo '${result}' | jq -r '.Parts[0].ETag'  0
    ${token} =          Execute and checkrc    echo '${result}' | jq -r '.NextToken'  0
                        Should Be equal       ${part1}    ${eTag1}
                        Should contain        ${result}   STANDARD

    ${result} =         Execute AWSS3APICli   list-parts --bucket ${BUCKET} --key ${PREFIX}/multipartKey5 --upload-id ${uploadID} --max-items 1 --starting-token ${token}
    ${part2} =          Execute and checkrc    echo '${result}' | jq -r '.Parts[0].ETag'  0
                       Should Be equal       ${part2}    ${eTag2}
                       Should contain        ${result}   STANDARD

#finally abort it
    ${result} =         Abort MPU    ${BUCKET}    ${PREFIX}/multipartKey5    ${uploadID}    0

Test Multipart Upload with the simplified aws s3 cp API
                        Execute AWSS3Cli        cp /tmp/22mb s3://${BUCKET}/mpyawscli
                        Execute AWSS3Cli        cp s3://${BUCKET}/mpyawscli /tmp/22mb.result
                        Execute AWSS3Cli        rm s3://${BUCKET}/mpyawscli
                        Compare files           /tmp/22mb        /tmp/22mb.result

Test Multipart Upload Put With Copy
    ${result} =         Execute AWSS3APICli     put-object --bucket ${BUCKET} --key ${PREFIX}/copytest/source --body /tmp/part1


    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/copytest/destination
    ${result} =         Execute AWSS3APICli      upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copytest/destination --upload-id ${uploadID} --part-number 1 --copy-source ${BUCKET}/${PREFIX}/copytest/source
                        Should contain           ${result}    ETag
                        Should contain           ${result}    LastModified
    ${eTag1} =          Execute and checkrc      echo '${result}' | jq -r '.CopyPartResult.ETag'   0


                        Complete MPU    ${BUCKET}    ${PREFIX}/copytest/destination    ${uploadID}    {ETag=${eTag1},PartNumber=1}
                        Execute AWSS3APICli     get-object --bucket ${BUCKET} --key ${PREFIX}/copytest/destination /tmp/part-result

                        Compare files           /tmp/part1        /tmp/part-result

Test Multipart Upload Put With Copy and range
    ${result} =         Execute AWSS3APICli     put-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/source --body /tmp/10mb


    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/copyrange/destination

    ${result} =         Execute AWSS3APICli      upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --upload-id ${uploadID} --part-number 1 --copy-source ${BUCKET}/${PREFIX}/copyrange/source --copy-source-range bytes=0-10485757
                        Should contain           ${result}    ETag
                        Should contain           ${result}    LastModified
    ${eTag1} =          Execute and checkrc      echo '${result}' | jq -r '.CopyPartResult.ETag'   0

    ${result} =         Execute AWSS3APICli      upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --upload-id ${uploadID} --part-number 2 --copy-source ${BUCKET}/${PREFIX}/copyrange/source --copy-source-range bytes=10485758-10485759
                        Should contain           ${result}    ETag
                        Should contain           ${result}    LastModified
    ${eTag2} =          Execute and checkrc      echo '${result}' | jq -r '.CopyPartResult.ETag'   0


                        Complete MPU    ${BUCKET}    ${PREFIX}/copyrange/destination    ${uploadID}    {ETag=${eTag1},PartNumber=1},{ETag=${eTag2},PartNumber=2}
                        Execute AWSS3APICli     get-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination /tmp/part-result

                        Compare files           /tmp/10mb        /tmp/part-result

Test Multipart Upload Put With Copy and range with IfModifiedSince
    ${curDate} =        Get Current Date
    ${beforeCreate} =   Subtract Time From Date     ${curDate}  1 day
    ${tomorrow} =       Add Time To Date            ${curDate}  1 day

    ${result} =         Execute AWSS3APICli     put-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/source --body /tmp/10mb

    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/copyrange/destination

#calc time-to-sleep from time-last-modified plus a few seconds
    ${result} =         Execute AWSS3APICli      head-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/source
    ${lastModified} =   Execute and checkrc      echo '${result}' | jq -r '.LastModified'    0
                        Should contain           ${result}    ${LastModified}
    ${lmDate} =         Convert Date 	 	 ${lastModified}  date_format=%a, %d %b %Y %H:%M:%S %Z
    ${afterCreate} =    Add Time To Date         ${lmDate}  3 seconds
    Wait Til Date Past  ${afterCreate}

    ${result} =         Execute AWSS3APICli and checkrc     upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --upload-id ${uploadID} --part-number 1 --copy-source ${BUCKET}/${PREFIX}/copyrange/source --copy-source-range bytes=0-10485757 --copy-source-if-modified-since '${afterCreate}'    255
                        Should contain           ${result}    PreconditionFailed

    ${result} =         Execute AWSS3APICli and checkrc     upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --upload-id ${uploadID} --part-number 2 --copy-source ${BUCKET}/${PREFIX}/copyrange/source --copy-source-range bytes=10485758-10485759 --copy-source-if-unmodified-since '${beforeCreate}'  255
                        Should contain           ${result}    PreconditionFailed

    ${result} =         Execute AWSS3APICli and checkrc     upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --upload-id ${uploadID} --part-number 1 --copy-source ${BUCKET}/${PREFIX}/copyrange/source --copy-source-range bytes=0-10485757 --copy-source-if-modified-since '${beforeCreate}'   0
                        Should contain           ${result}    ETag
                        Should contain           ${result}    LastModified

    ${eTag1} =          Execute and checkrc      echo '${result}' | jq -r '.CopyPartResult.ETag'   0

    ${result} =         Execute AWSS3APICli and checkrc     upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --upload-id ${uploadID} --part-number 2 --copy-source ${BUCKET}/${PREFIX}/copyrange/source --copy-source-range bytes=10485758-10485759 --copy-source-if-unmodified-since '${afterCreate}'   0
                        Should contain           ${result}    ETag
                        Should contain           ${result}    LastModified

    ${eTag2} =          Execute and checkrc      echo '${result}' | jq -r '.CopyPartResult.ETag'   0

# future date strings cause precondition to be ignored
    ${result} =         Execute AWSS3APICli and checkrc     upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --upload-id ${uploadID} --part-number 1 --copy-source ${BUCKET}/${PREFIX}/copyrange/source --copy-source-range bytes=0-10485757 --copy-source-if-modified-since '${tomorrow}'   0
                        Should contain           ${result}    ETag
                        Should contain           ${result}    LastModified

    ${eTag1} =          Execute and checkrc      echo '${result}' | jq -r '.CopyPartResult.ETag'   0

                        Complete MPU    ${BUCKET}    ${PREFIX}/copyrange/destination    ${uploadID}   {ETag=${eTag1},PartNumber=1},{ETag=${eTag2},PartNumber=2}
                        Execute AWSS3APICli     get-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination /tmp/part-result

                        Compare files           /tmp/10mb        /tmp/part-result

Test Multipart Upload list
    # Create 25 multipart uploads to test pagination
    ${uploadIDs}=    Create List
    FOR    ${index}    IN RANGE    25
        ${key}=    Set Variable    ${PREFIX}/listtest/key-${index}
        ${uploadID}=    Initiate MPU    ${BUCKET}    ${key}
        Append To List    ${uploadIDs}    ${uploadID}
    END

    # Test listing with max-items=10 (should get 3 pages: 10, 10, 5)
    ${result}=    Execute AWSS3APICli    list-multipart-uploads --bucket ${BUCKET} --prefix ${PREFIX}/listtest --max-items 10
    
    # Verify first page
    ${count}=    Execute and checkrc    echo '${result}' | jq -r '.Uploads | length'    0
    Should Be Equal    ${count}    10
    
    ${hasNext}=    Execute and checkrc    echo '${result}' | jq -r 'has("NextToken")'    0
    Should Be Equal    ${hasNext}    true
    
    ${nextToken}=    Execute and checkrc    echo '${result}' | jq -r '.NextToken'    0
    Should Not Be Empty    ${nextToken}

    # Get second page
    ${result}=    Execute AWSS3APICli    list-multipart-uploads --bucket ${BUCKET} --prefix ${PREFIX}/listtest --max-items 10 --starting-token ${nextToken}
    
    # Verify second page
    ${count}=    Execute and checkrc    echo '${result}' | jq -r '.Uploads | length'    0
    Should Be Equal    ${count}    10
    
    ${hasNext}=    Execute and checkrc    echo '${result}' | jq -r 'has("NextToken")'    0
    Should Be Equal    ${hasNext}    true
    
    ${nextToken}=    Execute and checkrc    echo '${result}' | jq -r '.NextToken'    0
    Should Not Be Empty    ${nextToken}

    # Get last page
    ${result}=    Execute AWSS3APICli    list-multipart-uploads --bucket ${BUCKET} --prefix ${PREFIX}/listtest --max-items 10 --starting-token ${nextToken}
    
    # Verify last page
    ${count}=    Execute and checkrc    echo '${result}' | jq -r '.Uploads | length'    0
    Should Be Equal    ${count}    5
    
    ${hasNext}=    Execute and checkrc    echo '${result}' | jq -r 'has("NextToken")'    0
    Should Be Equal    ${hasNext}    false

    # Test prefix filtering
    ${result}=    Execute AWSS3APICli    list-multipart-uploads --bucket ${BUCKET} --prefix ${PREFIX}/listtest/key-1
    ${count}=    Execute and checkrc    echo '${result}' | jq -r '.Uploads | length'    0
    Should Be Equal    ${count}    11    # Should match key-1, key-10 through key-19

    # Cleanup
    FOR    ${index}    IN RANGE    25
        ${key}=    Set Variable    ${PREFIX}/listtest/key-${index}
        ${uploadID}=    Get From List    ${uploadIDs}    ${index}
        Abort MPU    ${BUCKET}    ${key}    ${uploadID}    0
    END

Check Bucket Ownership Verification
    # 1. InitMultipartUpload
    ${correct_owner}=    Get bucket owner    ${BUCKET}
    ${uploadID}=         Execute AWSS3APICli with bucket owner check               create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/mpu/key1    ${correct_owner}
    ${uploadID}=         Execute and checkrc                                       echo '${uploadID}' | jq -r '.UploadId'    0

    # 2. upload-part
    ${ETag1} =  Execute AWSS3APICli with bucket owner check                        upload-part --bucket ${BUCKET} --key ${PREFIX}/mpu/key1 --part-number 1 --body /tmp/part1 --upload-id ${uploadID}  ${correct_owner}
    ${ETag1} =  Execute and checkrc                                                echo '${ETag1}' | jq -r '.ETag'    0


    # 3. upload-part-copy
    ${result}=    Execute AWSS3APICli                                              put-object --bucket ${BUCKET} --key ${PREFIX}/mpu/source --body /tmp/part2
    ${ETag2} =  Execute AWSS3APICli with bucket owner check                        upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/mpu/key1 --upload-id ${uploadID} --part-number 2 --copy-source ${BUCKET}/${PREFIX}/mpu/source  ${correct_owner}  ${correct_owner}
    ${ETag2} =  Execute and checkrc                                                echo '${ETag2}' | jq -r '.CopyPartResult.ETag'    0

    # 4. list-multipart-uploads
    Execute AWSS3APICli with bucket owner check                                    list-multipart-uploads --bucket ${BUCKET}  ${correct_owner}

    # 5. list-parts
    Execute AWSS3APICli with bucket owner check                                    list-parts --bucket ${BUCKET} --key ${PREFIX}/mpu/key1 --upload-id ${uploadID}  ${correct_owner}

    # 6. complete-multipart-upload
    ${parts}=    Set Variable                                                      {ETag=${ETag1},PartNumber=1},{ETag=${ETag2},PartNumber=2}
    Execute AWSS3APICli with bucket owner check                                    complete-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/mpu/key1 --upload-id ${uploadID} --multipart-upload 'Parts=[${parts}]'  ${correct_owner}

    # create another MPU to test abort-multipart-upload
    ${uploadID}=    Execute AWSS3APICli using bucket ownership verification        create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/mpu/aborttest    ${correct_owner}
    ${uploadID}=    Execute and checkrc                                            echo '${uploadID}' | jq -r '.UploadId'    0

    Execute AWSS3APICli with bucket owner check                                    abort-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/mpu/aborttest --upload-id ${uploadID}  ${correct_owner}

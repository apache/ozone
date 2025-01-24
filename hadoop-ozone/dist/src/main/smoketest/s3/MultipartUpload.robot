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
Test Timeout        5 minutes
Suite Setup         Setup Multipart Tests
Test Setup          Generate random prefix

*** Keywords ***
Setup Multipart Tests
    Setup s3 tests

    # 5MB + a bit
    Create Random File KB    /tmp/part1    5121

    # 1MB - a bit
    Create Random File KB    /tmp/part2    1023

Create Random file
    [arguments]             ${size_in_megabytes}
    Execute                 dd if=/dev/urandom of=/tmp/part1 bs=1048576 count=${size_in_megabytes} status=none

Create Random File KB
    [arguments]             ${file}    ${size_in_kilobytes}
    Execute                 dd if=/dev/urandom of=${file} bs=1024 count=${size_in_kilobytes} status=none

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
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey
    ${uploadID} =       Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey
                        Should contain          ${result}    UploadId
# initiate again
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey
    ${nextUploadID} =   Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey
                        Should contain          ${result}    UploadId
                        Should Not Be Equal     ${uploadID}  ${nextUploadID}

# upload part
# each part should be minimum 5mb, other wise during complete multipart
# upload we get error entity too small. So, considering further complete
# multipart upload, uploading each part as 5MB file, exception is for last part

    Run Keyword         Create Random file      5
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey --part-number 1 --body /tmp/part1 --upload-id ${nextUploadID}
                        Should contain          ${result}    ETag
# override part
    Run Keyword         Create Random file      5
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey --part-number 1 --body /tmp/part1 --upload-id ${nextUploadID}
                        Should contain          ${result}    ETag


Test Multipart Upload Complete
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --metadata="custom-key1=custom-value1,custom-key2=custom-value2,gdprEnabled=true"
    ${uploadID} =       Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey
                        Should contain          ${result}    UploadId

#upload parts
    Run Keyword         Create Random file            5
    ${result} =         Execute AWSS3APICli           upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --part-number 1 --body /tmp/part1 --upload-id ${uploadID}
    ${eTag1} =          Execute and checkrc           echo '${result}' | jq -r '.ETag'   0
                        Should contain                ${result}    ETag
    ${part1Md5Sum} =    Execute                       md5sum /tmp/part1 | awk '{print $1}'
                        Should Be Equal As Strings    ${eTag1}  ${part1Md5Sum}

                        Execute                       echo "Part2" > /tmp/part2
    ${result} =         Execute AWSS3APICli           upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --part-number 2 --body /tmp/part2 --upload-id ${uploadID}
    ${eTag2} =          Execute and checkrc           echo '${result}' | jq -r '.ETag'   0
                        Should contain                ${result}    ETag
    ${part2Md5Sum} =    Execute                       md5sum /tmp/part2 | awk '{print $1}'
                        Should Be Equal As Strings    ${eTag2}  ${part2Md5Sum}

#complete multipart upload
    ${result} =                 Execute AWSS3APICli           complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --multipart-upload 'Parts=[{ETag=${eTag1},PartNumber=1},{ETag=${eTag2},PartNumber=2}]'
                                Should contain                ${result}    ${BUCKET}
                                Should contain                ${result}    ${PREFIX}/multipartKey1
    ${resultETag} =             Execute and checkrc           echo '${result}' | jq -r '.ETag'   0
    ${expectedResultETag} =     Execute                       echo -n ${eTag1}${eTag2} | md5sum | awk '{print $1}'
                                Should contain                ${result}    ETag
                                Should Be Equal As Strings    ${resultETag}     "${expectedResultETag}-2"

#check whether the user defined metadata can be retrieved
    ${result} =                 Execute AWSS3ApiCli           head-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey1
                                Should contain                ${result}    \"custom-key1\": \"custom-value1\"
                                Should contain                ${result}    \"custom-key2\": \"custom-value2\"

    ${result} =                 Execute                       ozone sh key info /s3v/${BUCKET}/${PREFIX}/multipartKey1
                                Should contain                ${result}    \"custom-key1\" : \"custom-value1\"
                                Should contain                ${result}    \"custom-key2\" : \"custom-value2\"
                                Should not contain            ${result}    \"gdprEnabled\": \"true\"

#read file and check the key
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 /tmp/${PREFIX}-multipartKey1.result
                                Execute                    cat /tmp/part1 /tmp/part2 > /tmp/${PREFIX}-multipartKey1
    Compare files               /tmp/${PREFIX}-multipartKey1         /tmp/${PREFIX}-multipartKey1.result

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --part-number 1 /tmp/${PREFIX}-multipartKey1-part1.result
    Compare files               /tmp/part1        /tmp/${PREFIX}-multipartKey1-part1.result

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --part-number 2 /tmp/${PREFIX}-multipartKey1-part2.result
    Compare files               /tmp/part2        /tmp/${PREFIX}-multipartKey1-part2.result

Test Multipart Upload with user defined metadata size larger than 2 KB
    ${custom_metadata_value} =  Execute                               printf 'v%.0s' {1..3000}
    ${result} =                 Execute AWSS3APICli and checkrc       create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/mpuWithLargeMetadata --metadata="custom-key1=${custom_metadata_value}"    255
                                Should contain                        ${result}   MetadataTooLarge
                                Should not contain                    ${result}   custom-key1: ${custom_metadata_value}

Test Multipart Upload Complete Entity too small
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey2
    ${uploadID} =       Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey
                        Should contain          ${result}    UploadId

#upload parts
                        Execute                 echo "Part1" > /tmp/part1
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey2 --part-number 1 --body /tmp/part1 --upload-id ${uploadID}
    ${eTag1} =          Execute and checkrc     echo '${result}' | jq -r '.ETag'   0
                        Should contain          ${result}    ETag

                        Execute                 echo "Part2" > /tmp/part2
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey2 --part-number 2 --body /tmp/part2 --upload-id ${uploadID}
    ${eTag2} =          Execute and checkrc     echo '${result}' | jq -r '.ETag'   0
                        Should contain          ${result}    ETag

#complete multipart upload
    ${result} =         Execute AWSS3APICli and checkrc  complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey2 --multipart-upload 'Parts=[{ETag=${eTag1},PartNumber=1},{ETag=${eTag2},PartNumber=2}]'    255
                        Should contain          ${result}    EntityTooSmall


Test Multipart Upload Complete Invalid part errors and complete mpu with few parts
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey3
    ${uploadID} =       Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey
                        Should contain          ${result}    UploadId

#complete multipart upload when no parts uploaded
    ${result} =         Execute AWSS3APICli and checkrc  complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --multipart-upload 'Parts=[{ETag=etag1,PartNumber=1},{ETag=etag2,PartNumber=2}]'    255
                        Should contain          ${result}    InvalidPart
    ${result} =         Execute AWSS3APICli and checkrc  complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --multipart-upload 'Parts=[{ETag=etag1,PartNumber=2},{ETag=etag2,PartNumber=1}]'    255
                        Should contain          ${result}    InvalidPart
#upload parts
                        Run Keyword             Create Random file      5
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --part-number 1 --body /tmp/part1 --upload-id ${uploadID}
    ${eTag1} =          Execute and checkrc     echo '${result}' | jq -r '.ETag'   0
                        Should contain          ${result}    ETag


    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --part-number 2 --body /tmp/part1 --upload-id ${uploadID}
    ${eTag2} =          Execute and checkrc     echo '${result}' | jq -r '.ETag'   0
                        Should contain          ${result}    ETag

                        Execute                 echo "Part3" > /tmp/part3
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --part-number 3 --body /tmp/part3 --upload-id ${uploadID}
    ${eTag3} =          Execute and checkrc     echo '${result}' | jq -r '.ETag'   0
                        Should contain          ${result}    ETag

#complete multipart upload
    ${result} =         Execute AWSS3APICli and checkrc  complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --multipart-upload 'Parts=[{ETag=etag1,PartNumber=1},{ETag=etag2,PartNumber=2}]'    255
                        Should contain          ${result}    InvalidPart
    ${result} =         Execute AWSS3APICli and checkrc  complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --multipart-upload 'Parts=[{ETag=${eTag1},PartNumber=1},{ETag=etag2,PartNumber=2}]'    255
                        Should contain          ${result}    InvalidPart
    ${result} =         Execute AWSS3APICli and checkrc  complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --multipart-upload 'Parts=[{ETag=${eTag1},PartNumber=4},{ETag=etag2,PartNumber=2}]'    255
                        Should contain          ${result}    InvalidPartOrder
#complete multipart upload(merge with few parts)
    ${result} =         Execute AWSS3APICli     complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --multipart-upload 'Parts=[{ETag=${eTag1},PartNumber=1},{ETag=${eTag3},PartNumber=3}]'
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey3
                        Should contain          ${result}    ETag

    ${result} =         Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 /tmp/${PREFIX}-multipartKey3.result
                        Execute                    cat /tmp/part1 /tmp/part3 > /tmp/${PREFIX}-multipartKey3
    Compare files       /tmp/${PREFIX}-multipartKey3         /tmp/${PREFIX}-multipartKey3.result

    ${result} =         Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --part-number 1 /tmp/${PREFIX}-multipartKey3-part1.result
    Compare files       /tmp/part1         /tmp/${PREFIX}-multipartKey3-part1.result

    ${result} =         Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey3 --part-number 3 /tmp/${PREFIX}-multipartKey3-part3.result
    Compare files       /tmp/part3         /tmp/${PREFIX}-multipartKey3-part3.result

Test abort Multipart upload
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey4 --storage-class REDUCED_REDUNDANCY
    ${uploadID} =       Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey
                        Should contain          ${result}    UploadId

    ${result} =         Execute AWSS3APICli and checkrc    abort-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey4 --upload-id ${uploadID}    0

Test abort Multipart upload with invalid uploadId
    ${result} =         Execute AWSS3APICli and checkrc    abort-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey5 --upload-id "random"    255

Upload part with Incorrect uploadID
        ${result} =     Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey
                        Execute                 echo "Multipart upload" > /tmp/testfile
        ${result} =     Execute AWSS3APICli and checkrc     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey --part-number 1 --body /tmp/testfile --upload-id "random"  255
                        Should contain          ${result}    NoSuchUpload

Test list parts
#initiate multipart upload
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey5
    ${uploadID} =       Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey
                        Should contain          ${result}    UploadId

#upload parts
    Run Keyword         Create Random file      5
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey5 --part-number 1 --body /tmp/part1 --upload-id ${uploadID}
    ${eTag1} =          Execute and checkrc     echo '${result}' | jq -r '.ETag'   0
                        Should contain          ${result}    ETag

                        Execute                 echo "Part2" > /tmp/part2
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey5 --part-number 2 --body /tmp/part2 --upload-id ${uploadID}
    ${eTag2} =          Execute and checkrc     echo '${result}' | jq -r '.ETag'   0
                        Should contain          ${result}    ETag

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
    ${result} =         Execute AWSS3APICli and checkrc    abort-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey5 --upload-id ${uploadID}    0

Test Multipart Upload with the simplified aws s3 cp API
                        Create Random file      22
                        Execute AWSS3Cli        cp /tmp/part1 s3://${BUCKET}/mpyawscli
                        Execute AWSS3Cli        cp s3://${BUCKET}/mpyawscli /tmp/part1.result
                        Execute AWSS3Cli        rm s3://${BUCKET}/mpyawscli
                        Compare files           /tmp/part1        /tmp/part1.result

Test Multipart Upload Put With Copy
    Run Keyword         Create Random file      5
    ${result} =         Execute AWSS3APICli     put-object --bucket ${BUCKET} --key ${PREFIX}/copytest/source --body /tmp/part1


    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/copytest/destination

    ${uploadID} =       Execute and checkrc      echo '${result}' | jq -r '.UploadId'    0
                        Should contain           ${result}    ${BUCKET}
                        Should contain           ${result}    UploadId

    ${result} =         Execute AWSS3APICli      upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copytest/destination --upload-id ${uploadID} --part-number 1 --copy-source ${BUCKET}/${PREFIX}/copytest/source
                        Should contain           ${result}    ETag
                        Should contain           ${result}    LastModified
    ${eTag1} =          Execute and checkrc      echo '${result}' | jq -r '.CopyPartResult.ETag'   0


                        Execute AWSS3APICli     complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/copytest/destination --multipart-upload 'Parts=[{ETag=${eTag1},PartNumber=1}]'
                        Execute AWSS3APICli     get-object --bucket ${BUCKET} --key ${PREFIX}/copytest/destination /tmp/part-result

                        Compare files           /tmp/part1        /tmp/part-result

Test Multipart Upload Put With Copy and range
    Run Keyword         Create Random file      10
    ${result} =         Execute AWSS3APICli     put-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/source --body /tmp/part1


    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination

    ${uploadID} =       Execute and checkrc      echo '${result}' | jq -r '.UploadId'    0
                        Should contain           ${result}    ${BUCKET}
                        Should contain           ${result}    UploadId

    ${result} =         Execute AWSS3APICli      upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --upload-id ${uploadID} --part-number 1 --copy-source ${BUCKET}/${PREFIX}/copyrange/source --copy-source-range bytes=0-10485757
                        Should contain           ${result}    ETag
                        Should contain           ${result}    LastModified
    ${eTag1} =          Execute and checkrc      echo '${result}' | jq -r '.CopyPartResult.ETag'   0

    ${result} =         Execute AWSS3APICli      upload-part-copy --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --upload-id ${uploadID} --part-number 2 --copy-source ${BUCKET}/${PREFIX}/copyrange/source --copy-source-range bytes=10485758-10485759
                        Should contain           ${result}    ETag
                        Should contain           ${result}    LastModified
    ${eTag2} =          Execute and checkrc      echo '${result}' | jq -r '.CopyPartResult.ETag'   0


                        Execute AWSS3APICli     complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --multipart-upload 'Parts=[{ETag=${eTag1},PartNumber=1},{ETag=${eTag2},PartNumber=2}]'
                        Execute AWSS3APICli     get-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination /tmp/part-result

                        Compare files           /tmp/part1        /tmp/part-result

Test Multipart Upload Put With Copy and range with IfModifiedSince
    Run Keyword         Create Random file      10
    ${curDate} =        Get Current Date
    ${beforeCreate} =   Subtract Time From Date     ${curDate}  1 day
    ${tomorrow} =       Add Time To Date            ${curDate}  1 day

    ${result} =         Execute AWSS3APICli     put-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/source --body /tmp/part1

    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination

    ${uploadID} =       Execute and checkrc      echo '${result}' | jq -r '.UploadId'    0
                        Should contain           ${result}    ${BUCKET}
                        Should contain           ${result}    UploadId

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


                        Execute AWSS3APICli     complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination --multipart-upload 'Parts=[{ETag=${eTag1},PartNumber=1},{ETag=${eTag2},PartNumber=2}]'
                        Execute AWSS3APICli     get-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination /tmp/part-result

                        Compare files           /tmp/part1        /tmp/part-result

Test Multipart Upload list
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/listtest/key1
    ${uploadID1} =      Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/listtest/key1
                        Should contain          ${result}    UploadId

    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/listtest/key2
    ${uploadID2} =      Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/listtest/key2
                        Should contain          ${result}    UploadId

    ${result} =         Execute AWSS3APICli     list-multipart-uploads --bucket ${BUCKET} --prefix ${PREFIX}/listtest
                        Should contain          ${result}    ${uploadID1}
                        Should contain          ${result}    ${uploadID2}

    ${count} =          Execute and checkrc      echo '${result}' | jq -r '.Uploads | length'  0
                        Should Be Equal          ${count}     2

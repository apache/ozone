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
Suite Setup         Setup s3 tests
Test Setup          Generate random prefix

*** Keywords ***
Create Random file
    [arguments]             ${size_in_megabytes}
    Execute                 dd if=/dev/urandom of=/tmp/part1 bs=1048576 count=${size_in_megabytes}

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

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
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/multipartKey1
    ${uploadID} =       Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey
                        Should contain          ${result}    UploadId

#upload parts
    Run Keyword         Create Random file      5
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --part-number 1 --body /tmp/part1 --upload-id ${uploadID}
    ${eTag1} =          Execute and checkrc     echo '${result}' | jq -r '.ETag'   0
                        Should contain          ${result}    ETag

                        Execute                 echo "Part2" > /tmp/part2
    ${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --part-number 2 --body /tmp/part2 --upload-id ${uploadID}
    ${eTag2} =          Execute and checkrc     echo '${result}' | jq -r '.ETag'   0
                        Should contain          ${result}    ETag

#complete multipart upload
    ${result} =         Execute AWSS3APICli     complete-multipart-upload --upload-id ${uploadID} --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 --multipart-upload 'Parts=[{ETag=${eTag1},PartNumber=1},{ETag=${eTag2},PartNumber=2}]'
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    ${PREFIX}/multipartKey1
                        Should contain          ${result}    ETag

#read file and check the key
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${BUCKET} --key ${PREFIX}/multipartKey1 /tmp/${PREFIX}-multipartKey1.result
                                Execute                    cat /tmp/part1 /tmp/part2 > /tmp/${PREFIX}-multipartKey1
    Compare files               /tmp/${PREFIX}-multipartKey1         /tmp/${PREFIX}-multipartKey1.result

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
    ${afterCreate} =    Add Time To Date        ${curDate}  1 day

    ${result} =         Execute AWSS3APICli     put-object --bucket ${BUCKET} --key ${PREFIX}/copyrange/source --body /tmp/part1

    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key ${PREFIX}/copyrange/destination

    ${uploadID} =       Execute and checkrc      echo '${result}' | jq -r '.UploadId'    0
                        Should contain           ${result}    ${BUCKET}
                        Should contain           ${result}    UploadId

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

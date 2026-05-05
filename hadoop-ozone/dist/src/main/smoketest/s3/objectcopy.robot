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
${BUCKET}             generated
${DESTBUCKET}         generated1

*** Keywords ***
Create Dest Bucket
    ${postfix} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable   ${DESTBUCKET}             destbucket-${postfix}
    Execute AWSS3APICli  create-bucket --bucket ${DESTBUCKET}

*** Test Cases ***
Copy Object Happy Scenario
    Run Keyword if    '${DESTBUCKET}' == 'generated1'    Create Dest Bucket
                        Execute                    date > /tmp/copyfile
    ${file_checksum} =  Execute                    md5sum /tmp/copyfile | awk '{print $1}'

    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/copyobject/key=value/f1 --body /tmp/copyfile --metadata="custom-key1=custom-value1,custom-key2=custom-value2,gdprEnabled=true" --tagging="tag-key1=tag-value1&tag-key2=tag-value2"
    ${eTag} =           Execute and checkrc        echo '${result}' | jq -r '.ETag'  0
                        Should Be Equal            ${eTag}           \"${file_checksum}\"

    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/copyobject/key=value/
                        Should contain             ${result}         f1

    ${result} =         Execute AWSS3ApiCli        copy-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 --copy-source ${BUCKET}/${PREFIX}/copyobject/key=value/f1 --metadata="custom-key3=custom-value3,custom-key4=custom-value4" --tagging="tag-key3=tag-value3"
    ${eTag} =           Execute and checkrc        echo '${result}' | jq -r '.CopyObjectResult.ETag'  0
                        Should Be Equal            ${eTag}           \"${file_checksum}\"

    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${DESTBUCKET} --prefix ${PREFIX}/copyobject/key=value/
                        Should contain             ${result}         f1

    #check that the custom metadata of the source key has been copied to the destination key (default copy directive is COPY)
    ${result} =         Execute AWSS3ApiCli        head-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1
                        Should contain             ${result}    \"custom-key1\": \"custom-value1\"
                        Should contain             ${result}    \"custom-key2\": \"custom-value2\"
                        # COPY directive ignores any metadata specified in the copy object request
                        Should Not contain         ${result}    \"custom-key3\": \"custom-value3\"
                        Should Not contain         ${result}    \"custom-key4\": \"custom-value4\"

    #check that the tagging count is accurate
    ${result} =         Execute AWSS3APICli       get-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 /tmp/testfile2.result
                        Should contain            ${result}   TagCount
    ${tagCount} =       Execute and checkrc       echo '${result}' | jq -r '.TagCount'    0
                        Should Be Equal           ${tagCount}    2

    #copying again will not throw error
    #also uses the REPLACE copy directive
    ${result} =         Execute AWSS3ApiCli        copy-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 --copy-source ${BUCKET}/${PREFIX}/copyobject/key=value/f1 --metadata="custom-key3=custom-value3,custom-key4=custom-value4" --metadata-directive REPLACE --tagging="tag-key3=tag-value3" --tagging-directive REPLACE
    ${eTag} =           Execute and checkrc        echo '${result}' | jq -r '.CopyObjectResult.ETag'  0
                        Should Be Equal            ${eTag}           \"${file_checksum}\"

    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${DESTBUCKET} --prefix ${PREFIX}/copyobject/key=value/
                        Should contain             ${result}         f1
    ${result} =         Execute AWSS3ApiCli        head-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1
                        Should contain             ${result}    \"custom-key3\": \"custom-value3\"
                        Should contain             ${result}    \"custom-key4\": \"custom-value4\"
                        # REPLACE directive uses the custom metadata specified in the request instead of the source key's custom metadata
                        Should Not contain         ${result}    \"custom-key1\": \"custom-value1\"
                        Should Not contain         ${result}    \"custom-key2\": \"custom-value2\"
    ${result} =         Execute AWSS3APICli        get-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 /tmp/testfile2.result
                        Should contain             ${result}     TagCount
                        # REPLACE directive uses the tagging header specified in the request instead of the source key's tags
    ${tagCount} =       Execute and checkrc        echo '${result}' | jq -r '.TagCount'    0
                        Should Be Equal            ${tagCount}    1

Copy Object Where Bucket is not available
    ${result} =         Execute AWSS3APICli and checkrc        copy-object --bucket dfdfdfdfdfnonexistent --key ${PREFIX}/copyobject/key=value/f1 --copy-source ${BUCKET}/${PREFIX}/copyobject/key=value/f1      255
                        Should contain             ${result}        NoSuchBucket
    ${result} =         Execute AWSS3APICli and checkrc        copy-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 --copy-source dfdfdfdfdfnonexistent/${PREFIX}/copyobject/key=value/f1  255
                        Should contain             ${result}        NoSuchBucket

Copy Object Where both source and dest are same with change to storageclass
     ${file_checksum} =  Execute                    md5sum /tmp/copyfile | awk '{print $1}'
     ${result} =         Execute AWSS3APICli        copy-object --storage-class REDUCED_REDUNDANCY --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 --copy-source ${DESTBUCKET}/${PREFIX}/copyobject/key=value/f1
                         Should contain             ${result}        ETag
     ${eTag} =           Execute and checkrc        echo '${result}' | jq -r '.CopyObjectResult.ETag'  0
                         Should Be Equal            ${eTag}           \"${file_checksum}\"

Copy Object Where Key not available
    ${result} =         Execute AWSS3APICli and checkrc        copy-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 --copy-source ${BUCKET}/nonnonexistentkey       255
                        Should contain             ${result}        NoSuchKey

Copy Object using an invalid copy directive
    ${result} =         Execute AWSS3ApiCli and checkrc        copy-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 --copy-source ${BUCKET}/${PREFIX}/copyobject/key=value/f1 --metadata-directive INVALID       255
                        Should contain             ${result}        InvalidArgument
    ${result} =         Execute AWSS3ApiCli and checkrc        copy-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 --copy-source ${BUCKET}/${PREFIX}/copyobject/key=value/f1 --tagging-directive INVALID        255
                        Should contain             ${result}        InvalidArgument

Copy Object with user defined metadata size larger than 2 KB
                                Execute                    echo "Randomtext" > /tmp/testfile2
    ${custom_metadata_value} =  Generate Random String    3000
    ${result} =                 Execute AWSS3ApiCli and checkrc       copy-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyobject/key=value/f1 --copy-source ${BUCKET}/${PREFIX}/copyobject/key=value/f1 --metadata="custom-key1=${custom_metadata_value}" --metadata-directive REPLACE       255
                                Should contain                        ${result}   MetadataTooLarge

Check Bucket Ownership Verification
    Run Keyword if      '${DESTBUCKET}' == 'generated1'    Create Dest Bucket
    Execute              echo "Randomtext" > /tmp/testfile
    ${correct_dest_owner} =     Get bucket owner    ${DESTBUCKET}
    ${correct_source_owner} =   Get bucket owner    ${BUCKET}

    Execute AWSS3ApiCli                             put-object --bucket ${BUCKET} --key ${PREFIX}/copyowner/key=value/f1 --body /tmp/testfile
    Execute AWSS3APICli with bucket owner check     copy-object --bucket ${DESTBUCKET} --key ${PREFIX}/copyowner/key=value/f1 --copy-source ${BUCKET}/${PREFIX}/copyowner/key=value/f1  ${correct_dest_owner}  ${correct_source_owner}

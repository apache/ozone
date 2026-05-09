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
Documentation       S3 bucket CORS configuration test with aws cli
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests
Test Tags           no-bucket-type    bucket-cors

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated
${CORS_FILE}          /tmp/ozone-bucket-cors.json

*** Test Cases ***

Put get and delete bucket CORS configuration
    ${cors} =       Catenate    SEPARATOR=
    ...             {"CORSRules":[{"ID":"robot-write-cors",
    ...             "AllowedOrigins":["http://www.example.com"],
    ...             "AllowedMethods":["PUT","POST","DELETE"],
    ...             "AllowedHeaders":["*"],
    ...             "ExposeHeaders":["x-amz-server-side-encryption"],
    ...             "MaxAgeSeconds":3000},
    ...             {"ID":"robot-read-cors",
    ...             "AllowedOrigins":["*"],
    ...             "AllowedMethods":["GET","HEAD"]}]}
                    Create File    ${CORS_FILE}    ${cors}

    ${result} =     Execute AWSS3APICli    put-bucket-cors --bucket ${BUCKET} --cors-configuration file://${CORS_FILE}
    ${result} =     Execute AWSS3APICli    get-bucket-cors --bucket ${BUCKET}
                    Should Contain    ${result}    robot-write-cors
                    Should Contain    ${result}    robot-read-cors
                    Should Contain    ${result}    http://www.example.com
                    Should Contain    ${result}    x-amz-server-side-encryption

    ${preflight} =  Execute    curl --silent --show-error --include -X OPTIONS -H 'Origin: http://www.example.com' -H 'Access-Control-Request-Method: PUT' -H 'Access-Control-Request-Headers: x-amz-meta-test' ${ENDPOINT_URL}/${BUCKET}/${PREFIX}/cors-key
                    Should Contain    ${preflight}    HTTP/1.1 200
                    Should Contain    ${preflight}    Access-Control-Allow-Origin: http://www.example.com
                    Should Contain    ${preflight}    Access-Control-Allow-Methods: PUT, POST, DELETE
                    Should Contain    ${preflight}    Access-Control-Allow-Headers: x-amz-meta-test
                    Should Contain    ${preflight}    Access-Control-Max-Age: 3000
                    Should Contain    ${preflight}    Access-Control-Expose-Headers: x-amz-server-side-encryption

    ${preflight} =  Execute    curl --silent --show-error --include -X OPTIONS -H 'Origin: https://other.example.com' -H 'Access-Control-Request-Method: GET' ${ENDPOINT_URL}/${BUCKET}/${PREFIX}/cors-key
                    Should Contain    ${preflight}    HTTP/1.1 200
                    Should Contain    ${preflight}    Access-Control-Allow-Origin: https://other.example.com
                    Should Contain    ${preflight}    Access-Control-Allow-Methods: GET, HEAD

    ${status} =     Execute    curl --silent --show-error --output /dev/null --write-out '\%{http_code}' -X OPTIONS -H 'Origin: https://other.example.com' -H 'Access-Control-Request-Method: DELETE' ${ENDPOINT_URL}/${BUCKET}/${PREFIX}/cors-key
                    Should Be Equal    ${status}    403

    ${result} =     Execute AWSS3APICli    delete-bucket-cors --bucket ${BUCKET}
    ${result} =     Execute AWSS3APICli and checkrc    get-bucket-cors --bucket ${BUCKET}    255
                    Should Contain    ${result}    NoSuchCORSConfiguration

    [Teardown]      Remove File    ${CORS_FILE}

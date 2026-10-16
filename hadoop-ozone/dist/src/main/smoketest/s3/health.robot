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
Documentation       S3 gateway health endpoints test
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Test Timeout        5 minutes
Default Tags        no-bucket-type

*** Variables ***

${S3G_HEALTH}       http://s3g:19878

*** Keywords ***
# The health endpoints are unauthenticated, so a plain curl (no SPNEGO) must
# succeed even when security is enabled.
Readiness reports ready
    ${result} =         Execute                             curl -sS -i ${S3G_HEALTH}/health/ready
                        Should contain      ${result}       200
                        Should contain      ${result}       READY

*** Test Cases ***
Liveness endpoint returns OK
    ${result} =         Execute                             curl -sS -i ${S3G_HEALTH}/health/live
                        Should contain      ${result}       200
                        Should contain      ${result}       OK

Readiness endpoint reports ready when OM is reachable
    Wait Until Keyword Succeeds     30sec       5sec        Readiness reports ready

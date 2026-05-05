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
Documentation       Smoketest ozone cluster startup
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../ozone-lib/freon.robot
Test Timeout        5 minutes
Suite Setup         Get Security Enabled From Config

*** Variables ***
${user}              hadoop
${buckets}           5
${auditworkdir}      /tmp

*** Keywords ***
Set username
    ${principal} =     Get test user principal    testuser
    Set Suite Variable    ${user}    ${principal}
    [Return]      ${principal}

Create data
    Freon OMBG    prefix=auditparser    n=${buckets}
    Freon OCKG    prefix=auditparser    n=100

*** Test Cases ***
Testing audit parser
    [Setup]            Create data

    ${logdir} =        Get Environment Variable      OZONE_LOG_DIR     /var/log/ozone
    ${logfile} =       Execute              ls -t "${logdir}" | grep om-audit | head -1
                       Execute              ozone debug auditparser "${auditworkdir}/audit.db" load "${logdir}/${logfile}"
    ${result} =        Execute              ozone debug auditparser "${auditworkdir}/audit.db" template top5cmds
                       Should Contain       ${result}  ALLOCATE_KEY
    ${result} =        Execute              ozone debug auditparser "${auditworkdir}/audit.db" template top5users
    Run Keyword If     '${SECURITY_ENABLED}' == 'true'      Set username
                       Should Contain       ${result}  ${user}
    ${result} =        Execute              ozone debug auditparser "${auditworkdir}/audit.db" query "select count(*) from audit where op='CREATE_VOLUME' and RESULT='SUCCESS'"
    ${result} =        Convert To Number     ${result}
                       Should be true       ${result}>=1
    ${result} =        Execute              ozone debug auditparser "${auditworkdir}/audit.db" query "select count(*) from audit where op='CREATE_BUCKET' and RESULT='SUCCESS'"
    ${result} =        Convert To Number     ${result}
                       Should be true       ${result}>=${buckets}

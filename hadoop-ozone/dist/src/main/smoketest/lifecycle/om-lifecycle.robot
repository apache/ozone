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
Documentation       Ozone admin om lifecycle commands
Library             OperatingSystem
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${OM_SERVICE_ID}    %{OM_SERVICE_ID}

*** Test Cases ***
Test Lifecycle Status
    ${output} =         Execute             ozone admin om lifecycle status --service-id '${OM_SERVICE_ID}'
                        Should Contain      ${output}    IsEnabled
                        Should Contain      ${output}    IsSuspended

Test Lifecycle Suspend And Resume
    ${output} =         Execute             ozone admin om lifecycle suspend --service-id '${OM_SERVICE_ID}'
                        Should Contain      ${output}    Lifecycle Service has been suspended

    ${output} =         Execute             ozone admin om lifecycle status --service-id '${OM_SERVICE_ID}'
                        Should Contain      ${output}    IsSuspended: true

    ${output} =         Execute             ozone admin om lifecycle resume --service-id '${OM_SERVICE_ID}'
                        Should Contain      ${output}    Lifecycle Service has been resumed

    ${output} =         Execute             ozone admin om lifecycle status --service-id '${OM_SERVICE_ID}'
                        Should Contain      ${output}    IsSuspended: false

Test Lifecycle Status After Leader Transfer
    ${output} =         Execute             ozone admin om roles --service-id '${OM_SERVICE_ID}'
    ${is_ha} =          Run Keyword And Return Status    Should Contain    ${output}    FOLLOWER
    IF    ${is_ha}
        ${output} =         Execute             ozone admin om lifecycle suspend --service-id '${OM_SERVICE_ID}'
                            Should Contain      ${output}    Lifecycle Service has been suspended

        ${output} =         Execute             ozone admin om transfer --service-id '${OM_SERVICE_ID}' -r
                            Should Contain      ${output}    Transfer leadership successfully

        ${output} =         Execute             ozone admin om lifecycle status --service-id '${OM_SERVICE_ID}'
                            Should Contain      ${output}    IsSuspended: true

        ${output} =         Execute             ozone admin om lifecycle resume --service-id '${OM_SERVICE_ID}'
                            Should Contain      ${output}    Lifecycle Service has been resumed

        ${output} =         Execute             ozone admin om lifecycle status --service-id '${OM_SERVICE_ID}'
                            Should Contain      ${output}    IsSuspended: false
    ELSE
        Pass Execution      Cluster is not HA, skipping leader transfer test
    END

Test Lifecycle Suspend And Resume Requires Admin
    # This test verifies that suspend and resume commands require admin privileges
    # while the status command does not.
    # The Requires admin privilege keyword automatically switches to testuser2 via kinit
    # in secure environments and verifies access is denied.
    
    Get Security Enabled From Config
    IF    '${SECURITY_ENABLED}' == 'true'
        # First switch to non-admin user
        Kinit test user     testuser2     testuser2.keytab
        
        # Status should work for non-admin
        ${output} =         Execute and checkrc    ozone admin om lifecycle status --service-id '${OM_SERVICE_ID}'     0
                            Should Contain      ${output}    IsEnabled
                            
        # Suspend should fail for non-admin
        Access should be denied    ozone admin om lifecycle suspend --service-id '${OM_SERVICE_ID}'
        
        # Resume should fail for non-admin
        Access should be denied    ozone admin om lifecycle resume --service-id '${OM_SERVICE_ID}'
        
        # Switch back to admin user for subsequent tests
        Kinit test user     testuser     testuser.keytab
        
        # Verify admin can suspend and resume
        ${output} =         Execute and checkrc    ozone admin om lifecycle suspend --service-id '${OM_SERVICE_ID}'    0
                            Should Contain      ${output}    Lifecycle Service has been suspended
                            
        ${output} =         Execute and checkrc    ozone admin om lifecycle resume --service-id '${OM_SERVICE_ID}'     0
                            Should Contain      ${output}    Lifecycle Service has been resumed
    ELSE
        # In non-secure environments, we can test by passing a different user via HADOOP_USER_NAME
        
        # Status should work for non-admin
        ${output} =         Execute and checkrc    env HADOOP_USER_NAME=testuser2 ozone admin om lifecycle status --service-id '${OM_SERVICE_ID}'    0
                            Should Contain      ${output}    IsEnabled
                            
        # Suspend should fail for non-admin
        ${output} =         Execute and checkrc    env HADOOP_USER_NAME=testuser2 ozone admin om lifecycle suspend --service-id '${OM_SERVICE_ID}'   255
                            Should Contain      ${output}    Access denied
                            Should Contain      ${output}    Superuser privilege is required
                            
        # Resume should fail for non-admin
        ${output} =         Execute and checkrc    env HADOOP_USER_NAME=testuser2 ozone admin om lifecycle resume --service-id '${OM_SERVICE_ID}'    255
                            Should Contain      ${output}    Access denied
                            Should Contain      ${output}    Superuser privilege is required
                            
        # Verify admin (default user) can suspend and resume
        ${output} =         Execute and checkrc    ozone admin om lifecycle suspend --service-id '${OM_SERVICE_ID}'    0
                            Should Contain      ${output}    Lifecycle Service has been suspended
                            
        ${output} =         Execute and checkrc    ozone admin om lifecycle resume --service-id '${OM_SERVICE_ID}'     0
                            Should Contain      ${output}    Lifecycle Service has been resumed
    END

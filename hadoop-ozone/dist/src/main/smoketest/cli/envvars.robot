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
Documentation       Test ozone envvars command
Library             BuiltIn
Library             OperatingSystem
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Save Environment
Suite Teardown      Restore Environment

*** Variables ***
${OZONE_HOME}    /opt/hadoop

*** Keywords ***
Save Environment
    ${saved} =    Get Environment Variables
    Set Suite Variable    ${SAVED_ENV}    ${saved}

Restore Environment
    FOR    ${key}    IN
    ...    HADOOP_HOME    HADOOP_CONF_DIR    HADOOP_LIBEXEC_DIR
    ...    OZONE_HOME    OZONE_CONF_DIR    OZONE_LIBEXEC_DIR    OZONE_DEPRECATION_WARNING
        IF    '${key}' in ${SAVED_ENV}
            Set Environment Variable    ${key}    ${SAVED_ENV}[${key}]
        ELSE
            Run Keyword And Ignore Error    Remove Environment Variable    ${key}
        END
    END

*** Test Cases ***
Ignores deprecated vars if new ones are set
    Set Environment Variable       HADOOP_HOME       /usr/local/hadoop
    Set Environment Variable       OZONE_HOME        ${OZONE_HOME}
    Set Environment Variable       HADOOP_CONF_DIR   /etc/hadoop
    Set Environment Variable       OZONE_CONF_DIR    ${OZONE_HOME}/etc/hadoop
    ${output} =         Execute          ozone envvars
                        Should Contain   ${output}   OZONE_HOME='${OZONE_HOME}'
                        Should Contain   ${output}   OZONE_CONF_DIR='${OZONE_HOME}/etc/hadoop'
                        Should Contain   ${output}   OZONE_LIBEXEC_DIR='${OZONE_HOME}/libexec'
                        Should Contain   ${output}   HDDS_LIB_JARS_DIR='${OZONE_HOME}/share/ozone/lib'
                        Should Not Contain   ${output}   WARNING: HADOOP_HOME
                        Should Not Contain   ${output}   WARNING: HADOOP_CONF_DIR

Find valid dirs
    Set Environment Variable       HADOOP_HOME   /usr/local/hadoop
    Remove Environment Variable    HADOOP_CONF_DIR
    Remove Environment Variable    HADOOP_LIBEXEC_DIR
    Remove Environment Variable    OZONE_HOME
    Remove Environment Variable    OZONE_CONF_DIR
    Remove Environment Variable    OZONE_LIBEXEC_DIR
    ${output} =         Execute          ozone envvars
                        Should Contain   ${output}   OZONE_HOME='/opt/hadoop'
                        Should Contain   ${output}   HDDS_LIB_JARS_DIR='${OZONE_HOME}/share/ozone/lib'
                        Should Contain   ${output}   OZONE_CONF_DIR='/opt/hadoop/etc/hadoop'
                        Should Contain   ${output}   OZONE_LIBEXEC_DIR='/opt/hadoop/libexec'
                        Should Contain   ${output}   WARNING: HADOOP_HOME
                        Should Not Contain   ${output}   WARNING: OZONE_CONF_DIR
                        Should Not Contain   ${output}   WARNING: HADOOP_CONF_DIR

Picks up deprecated vars if valid
    Set Environment Variable       HADOOP_HOME           /opt/hadoop
    Set Environment Variable       HADOOP_LIBEXEC_DIR    %{HADOOP_HOME}/libexec
    Set Environment Variable       HADOOP_CONF_DIR       /etc/hadoop
    Remove Environment Variable    OZONE_HOME
    Remove Environment Variable    OZONE_CONF_DIR
    ${output} =         Execute          ozone envvars
                        Should contain   ${output}   OZONE_HOME='%{HADOOP_HOME}'
                        Should contain   ${output}   HDDS_LIB_JARS_DIR='%{HADOOP_HOME}/share/ozone/lib'
                        Should contain   ${output}   OZONE_CONF_DIR='/etc/hadoop'
                        Should Contain   ${output}   OZONE_LIBEXEC_DIR='%{HADOOP_HOME}/libexec'
                        Should contain   ${output}   WARNING: HADOOP_HOME
                        Should contain   ${output}   WARNING: HADOOP_CONF_DIR

Warning for deprecated vars can be suppressed
    Set Environment Variable       OZONE_DEPRECATION_WARNING    false
    Set Environment Variable       HADOOP_HOME           /opt/hadoop
    Set Environment Variable       HADOOP_LIBEXEC_DIR    %{HADOOP_HOME}/libexec
    Set Environment Variable       HADOOP_CONF_DIR       /etc/hadoop
    Remove Environment Variable    OZONE_HOME
    Remove Environment Variable    OZONE_CONF_DIR
    ${output} =         Execute          ozone envvars
                        Should contain   ${output}   OZONE_HOME='%{HADOOP_HOME}'
                        Should contain   ${output}   HDDS_LIB_JARS_DIR='%{HADOOP_HOME}/share/ozone/lib'
                        Should contain   ${output}   OZONE_CONF_DIR='/etc/hadoop'
                        Should Contain   ${output}   OZONE_LIBEXEC_DIR='%{HADOOP_HOME}/libexec'
                        Should Not Contain   ${output}   WARNING: HADOOP_HOME
                        Should Not Contain   ${output}   WARNING: HADOOP_CONF_DIR

Works with only OZONE_HOME defined
    Remove Environment Variable    HADOOP_HOME
    Remove Environment Variable    HADOOP_CONF_DIR
    Set Environment Variable       OZONE_HOME    ${OZONE_HOME}
    ${output} =         Execute          ozone envvars
                        Should contain   ${output}   OZONE_HOME='${OZONE_HOME}'
                        Should contain   ${output}   HDDS_LIB_JARS_DIR='${OZONE_HOME}/share/ozone/lib'
                        Should contain   ${output}   OZONE_CONF_DIR='${OZONE_HOME}/etc/hadoop'
                        Should Not Contain   ${output}   WARNING: HADOOP_HOME
                        Should Not Contain   ${output}   WARNING: HADOOP_CONF_DIR

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
Library    ../lib/BytemanLibrary.py
Library    String


*** Variables ***
@{ALL_COMPONENTS}         datanode1    datanode2    datanode3    om1    om2    om3    recon    scm1    scm2    scm3    s3g
@{DATANODE_COMPONENTS}    datanode1    datanode2    datanode3
@{OM_COMPONENTS}          om1    om2    om3
@{SCM_COMPONENTS}         scm1    scm2    scm3

*** Keywords ***
Inject Fault Into All Components
    [Arguments]    ${rule_file}
    Log    Injecting fault ${rule_file} into all components
    FOR    ${component}    IN    @{ALL_COMPONENTS}
        Run Keyword And Continue On Failure    Add Byteman Rule    ${component}    ${rule_file}
    END


Remove Fault From All Components
    [Arguments]    ${rule_file}
    Log    Removing fault ${rule_file} from all components
    FOR    ${component}    IN    @{ALL_COMPONENTS}
        Run Keyword And Continue On Failure    Remove Byteman Rule    ${component}    ${rule_file}
    END


List Byteman Rules for All Components
    Log    Listing active rules for all components
    FOR    ${component}    IN    @{ALL_COMPONENTS}
        Run Keyword And Continue On Failure    List Byteman Rules    ${component}
    END


Remove All Rules From All Components
    Log    Removing all rules from all components
    FOR    ${component}    IN    @{ALL_COMPONENTS}
        Run Keyword And Continue On Failure    Remove All Byteman Rules    ${component}
    END


Inject Fault Into Datanodes Only
    [Arguments]    ${rule_file}
    Log    Injecting fault ${rule_file} into datanodes only
    FOR    ${component}    IN    @{DATANODE_COMPONENTS}
        Run Keyword And Continue On Failure    Add Byteman Rule    ${component}    ${rule_file}
    END

List Byteman Rules for Datanodes
    Log    Listing active rules for all datanodes
    FOR    ${component}    IN    @{DATANODE_COMPONENTS}
        Run Keyword And Continue On Failure    List Byteman Rules    ${component}
    END

Remove Fault From Datanodes Only
    [Arguments]    ${rule_file}
    Log    Removing fault ${rule_file} from datanodes only
    FOR    ${component}    IN    @{DATANODE_COMPONENTS}
        Run Keyword And Continue On Failure    Remove Byteman Rule    ${component}    ${rule_file}
    END


Inject Fault Into OMs Only
    [Arguments]    ${rule_file}
    Log    Injecting fault ${rule_file} into oms only
    FOR    ${component}    IN    @{OM_COMPONENTS}
        Run Keyword And Continue On Failure    Add Byteman Rule    ${component}    ${rule_file}
    END

List Byteman Rules for OMs
    Log    Listing active rules for all OMs
    FOR    ${component}    IN    @{OM_COMPONENTS}
        Run Keyword And Continue On Failure    List Byteman Rules    ${component}
    END

Remove Fault From OMs Only
    [Arguments]    ${rule_file}
    Log    Removing fault ${rule_file} from oms only
    FOR    ${component}    IN    @{OM_COMPONENTS}
        Run Keyword And Continue On Failure    Remove Byteman Rule    ${component}    ${rule_file}
    END


Inject Fault Into SCMs Only
    [Arguments]    ${rule_file}
    Log    Injecting fault ${rule_file} into scms only
    FOR    ${component}    IN    @{SCM_COMPONENTS}
        Run Keyword And Continue On Failure    Add Byteman Rule    ${component}    ${rule_file}
    END

List Byteman Rules for SCMs
    Log    Listing active rules for all SCMs
    FOR    ${component}    IN    @{SCM_COMPONENTS}
        Run Keyword And Continue On Failure    List Byteman Rules    ${component}
    END

Remove Fault From SCMs Only
    [Arguments]    ${rule_file}
    Log    Removing fault ${rule_file} from scms only
    FOR    ${component}    IN    @{SCM_COMPONENTS}
        Run Keyword And Continue On Failure    Remove Byteman Rule    ${component}    ${rule_file}
    END

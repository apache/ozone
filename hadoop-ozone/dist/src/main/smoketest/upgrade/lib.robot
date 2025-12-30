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
Documentation       Keywords for Upgrade Tests
Library             OperatingSystem
Resource            ../lib/os.robot
Resource            ../admincli/lib.resource

*** Keywords ***
OM Finalization Status
    ${param} =     Get OM Service Param
    ${result} =    Execute      ozone admin om finalizationstatus ${param}
    Log       ${result}
    RETURN    ${result}


Finalize OM
    ${param} =     Get OM Service Param
    ${result} =    Execute      ozone admin om finalizeupgrade ${param}
    Log       ${result}
    RETURN    ${result}


Prepare OM
    ${param} =     Get OM Service Param
    ${result} =       Execute     ozone admin om prepare ${param}
    Should contain    ${result}   OM Preparation successful!


SCM Finalization Status
    ${result} =    Execute      ozone admin scm finalizationstatus
    Log       ${result}
    RETURN    ${result}


Finalize SCM
    ${result} =    Execute      ozone admin scm finalizeupgrade
    Log       ${result}
    RETURN    ${result}

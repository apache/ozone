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
Resource            ../lib/os.robot

*** Keywords ***
Freon DCG
    [arguments]    ${prefix}=dcg    ${n}=1    ${size}=1024
    Return From Keyword If    '${SECURITY_ENABLED}' == 'true'
    ${result} =        Execute          ozone freon dcg -t1 -n${n} -s${size} -p ${prefix}
                       Should contain   ${result}   Successful executions: ${n}

Freon DCV
    [arguments]    ${prefix}=dcg    ${n}=1
    Return From Keyword If    '${SECURITY_ENABLED}' == 'true'
    ${result} =        Execute          ozone freon dcv -t1 -n${n} -p ${prefix}
                       Should contain   ${result}   Successful executions: ${n}

Freon OCKG
    [arguments]    ${prefix}=ockg    ${n}=1    ${size}=1024
    ${result} =        Execute          ozone freon ockg ${OM_HA_PARAM} -t1 -n${n} -s${size} -p ${prefix}
                       Should contain   ${result}   Successful executions: ${n}

Freon OCKV
    [arguments]    ${prefix}=ockg    ${n}=1
    ${result} =        Execute          ozone freon ockv ${OM_HA_PARAM} -t1 -n${n} -p ${prefix}
                       Should contain   ${result}   Successful executions: ${n}

Freon OCKR
    [arguments]    ${prefix}=ockg    ${n}=1
    ${result} =        Execute          ozone freon ockr ${OM_HA_PARAM} -t1 -n${n} -p ${prefix}
                       Should contain   ${result}   Successful executions: ${n}

Freon OMKG
    [arguments]    ${prefix}=omkg    ${n}=1    ${size}=1024
    ${result} =        Execute          ozone freon omkg ${OM_HA_PARAM} -t1 -n${n} -s${size} -p ${prefix}
                       Should contain   ${result}   Successful executions: ${n}

Freon OMBG
    [arguments]    ${prefix}=ombg    ${n}=1
    ${result} =        Execute          ozone freon ombg ${OM_HA_PARAM} -t1 -n${n} -p ${prefix}
                       Should contain   ${result}   Successful executions: ${n}

Freon OMBV
    [arguments]    ${prefix}=ombg    ${n}=1
    ${result} =        Execute          ozone freon ombv ${OM_HA_PARAM} -t1 -n${n} -p ${prefix}
                       Should contain   ${result}   Successful executions: ${n}

Freon OMBR
    [arguments]    ${prefix}=ombg    ${n}=1
    ${result} =        Execute          ozone freon ombr ${OM_HA_PARAM} -t1 -n${n} -p ${prefix}
                       Should contain   ${result}   Successful executions: ${n}

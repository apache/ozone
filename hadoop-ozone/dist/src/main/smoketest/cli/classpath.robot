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
Documentation       Test ozone classpath command and --validate classpath checks (HDDS-7373)
Library             BuiltIn
Resource            ../lib/os.robot
Resource            ../ozone-lib/shell.robot
Test Timeout        5 minutes

*** Variables ***
${TEMP_DIR}    /tmp

*** Keywords ***
Append missing jar path to classpath descriptor
    [arguments]    ${ozone_home}    ${artifact}    ${bogus_jar}
    ${cp_file} =    Set Variable    ${ozone_home}/share/ozone/classpath/${artifact}.classpath
    Execute    sed -i 's_$_:${bogus_jar}_' '${cp_file}'

Copy Ozone install and inject missing jar classpath entry
    [arguments]    ${copy_subdir}    ${bogus_jar_basename}    ${artifact}
    ${OZONE_COPY} =    Set Variable    ${TEMP_DIR}/${copy_subdir}
    ${bogus_jar} =    Set Variable    ${TEMP_DIR}/${bogus_jar_basename}
    Copy Directory    ${OZONE_DIR}    ${OZONE_COPY}
    Append missing jar path to classpath descriptor    ${OZONE_COPY}    ${artifact}    ${bogus_jar}
    [return]    ${OZONE_COPY}    ${bogus_jar}

*** Test Cases ***
Ignores HADOOP_CLASSPATH if OZONE_CLASSPATH is set
    [setup]    Create File         ${TEMP_DIR}/hadoop-classpath.jar
    Set Environment Variable   HADOOP_CLASSPATH  ${TEMP_DIR}/hadoop-classpath.jar
    Set Environment Variable   OZONE_CLASSPATH   ${EMPTY}
    ${output} =         Execute          ozone classpath ozone-insight
                        Should Contain   ${output}   hdds-interface
                        Should Not Contain   ${output}   ${TEMP_DIR}/hadoop-classpath.jar
    [teardown]    Remove File         ${TEMP_DIR}/hadoop-classpath.jar

Picks up items from OZONE_CLASSPATH
    [setup]    Create File         ${TEMP_DIR}/ozone-classpath.jar
    Set Environment Variable   OZONE_CLASSPATH  ${TEMP_DIR}/ozone-classpath.jar
    ${output} =         Execute          ozone classpath ozone-insight
                        Should Contain   ${output}   ${TEMP_DIR}/ozone-classpath.jar
    [teardown]    Remove File         ${TEMP_DIR}/ozone-classpath.jar

Adds optional dir entries
    Set Environment Variable   OZONE_CLASSPATH  ${EMPTY}
    ${OZONE_COPY} =     Set Variable     ${TEMP_DIR}/ozone-copy
    Copy Directory      ${OZONE_DIR}     ${OZONE_COPY}
    ${jars_dir} =       Find Jars Dir    ${OZONE_COPY}
    Create File         ${jars_dir}/ozone-insight/optional.jar

    ${output} =         Execute          ${OZONE_COPY}/bin/ozone classpath ozone-insight
                        Should Contain   ${output}   ${jars_dir}/ozone-insight/optional.jar

    [teardown]    Remove Directory    ${OZONE_COPY}    recursive=True

Validate classpath succeeds when all jars are present
    ${output} =         Execute          ozone --validate classpath ozone-insight
                        Should Contain   ${output}   Validating classpath file:
                        Should Contain   ${output}   Validation SUCCESSFUL

Validate classpath fails when classpath descriptor is missing
    ${output} =         Execute and checkrc    ozone --validate classpath hdds-nonexistent-artifact-7373    1
                        Should Contain   ${output}   ERROR: Classpath file descriptor

Validate classpath fails when a listed jar is missing
    ${OZONE_COPY}    ${bogus_jar} =    Copy Ozone install and inject missing jar classpath entry
    ...    ozone-validate-missing-jar    ozone-robot-missing-7373.jar    ozone-insight
    ${output} =         Execute and checkrc    ${OZONE_COPY}/bin/ozone --validate classpath ozone-insight    1
                        Should Contain   ${output}   ERROR: Jar file ${bogus_jar} is missing
                        Should Contain   ${output}   Validation FAILED due to missing jar files!
    [teardown]    Remove Directory    ${OZONE_COPY}    recursive=True

Validate with wrong subcommand prints usage
    ${output} =         Execute and checkrc    ozone --validate version    1
                        Should Contain   ${output}   Usage I: ozone --validate classpath

Validate continue allows daemon command after failed classpath check
    ${OZONE_COPY}    ${bogus_jar} =    Copy Ozone install and inject missing jar classpath entry
    ...    ozone-validate-continue    ozone-robot-missing-scm-7373.jar    hdds-server-scm
    ${output} =         Execute And Ignore Error    ${OZONE_COPY}/bin/ozone --validate continue --daemon status scm
                        Should Contain   ${output}   Validation FAILED due to missing jar files! Continuing command execution
    [teardown]    Remove Directory    ${OZONE_COPY}    recursive=True

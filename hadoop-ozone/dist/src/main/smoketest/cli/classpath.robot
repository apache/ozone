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
Documentation       Test ozone classpath command
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Test Cases ***
Ignores HADOOP_CLASSPATH if OZONE_CLASSPATH is set
    Create File         %{OZONE_HOME=/opt/hadoop}/share/ozone/lib/hadoop-classpath.jar
    Set Environment Variable   HADOOP_CLASSPATH  %{OZONE_HOME=/opt/hadoop}/share/ozone/lib/hadoop-classpath.jar
    Set Environment Variable   OZONE_CLASSPATH   ${EMPTY}
    ${output} =         Execute          ozone classpath hadoop-ozone-insight
                        Should Contain   ${output}   hadoop-hdds-interface
                        Should Not Contain   ${output}   hadoop-classpath.jar

Picks up items from OZONE_CLASSPATH
    Create File         %{OZONE_HOME=/opt/hadoop}/share/ozone/lib/ozone-classpath.jar
    Set Environment Variable   OZONE_CLASSPATH  %{OZONE_HOME=/opt/hadoop}/share/ozone/lib/ozone-classpath.jar
    ${output} =         Execute          ozone classpath hadoop-ozone-insight
                        Should Contain   ${output}   ozone-classpath.jar

Adds optional dir entries
    Create File         %{OZONE_HOME=/opt/hadoop}/share/ozone/lib/hadoop-ozone-insight/optional.jar
    ${output} =         Execute          ozone classpath hadoop-ozone-insight
                        Should Contain   ${output}   optional.jar

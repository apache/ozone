#!/usr/bin/env bash
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

if [[ "${OZONE_SHELL_EXECNAME}" == ozone ]]; then
   ozone_add_profile ozone_manager
fi

_ozone_manager_hadoop_finalize() {
  if [[ "${OZONE_CLASSNAME}" == "org.apache.hadoop.ozone.om.OzoneManagerStarter" ]]; then
    if [[ -n ${OZONE_MANAGER_CLASSPATH} ]]; then
      echo "Ozone Manager classpath extended by ${OZONE_MANAGER_CLASSPATH}"
      ozone_add_to_classpath_userpath "${OZONE_MANAGER_CLASSPATH}"
    fi

    if [[ ! "$OZONE_CLASSPATH" =~ "ozone-multitenancy" ]]; then
      ozone_add_classpath_from_file "${OZONE_HOME}/share/ozone/classpath/ozone-multitenancy-ranger.classpath"
    fi
  fi
}

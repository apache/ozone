#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Get the version string of the bash
bash_version_string=$(bash --version | head -n 1)

# Extract the major, minor, and patch version numbers.
BASH_VERSION_NUM=$(echo "$bash_version_string" | awk -F'[^0-9]*' '{print $2"."$3"."$4}')

# Extract major and minor for the comparison logic
BASH_MAJOR_VERSION=$(echo "$BASH_VERSION_NUM" | cut -d. -f1)
BASH_MINOR_VERSION=$(echo "$BASH_VERSION_NUM" | cut -d. -f2)

# Check bash version and call the appropriate script with appropriate flags
if [[ $BASH_MAJOR_VERSION -lt 4 ]] || { [[ $BASH_MAJOR_VERSION -eq 4 ]] && [[ $BASH_MINOR_VERSION -lt 2 ]]; }; then
    echo "+------------------------------------------------------------------------------------+"
    echo "|                                                                                    |"
    echo "|  WARNING: You are running Bash version $BASH_VERSION_NUM.                                     |"
    echo "|  It is recommended to use Bash 4.2 or newer.                                       |"
    echo "|  Bash 4.2 was released on February 13, 2011 and addresses vulnerabilities such as: |"
    echo "|  CVE-2019-18276, CVE-2019-9924, CVE-2016-9401, and CVE-2016-7543.                  |"
    echo "|                                                                                    |"
    echo "|  Please consider updating your bash to a more recent version.                      |"
    echo "|                                                                                    |"
    echo "+------------------------------------------------------------------------------------+"
    ./bash_old.sh -v
else
    ./bash_new.sh -v -z
fi
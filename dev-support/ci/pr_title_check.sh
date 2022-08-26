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

TITLE=$1

assertMatch() {
    echo "${TITLE}" | grep -E "$1" >/dev/null
    ret=$?
    if [ $ret -ne 0 ]; then
        echo $2
        exit 1
    fi
}

assertNotMatch() {
    echo "${TITLE}" | grep -E -v "$1" >/dev/null
    ret=$?
    if [ $ret -ne 0 ]; then
        echo $2
        exit 1
    fi
}

assertMatch    '^HDDS'                             'Fail: must start with HDDS'
assertMatch    '^HDDS-'                            'Fail: missing dash in Jira'
assertNotMatch '^HDDS-0'                           'Fail: leading zero in Jira'
assertMatch    '^HDDS-[1-9][0-9]{0,4}[^0-9]'       'Fail: Jira must be 1 to 5 digits'
assertMatch    '^HDDS-[1-9][0-9]{0,4}\.'           'Fail: missing dot after Jira'
assertMatch    '^HDDS-[1-9][0-9]{0,4}\. '          'Fail: missing space after Jira'
assertNotMatch '[[:space:]]$'                      'Fail: trailing space'
assertNotMatch '\.{3,}$'                           'Fail: trailing ellipsis indicates title is cut'
assertNotMatch '…$'                                'Fail: trailing ellipsis indicates title is cut'
assertNotMatch '[[:space:]]{2}'                    'Fail: two consecutive spaces'
assertMatch    '^HDDS-[1-9][0-9]{0,4}\. .*[^ ]$'   'Fail: not match "^HDDS-[1-9][0-9]{0,4}\. .*[^ ]$"'

echo 'OK'

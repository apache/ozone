#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# shellcheck source=dev-support/ci/lib/_script_init.sh
. dev-support/ci/lib/_script_init.sh

# Remove the square brackets
ALL_BASIC_CHECKS="${ALL_BASIC_CHECKS[@]#\[}"
ALL_BASIC_CHECKS="${ALL_BASIC_CHECKS[@]%\]}"

# Replace commas with spaces to form a space-delimited list
SPACE_DELIMITED_ALL_CHECKS=$(echo "$ALL_BASIC_CHECKS" | tr -d '"' | tr ',' ' ')

BASIC_CHECKS=$(grep -lr '^#checks:basic' hadoop-ozone/dev-support/checks \
                       | sort -u | xargs -n1 basename \
                       | cut -f1 -d'.')

UNIT_CHECKS=$(grep -lr '^#checks:unit' hadoop-ozone/dev-support/checks \
                       | sort -u | xargs -n1 basename \
                       | cut -f1 -d'.')

if [[ -n "${SPACE_DELIMITED_ALL_CHECKS}" ]]; then
    SPACE_DELIMITED_ALL_CHECKS=" ${SPACE_DELIMITED_ALL_CHECKS[*]} "     # add framing blanks
    basic=()
    for item in ${BASIC_CHECKS[@]}; do
      if [[ $SPACE_DELIMITED_ALL_CHECKS =~ " $item " ]] ; then          # use $item as regexp
        basic+=($item)
      fi
    done
    if [[ -n "${basic[@]}" ]]; then
        initialization::ga_output needs-basic-check "true"
    fi
    initialization::ga_output basic-checks \
        "$(initialization::parameters_to_json ${basic[@]})"

    unit=()
    for item in ${UNIT_CHECKS[@]}; do
      if [[ $SPACE_DELIMITED_ALL_CHECKS =~ " $item " ]] ; then    # use $item as regexp
        unit+=($item)
      fi
    done
    if [[ -n "${unit[@]}" ]]; then
        initialization::ga_output needs-unit-check "true"
    fi
    initialization::ga_output unit-checks \
        "$(initialization::parameters_to_json ${unit[@]})"
fi


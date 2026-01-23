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

if [[ -n "${SPACE_DELIMITED_ALL_CHECKS}" ]]; then
  # add framing blanks
  SPACE_DELIMITED_ALL_CHECKS=" ${SPACE_DELIMITED_ALL_CHECKS[*]} "

  for check in basic; do
    CHECKS=$(grep -lr "^#checks:${check}$" hadoop-ozone/dev-support/checks \
      | sort -u \
      | xargs -n1 basename \
      | cut -f1 -d'.')

    check_list=()
    for item in ${CHECKS[@]}; do
      # use $item as regex
      if [[ $SPACE_DELIMITED_ALL_CHECKS =~ " $item " ]] ; then
        check_list+=($item)
      fi
    done
    if [[ -n "${check_list[@]}" ]]; then
      initialization::ga_output "needs-${check}-check" "true"
    fi
    initialization::ga_output "${check}-checks" \
      "$(initialization::parameters_to_json ${check_list[@]})"
  done
fi

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

# This script does common processing after Maven-based checks.
#
# - ensures Maven error is reported as failure
# - writes number of failures into file
# - exits with the correct code

# Prerequisites:
# - $rc should be set to Maven exit code
# - $REPORT_DIR should be defined
# - $REPORT_FILE should be defined
# - Maven output should be saved in $REPORT_DIR/output.log

if [[ ! -d "${REPORT_DIR}" ]]; then
  mkdir -p "${REPORT_DIR}"
fi

if [[ ! -s "${REPORT_FILE}" ]]; then
  # check if there are errors in the log
  if [[ -n "${ERROR_PATTERN:-}" ]]; then
    if [[ -e "${REPORT_DIR}/output.log" ]]; then
      grep -m25 "${ERROR_PATTERN}" "${REPORT_DIR}/output.log" > "${REPORT_FILE}"
    else
      echo "Unknown failure, output.log missing" > "${REPORT_FILE}"
    fi
  fi
  # script failed, but report file is empty (does not reflect failure)
  if [[ ${rc} -ne 0 ]] && [[ ! -s "${REPORT_FILE}" ]]; then
    echo "Unknown failure, check output.log" > "${REPORT_FILE}"
  fi
fi

# number of failures = number of lines in report, unless file already created with custom count
if [[ ! -s "${REPORT_DIR}/failures" ]]; then
  wc -l "$REPORT_FILE" | awk '{ print $1 }' > "$REPORT_DIR/failures"
fi

# exit with failure if report is not empty
if [[ -s "${REPORT_FILE}" ]] && [[ ${ITERATIONS:-1} -eq 1 ]]; then
  rc=1
fi

exit ${rc}

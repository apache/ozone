#!/usr/bin/env bats
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

setup() {
  REPORT_DIR=$(mktemp -d)
  export REPORT_DIR
}

teardown() {
  rm -rf "${REPORT_DIR}"
}

@test "Error Prone reports Maven and javac findings and fails for errors" {
  export rc=1
  cat > "${REPORT_DIR}/output.log" <<'EOF'
[WARNING] The following options were not recognized by any processor: '[artifactId]'
[WARNING] /src/Legacy.java:[1,1] [JdkObsolete] legacy finding
[WARNING] /src/Custom.java:[3,1] [Custom-Rule_1] custom finding
[ERROR] /src/Format.java:[2,1] [FormatString] error finding
[ERROR] /src/Format.java:[2,1] [FormatString] error finding
/src/Raw.java:4: warning: [RawWarning] raw warning
/src/Raw.java:5: error: [RawError] raw error
[ERROR] Failed to execute goal example:example: failed
EOF

  run hadoop-ozone/dev-support/checks/_errorprone_report.sh

  [ "$status" -eq 1 ]
  [ "$(wc -l < "${REPORT_DIR}/diagnostics.txt")" -eq 5 ]
  grep -q '^\[WARNING\].*\[JdkObsolete\]' "${REPORT_DIR}/diagnostics.txt"
  grep -q '^\[ERROR\].*\[FormatString\]' "${REPORT_DIR}/diagnostics.txt"
  grep -q '^\[WARNING\].*\[Custom-Rule_1\]' "${REPORT_DIR}/diagnostics.txt"
  grep -q '^/src/Raw.java:4: warning: \[RawWarning\]' "${REPORT_DIR}/diagnostics.txt"
  grep -q '^/src/Raw.java:5: error: \[RawError\]' "${REPORT_DIR}/diagnostics.txt"
  ! grep -q 'Failed to execute goal' "${REPORT_DIR}/diagnostics.txt"
  [ "$(wc -l < "${REPORT_DIR}/summary.txt")" -eq 2 ]
  grep -q '^\[ERROR\].*\[FormatString\]' "${REPORT_DIR}/summary.txt"
  grep -q '^/src/Raw.java:5: error: \[RawError\]' "${REPORT_DIR}/summary.txt"
  [ "$(< "${REPORT_DIR}/failures")" -eq 2 ]
}

@test "Error Prone warnings do not fail the check" {
  export rc=0
  cat > "${REPORT_DIR}/output.log" <<'EOF'
[WARNING] /src/Legacy.java:[1,1] [JdkObsolete] legacy finding
/src/Raw.java:4: warning: [RawWarning] raw warning
EOF

  run hadoop-ozone/dev-support/checks/_errorprone_report.sh

  [ "$status" -eq 0 ]
  [ "$(wc -l < "${REPORT_DIR}/diagnostics.txt")" -eq 2 ]
  [ "$(< "${REPORT_DIR}/failures")" -eq 0 ]
  [ ! -s "${REPORT_DIR}/summary.txt" ]
}

@test "Error Prone reports an unknown Maven failure" {
  export rc=1
  cat > "${REPORT_DIR}/output.log" <<'EOF'
[ERROR] Failed to execute goal example:example: failed
EOF

  run hadoop-ozone/dev-support/checks/_errorprone_report.sh

  [ "$status" -eq 1 ]
  [ ! -s "${REPORT_DIR}/diagnostics.txt" ]
  [ "$(< "${REPORT_DIR}/summary.txt")" = "Unknown failure, check output.log" ]
  [ "$(< "${REPORT_DIR}/failures")" -eq 1 ]
}

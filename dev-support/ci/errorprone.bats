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
  TEST_TMPDIR=$(mktemp -d)
  export TEST_TMPDIR
  export OUTPUT_DIR="${TEST_TMPDIR}/report"
  mkdir -p "${TEST_TMPDIR}/bin" "${OUTPUT_DIR}"
  echo 9 > "${OUTPUT_DIR}/failures"
  export MAVEN_EXIT_CODE=1
  cat > "${TEST_TMPDIR}/bin/mvn" <<'EOF'
#!/usr/bin/env bash
rm -rf "${OUTPUT_DIR}"
printf '%s\n' \
  "[WARNING] The following options were not recognized by any processor: '[artifactId]'" \
  "[WARNING] /src/Legacy.java:[1,1] [JdkObsolete] legacy finding" \
  "[WARNING] /src/Custom.java:[3,1] [Custom-Rule_1] custom finding" \
  "[ERROR] /src/Format.java:[2,1] [FormatString] error finding" \
  "[ERROR] /src/Format.java:[2,1] [FormatString] error finding" \
  "/src/Raw.java:4: warning: [RawWarning] raw warning" \
  "[ERROR] Failed to execute goal example:example: failed"
printf '%s\n' "/src/Raw.java:5: error: [RawError] raw error" >&2
exit "${MAVEN_EXIT_CODE}"
EOF
  chmod +x "${TEST_TMPDIR}/bin/mvn"
  export PATH="${TEST_TMPDIR}/bin:${PATH}"
}

teardown() {
  rm -rf "${TEST_TMPDIR}"
}

@test "Error Prone reports Maven and javac findings and fails for errors" {
  run hadoop-ozone/dev-support/checks/errorprone.sh

  [ "$status" -eq 1 ]
  [ "$(wc -l < "${OUTPUT_DIR}/diagnostics.txt")" -eq 5 ]
  grep -q '^\[WARNING\].*\[JdkObsolete\]' "${OUTPUT_DIR}/diagnostics.txt"
  grep -q '^\[ERROR\].*\[FormatString\]' "${OUTPUT_DIR}/diagnostics.txt"
  grep -q '^\[WARNING\].*\[Custom-Rule_1\]' "${OUTPUT_DIR}/diagnostics.txt"
  grep -q '^/src/Raw.java:4: warning: \[RawWarning\]' "${OUTPUT_DIR}/diagnostics.txt"
  grep -q '^/src/Raw.java:5: error: \[RawError\]' "${OUTPUT_DIR}/diagnostics.txt"
  ! grep -q 'Failed to execute goal' "${OUTPUT_DIR}/diagnostics.txt"
  [ "$(wc -l < "${OUTPUT_DIR}/summary.txt")" -eq 2 ]
  grep -q '^\[ERROR\].*\[FormatString\]' "${OUTPUT_DIR}/summary.txt"
  grep -q '^/src/Raw.java:5: error: \[RawError\]' "${OUTPUT_DIR}/summary.txt"
  [ "$(< "${OUTPUT_DIR}/failures")" -eq 2 ]
}

@test "Error Prone warnings do not fail the check" {
  export MAVEN_EXIT_CODE=0
  cat > "${TEST_TMPDIR}/bin/mvn" <<'EOF'
#!/usr/bin/env bash
rm -rf "${OUTPUT_DIR}"
printf '%s\n' \
  "[WARNING] /src/Legacy.java:[1,1] [JdkObsolete] legacy finding" \
  "/src/Raw.java:4: warning: [RawWarning] raw warning"
exit "${MAVEN_EXIT_CODE}"
EOF
  chmod +x "${TEST_TMPDIR}/bin/mvn"

  run hadoop-ozone/dev-support/checks/errorprone.sh

  [ "$status" -eq 0 ]
  [ "$(wc -l < "${OUTPUT_DIR}/diagnostics.txt")" -eq 2 ]
  [ "$(< "${OUTPUT_DIR}/failures")" -eq 0 ]
  [ ! -s "${OUTPUT_DIR}/summary.txt" ]
}

@test "Error Prone reports an unknown Maven failure" {
  cat > "${TEST_TMPDIR}/bin/mvn" <<'EOF'
#!/usr/bin/env bash
rm -rf "${OUTPUT_DIR}"
echo "[ERROR] Failed to execute goal example:example: failed"
exit "${MAVEN_EXIT_CODE}"
EOF
  chmod +x "${TEST_TMPDIR}/bin/mvn"

  run hadoop-ozone/dev-support/checks/errorprone.sh

  [ "$status" -eq 1 ]
  [ ! -s "${OUTPUT_DIR}/diagnostics.txt" ]
  [ "$(< "${OUTPUT_DIR}/summary.txt")" = "Unknown failure, check output.log" ]
  [ "$(< "${OUTPUT_DIR}/failures")" -eq 1 ]
}

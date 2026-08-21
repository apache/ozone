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
  export INCLUDE_ERROR=true
  cat > "${TEST_TMPDIR}/bin/mvn" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' \
  "[WARNING] The following options were not recognized by any processor: '[artifactId]'" \
  "[WARNING] /src/Legacy.java:[1,1] [JdkObsolete] legacy finding"
if [[ "${INCLUDE_ERROR}" == "true" ]]; then
  echo "[ERROR] /src/Format.java:[2,1] [FormatString] error finding"
  echo "[ERROR] /src/Format.java:[2,1] [FormatString] error finding"
fi
exit "${MAVEN_EXIT_CODE}"
EOF
  chmod +x "${TEST_TMPDIR}/bin/mvn"
  export PATH="${TEST_TMPDIR}/bin:${PATH}"
}

teardown() {
  rm -rf "${TEST_TMPDIR}"
}

@test "Error Prone reports all findings and fails for errors" {
  run hadoop-ozone/dev-support/checks/errorprone.sh

  [ "$status" -eq 1 ]
  [ "$(wc -l < "${OUTPUT_DIR}/summary.txt")" -eq 2 ]
  grep -q '^\[WARNING\].*\[JdkObsolete\]' "${OUTPUT_DIR}/summary.txt"
  grep -q '^\[ERROR\].*\[FormatString\]' "${OUTPUT_DIR}/summary.txt"
  grep -q '^### Error Prone errors$' "${OUTPUT_DIR}/summary.md"
  [ "$(grep -c '^\[ERROR\].*\[FormatString\]' "${OUTPUT_DIR}/summary.md")" -eq 1 ]
  ! grep -q '^\[WARNING\]' "${OUTPUT_DIR}/summary.md"
  [ "$(< "${OUTPUT_DIR}/failures")" -eq 1 ]
}

@test "Error Prone warnings do not fail the check" {
  export MAVEN_EXIT_CODE=0
  export INCLUDE_ERROR=false

  run hadoop-ozone/dev-support/checks/errorprone.sh

  [ "$status" -eq 0 ]
  [ "$(wc -l < "${OUTPUT_DIR}/summary.txt")" -eq 1 ]
  [ "$(< "${OUTPUT_DIR}/failures")" -eq 0 ]
  [ ! -e "${OUTPUT_DIR}/summary.md" ]
}

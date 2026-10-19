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
  local checks=hadoop-ozone/dev-support/checks
  mkdir -p "${TEST_TMPDIR}/${checks}/install" "${TEST_TMPDIR}/bin"
  cp "${checks}/infer.sh" "${checks}/_lib.sh" "${TEST_TMPDIR}/${checks}/"
  cp "${checks}/install/infer.sh" "${TEST_TMPDIR}/${checks}/install/"
  cd "${TEST_TMPDIR}"
  POMS=(hadoop-ozone/ozonefs-hadoop2/pom.xml hadoop-ozone/ozonefs-shaded/pom.xml)
  for pom in "${POMS[@]}"; do
    mkdir -p "$(dirname "${pom}")"
    printf '%s\n' '<project/>' > "${pom}"
  done
  git init -q
  git add hadoop-ozone
  git -c user.name=Test -c user.email=test@example.com -c core.hooksPath=/dev/null commit -qm initial
  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'if [[ "$1" == debug ]]; then' \
    '  if [[ "${INFER_EMPTY_CAPTURE:-false}" != true ]]; then echo src/main/java/Example.java; fi' \
    '  exit 0' \
    'fi' \
    'touch "${TEST_TMPDIR}/infer-called"' \
    'printf "%s\n" "$@" > "${TEST_TMPDIR}/infer-args"' \
    'for arg in "$@"; do if [[ "$arg" == clean ]]; then rm -rf target; fi; done' \
    'if [[ "${INFER_NO_REPORT:-false}" != true ]]; then' \
    '  mkdir -p infer-out' \
    '  printf "%s\n" "${INFER_REPORT:-No issues found}" > infer-out/report.txt' \
    '  printf "%s\n" "${INFER_JSON-[]}" > infer-out/report.json' \
    'fi' \
    'echo "Infer output"' \
    'printf "%s\n" modified > hadoop-ozone/ozonefs-hadoop2/pom.xml' \
    'printf "%s\n" modified > hadoop-ozone/ozonefs-shaded/pom.xml' \
    'if [[ -n "${INFER_SIGNAL:-}" ]]; then kill -"${INFER_SIGNAL}" "$PPID"; fi' \
    'exit "${INFER_EXIT_CODE:-0}"' > bin/infer
  chmod +x bin/infer
  export PATH="${TEST_TMPDIR}/bin:${PATH}"
  export OZONE_PREFER_LOCAL_TOOL=true
}

teardown() {
  cd /
  rm -rf "${TEST_TMPDIR}"
}

@test "Infer refuses unstaged POM edits without changing either file" {
  printf '%s\n' 'local edits' >> "${POMS[0]}"
  cp "${POMS[0]}" expected

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 1 ]
  [ ! -e infer-called ]
  cmp expected "${POMS[0]}"
  git diff --quiet -- "${POMS[1]}"
}

@test "Infer refuses staged POM edits without changing the index or worktree" {
  printf '%s\n' 'staged edits' >> "${POMS[1]}"
  git add "${POMS[1]}"
  cp "${POMS[1]}" expected
  local index_before
  index_before=$(git write-tree)

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 1 ]
  [ ! -e infer-called ]
  cmp expected "${POMS[1]}"
  [ "$(git write-tree)" = "${index_before}" ]
}

@test "Infer refuses a deleted POM without recreating it" {
  rm "${POMS[0]}"

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 1 ]
  [ ! -e infer-called ]
  [ ! -e "${POMS[0]}" ]
}

@test "Infer restores POMs after successful analysis and preserves unrelated edits" {
  printf '%s\n' '# local edits' >> hadoop-ozone/dev-support/checks/_lib.sh
  cp hadoop-ozone/dev-support/checks/_lib.sh expected

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 0 ]
  [ -e infer-called ]
  git diff --exit-code -- "${POMS[@]}"
  cmp expected hadoop-ozone/dev-support/checks/_lib.sh
}

@test "Infer restores POMs after an analysis failure" {
  export INFER_EXIT_CODE=2

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 1 ]
  [ -e infer-called ]
  git diff --exit-code -- "${POMS[@]}"
}

@test "Infer restores POMs when interrupted by TERM" {
  export INFER_SIGNAL=TERM

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 143 ]
  [ -e infer-called ]
  git diff --exit-code -- "${POMS[@]}"
}

@test "Infer restores POMs when interrupted by INT" {
  export INFER_SIGNAL=INT

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 130 ]
  [ -e infer-called ]
  git diff --exit-code -- "${POMS[@]}"
}

@test "Infer cleans the build without losing the output log" {
  mkdir -p target
  touch target/previous-build

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 0 ]
  [ ! -e target/previous-build ]
  grep -q 'Infer output' target/infer/output.log
  grep -qx -- '-Ddevelocity.cache.local.enabled=false' infer-args
  grep -qx -- '-Ddevelocity.cache.remote.enabled=false' infer-args
}

@test "Infer failures are not hidden by a fresh report" {
  export INFER_EXIT_CODE=5

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 1 ]
  grep -q 'code 5' target/infer/summary.txt
  [ -f target/infer/report.txt ]
}

@test "Infer failures do not reuse previous reports" {
  mkdir -p target/infer infer-out
  printf '%s\n' stale > target/infer/report.txt
  printf '%s\n' stale > infer-out/report.txt
  export INFER_EXIT_CODE=5 INFER_NO_REPORT=true

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 1 ]
  [ ! -e target/infer/report.txt ]
  grep -q 'code 5' target/infer/summary.txt
}

@test "Infer requires a report even when the command succeeds" {
  export INFER_NO_REPORT=true

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 1 ]
  grep -q 'no report' target/infer/summary.txt
}

@test "Infer rejects empty capture instead of reporting a clean analysis" {
  export INFER_EMPTY_CAPTURE=true

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 1 ]
  grep -q 'no source files' target/infer/summary.txt
}

@test "Infer clean reports contain a single zero count" {
  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 0 ]
  [ "$(cat target/infer/failures)" = 0 ]
  grep -qx 'Infer analysis complete. Found 0 issues.' target/infer/summary.txt
}

@test "Infer counts findings from JSON and keeps them informational" {
  export INFER_REPORT='Example.java:1: error: Null Dereference'
  export INFER_JSON='[{"severity":"ERROR"},{"severity":"WARNING"},{"suppressed":true},{"censored_reason":"filtered"}]'

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 0 ]
  [ "$(cat target/infer/failures)" = 2 ]
  cmp infer-out/report.json target/infer/report.json
}

@test "Infer rejects an invalid JSON report" {
  local report
  for report in 'invalid JSON' '' '[] []' '{}' 'null'; do
    export INFER_JSON="${report}"
    run bash hadoop-ozone/dev-support/checks/infer.sh

    [ "${status}" -eq 1 ]
    grep -q 'invalid JSON report' target/infer/summary.txt
  done
}

@test "Infer installation failures preserve diagnostics in the artifact" {
  export OZONE_PREFER_LOCAL_TOOL=false
  printf '%s\n' '#!/usr/bin/env bash' 'echo "download failed" >&2' 'exit 22' > bin/curl
  chmod +x bin/curl

  run bash hadoop-ozone/dev-support/checks/infer.sh

  [ "${status}" -eq 1 ]
  [ ! -e infer-called ]
  grep -q 'Failed to install infer' target/infer/summary.txt
  grep -q 'download failed' target/infer/output.log
  ! grep -q 'unbound variable' target/infer/output.log
  git diff --exit-code -- "${POMS[@]}"
}

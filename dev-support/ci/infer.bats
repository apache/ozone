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
    'touch "${TEST_TMPDIR}/infer-called"' \
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

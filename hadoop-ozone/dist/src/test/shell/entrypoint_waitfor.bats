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

#
# Can be executed with bats (https://github.com/bats-core/bats-core)
# bats entrypoint_waitfor.bats
#

ENTRYPOINT="${BATS_TEST_DIRNAME}/../../main/dockerlibexec/entrypoint.sh"

setup() {
  RELTMP="${BATS_TEST_DIRNAME}/../../../target/test-dir/bats.$$.${RANDOM}"
  mkdir -p "${RELTMP}"

  TMP=$(cd -P -- "${RELTMP}" >/dev/null && pwd -P)
  export TMP

  # Stub nc on PATH: record its arguments and fail, so that entrypoint.sh
  # makes a single attempt (WAITFOR_TIMEOUT=0) and exits before exec.
  cat > "${TMP}/nc" <<'EOF'
#!/usr/bin/env bash
echo "$*" > "${NC_ARGS_FILE}"
exit 1
EOF
  chmod +x "${TMP}/nc"

  export NC_ARGS_FILE="${TMP}/nc.args"
  export PATH="${TMP}:${PATH}"
  export WAITFOR_TIMEOUT=0
}

teardown() {
  rm -rf "${TMP}"
}

assert_nc_args() {
  export WAITFOR="$1"

  run "${ENTRYPOINT}" true

  [[ "$status" -eq 1 ]]
  [[ "$(cat "${NC_ARGS_FILE}")" == "$2" ]]
}

@test "WAITFOR with hostname" {
  assert_nc_args "localhost:9878" "-z localhost 9878"
}

@test "WAITFOR with IPv4 address" {
  assert_nc_args "10.0.0.1:9878" "-z 10.0.0.1 9878"
}

@test "WAITFOR with bracketed IPv6 address" {
  assert_nc_args "[2001:db8::1]:9878" "-z 2001:db8::1 9878"
}

@test "WAITFOR with bracketed IPv6 loopback" {
  assert_nc_args "[::1]:9876" "-z ::1 9876"
}

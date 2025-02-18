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
# This script gets a list of the changed files from git, and matches
# that list against a set of regexes, each of which corresponds to a
# test group.  For each set of matches, a flag is set to let github
# actions know to run that test group.

# Downloads Node.js for multiple platforms (Linux, Mac)

set -u -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}/../.." || exit 1

: ${REPO_DIR:="${HOME}/.m2/repository"}

mkdir -p "${REPO_DIR}"

declare -i rc=1

if [[ -z "${NODEJS_VERSION:-}" ]]; then
  NODEJS_VERSION=$(mvn help:evaluate -Dexpression=nodejs.version -q -DforceStdout -Dscan=false)
fi

if [[ -n "${NODEJS_VERSION}" ]]; then
  rc=0
  for platform in darwin-x64 linux-x64; do
    url="https://nodejs.org/dist/v${NODEJS_VERSION}/node-v${NODEJS_VERSION}-${platform}.tar.gz"
    output="${REPO_DIR}/com/github/eirslett/node/${NODEJS_VERSION}/node-${NODEJS_VERSION}-${platform}.tar.gz"
    mkdir -pv "$(dirname "${output}")"

    irc=1
    for i in 1 2 3; do
      echo "Downloading ${url}, attempt ${i}"
      if curl --location --continue-at - -o "${output}" "${url}"; then
        irc=0
        break
      fi
      sleep 2
    done

    if [[ ${rc} -eq 0 ]]; then
      rc=${irc}
    fi
  done
fi

exit ${rc}

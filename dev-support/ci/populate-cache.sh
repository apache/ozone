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

# Populates local Maven repository (similar to go-offline, but using actual build).
# Downloads Node.js for multiple platforms (Linux, Mac)

set -u -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}/../.." || exit 1

: ${REPO_DIR:="${HOME}/.m2/repository"}
: ${MARKER_FILE:="${REPO_DIR}/.cache/org-apache-ozone/partial-cache"}

declare -i rc=0

if [[ ! -d "${REPO_DIR}" ]]; then
  echo "Error: repository dir ${REPO_DIR} does not exist"
  exit 1
fi

# Download Node.js manually
NODEJS_VERSION=$(mvn help:evaluate -Dexpression=nodejs.version -q -DforceStdout)
if [[ -n "${NODEJS_VERSION}" ]]; then
  for platform in darwin-x64 linux-x64; do
    output="${REPO_DIR}/com/github/eirslett/node/${NODEJS_VERSION}/node-${NODEJS_VERSION}-${platform}.tar.gz"
    url="https://nodejs.org/dist/v${NODEJS_VERSION}/node-v${NODEJS_VERSION}-${platform}.tar.gz"
    mkdir -pv "$(dirname "${output}")"
    curl --retry 3 --location --continue-at - -o "${output}" "${url}"
    irc=$?

    if [[ ${rc} -eq 0 ]]; then
      rc=${irc}
    fi
  done
fi

MAVEN_OPTIONS="--batch-mode --fail-at-end --no-transfer-progress --show-version -Pgo-offline -Pdist"
if ! mvn ${MAVEN_OPTIONS} clean verify; then
  rc=1
fi

if [[ ${rc} -eq 0 ]]; then
  rm -fv "${MARKER_FILE}"
else
  # Create Marker file if build fails to indicate cache is partial, needs to be rebuilt next time
  mkdir -pv "$(dirname "${MARKER_FILE}")"
  touch "${MARKER_FILE}"
fi

exit ${rc}

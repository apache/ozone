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


# This script checks if all third-party dependencies have licenses we can use.
# Optionally accepts the aggregated third-party license list file to be checked.
# Otherwise it requires Ozone to be available from Maven repo (can be local),
# so that it can generate the license list.
#
# When adding a new dependency to Ozone with a license that fails to match:
# * verify that the license is allowed, ref: https://www.apache.org/legal/resolved.html
# * tweak the patterns to allow
#
# Items for which license-maven-plugin cannot find license (e.g. jettison,
# jsp-api) are output as "Unknown license".  These dependencies should be
# filtered explicitly by adding them to the `license.exceptions` file, instead
# of allowing the generic "Unknown license".

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/license"}
mkdir -p "$REPORT_DIR"
REPORT_FILE="${REPORT_DIR}/summary.txt"

DEFAULT_SRC="target/generated-sources/license/THIRD-PARTY.txt"
src="${1:-${DEFAULT_SRC}}"

if [[ ! -e ${src} ]]; then
  MAVEN_OPTIONS="-B -fae -DskipDocs -DskipRecon --no-transfer-progress ${MAVEN_OPTIONS:-}"
  mvn ${MAVEN_OPTIONS} license:aggregate-add-third-party | tee "${REPORT_DIR}/output.log"
  src="${DEFAULT_SRC}"
fi

L='Licen[cs]e' # sometimes misspelled

# filter all allowed licenses; any remaining item indicates a possible problem
grep '(' ${src} \
  | grep -v -f <(grep -v -e '^#' -e '^$' "${DIR}"/license.exceptions | cut -f1 -d' ') \
  | ( grep -i -v \
    -e "Apache ${L}" -e "Apache Software ${L}" -e "Apache v2" -e "Apache.2" \
    -e "Bouncy Castle ${L}" \
    -e "(BSD)" -e "(The BSD ${L})" -e "\<BSD.[23]" -e "\<BSD ${L} [23]" -e "\<[23]\>.Clause.\<BSD\>" \
    -e "(CDDL\>" -e ' CDDL '\
    -e "(EDL\>" -e "Eclipse Distribution ${L}" \
    -e "(EPL\>" -e "Eclipse Public ${L}" \
    -e "(MIT)" -e "(MIT-0)" -e "\<MIT ${L}" \
    -e "Modified BSD\>" \
    -e "New BSD ${L}" \
    -e "Public Domain" \
    -e "Revised BSD\>" \
    || true ) \
  | sort -u \
  | tee "${REPORT_FILE}"
rc=$?

ERROR_PATTERN=""
source "${DIR}/_post_process.sh"

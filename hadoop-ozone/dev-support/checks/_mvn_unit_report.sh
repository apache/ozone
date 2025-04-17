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

REPORT_DIR=${REPORT_DIR:-$PWD}

_realpath() {
  if realpath "$@" > /dev/null; then
    realpath "$@"
  else
    local relative_to
    relative_to=$(realpath "${1/--relative-to=/}") || return 1
    realpath "$2" | sed -e "s@${relative_to}/@@"
  fi
}

tempfile="${REPORT_DIR}/summary.tmp"

## generate summary txt file
failures=${REPORT_DIR}/failures.txt
find "." -not -path '*/iteration*' -name 'TEST*.xml' -print0 \
    | xargs -n1 -0 "grep" -l -E "<failure|<error" \
    | awk -F/ '{sub("'"TEST-"'",""); sub(".xml",""); print $NF}' \
    > "${failures}"
cat ${failures} > "${tempfile}"

leaks=${REPORT_DIR}/leaks.txt
if [[ "${CHECK:-unit}" == "integration" ]]; then
  find hadoop-ozone/integration-test* -not -path '*/iteration*' -name '*-output.txt' -print0 \
      | xargs -n1 -0 "grep" -l -E "not closed properly|was not shutdown properly" \
      | awk -F/ '{sub("-output.txt",""); print $NF}' \
      > "${leaks}"
  cat ${leaks} >> "${tempfile}"
fi

cluster=${REPORT_DIR}/cluster-startup-errors.txt
if [[ "${CHECK:-unit}" == "integration" ]]; then
  find hadoop-ozone/integration-test* -not -path '*/iteration*' -name '*-output.txt' -print0 \
      | xargs -n1 -0 "grep" -l -E "Unable to build MiniOzoneCluster" \
      | awk -F/ '{sub("-output.txt",""); print $NF}' \
      > "${cluster}"
  cat ${cluster} >> "${tempfile}"
fi

#Copy heap dump and dump leftovers
find "." -not -path '*/iteration*' \
    \( -name "*.hprof" \
    -or -name "*.dump" \
    -or -name "*.dumpstream" \
    -or -name "hs_err_*.log" \) \
  -exec mv {} "$REPORT_DIR/" \;

## Add the tests where the JVM is crashed
crashes=${REPORT_DIR}/crashes.txt
grep -A1 'Crashed tests' "${REPORT_DIR}/output.log" \
  | grep -v -e 'Crashed tests' -e '--' \
  | cut -f2- -d' ' \
  | sort -u \
  > "${crashes}"
cat "${crashes}" >> "${tempfile}"

# Check for tests that started but were not finished
timeouts=${REPORT_DIR}/timeouts.txt
if grep -q 'There was a timeout.*in the fork' "${REPORT_DIR}/output.log"; then
  diff -uw \
    <(grep -e 'Running org' "${REPORT_DIR}/output.log" \
      | sed -e 's/.* \(org[^ ]*\)/\1/' \
      | uniq -c \
      | sort -u -k2) \
    <(grep -e 'Tests run: .* in org' "${REPORT_DIR}/output.log" \
      | sed -e 's/.* \(org[^ ]*\)/\1/' \
      | uniq -c \
      | sort -u -k2) \
    | grep '^- ' \
    | awk '{ print $3 }' \
    > "${timeouts}"
  cat "${timeouts}" >> "${tempfile}"
fi

sort -u "${tempfile}" | tee "${REPORT_DIR}/summary.txt"
rm "${tempfile}"

#Collect of all of the report files of FAILED tests
for failed_test in $(< ${REPORT_DIR}/summary.txt); do
  for file in $(find "." -not -path '*/iteration*' \
      \( -name "${failed_test}.txt" -or -name "${failed_test}-output.txt" -or -name "TEST-${failed_test}.xml" \)); do
    dir=$(dirname "${file}")
    dest_dir=$(_realpath --relative-to="${PWD}" "${dir}/../..") || continue
    mkdir -pv "${REPORT_DIR}/${dest_dir}"
    mv -v "${file}" "${REPORT_DIR}/${dest_dir}"/
  done
done

## Check if Maven was killed
if grep -q 'Killed.* mvn .* test ' "${REPORT_DIR}/output.log"; then
  echo 'Maven test run was killed' >> "${REPORT_DIR}/summary.txt"
fi

## generate summary markdown file
export SUMMARY_FILE="$REPORT_DIR/summary.md"
echo -n > "$SUMMARY_FILE"
if [ -s "${failures}" ]; then
  printf "# Failed Tests\n\n" >> "$SUMMARY_FILE"
  cat "${failures}" | sed 's/^/ * /' >> "$SUMMARY_FILE"
fi
rm -f "${failures}"

if [[ -s "${leaks}" ]]; then
  printf "# Leaks Detected\n\n" >> "$SUMMARY_FILE"
  cat "${leaks}" | sed 's/^/ * /' >> "$SUMMARY_FILE"
fi
rm -f "${leaks}"

if [[ -s "${cluster}" ]]; then
  printf "# Cluster Startup Errors\n\n" >> "$SUMMARY_FILE"
  cat "${cluster}" | sed 's/^/ * /' >> "$SUMMARY_FILE"
fi
rm -f "${cluster}"

if [[ -s "${crashes}" ]]; then
  printf "# Crashed Tests\n\n" >> "$SUMMARY_FILE"
  cat "${crashes}" | sed 's/^/ * /' >> "$SUMMARY_FILE"
fi
rm -f "${crashes}"

if [[ -s "${timeouts}" ]]; then
  printf "# Fork Timeout\n\n" >> "$SUMMARY_FILE"
  cat "${timeouts}" | sed 's/^/ * /' >> "$SUMMARY_FILE"
fi
rm -f "${timeouts}"

## generate counter
wc -l "$REPORT_DIR/summary.txt" | awk '{print $1}'> "$REPORT_DIR/failures"

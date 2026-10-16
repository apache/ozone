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

# shellcheck source=dev-support/ci/lib/_script_init.sh
. dev-support/ci/lib/_script_init.sh

SUITES=$(grep --no-filename -r '^#suite:' hadoop-ozone/dist/src/main/compose \
  | sort -u | cut -f2 -d':' | grep -v 'failing')
VERSIONS=$(grep '^old_versions=' hadoop-ozone/dist/src/main/compose/xcompat/lib.sh \
  | cut -f2 -d'"')

MATRIX=$(
  export SUITES
  python3 <<'PY'
import json
import os

parallel_s3 = {"HA-unsecure", "HA-secure", "secure", "EC"}
matrix = []
for suite in os.environ["SUITES"].split():
    if suite in ("compat-old", "failing"):
        continue
    if suite in parallel_s3:
        matrix.append({"suite": suite, "aws_cli": "aws", "split": f"{suite}-awscli-v1"})
        matrix.append({"suite": suite, "aws_cli": "aws2", "split": f"{suite}-awscli-v2"})
    else:
        matrix.append({"suite": suite, "aws_cli": "", "split": suite})
print(json.dumps(matrix))
PY
)

initialization::ga_output suites \
    "$(initialization::parameters_to_json ${SUITES})"
initialization::ga_output versions \
    "$(initialization::parameters_to_json ${VERSIONS})"
initialization::ga_output matrix "${MATRIX}"

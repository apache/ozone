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

# output modules with any Java tests, except the "main" ozone-integration-test
MODULES=$(find * -type d -path '*/src/test/java' | sed 's@/src/test/java@@' \
    | xargs -I{} grep -m1 '^  <artifactId>' {}/pom.xml \
    | cut -f2 -d'<' | cut -f2 -d'>' \
    | grep -v '^ozone-integration-test$' \
    | sort -u)

initialization::ga_output modules \
    "$(initialization::parameters_to_json ${MODULES})"

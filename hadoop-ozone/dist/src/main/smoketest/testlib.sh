#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## @description  Run Robot Framework report generator (rebot) in ozone-runner container.
## @param input directory where source Robot XML files are
## @param output directory where report should be placed
## @param rebot options and arguments

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SCRIPT_DIR}/../compose/testlib.sh"

run_rebot() {
  local input_dir="$(realpath "$1")"
  local output_dir="$(realpath "$2")"

  shift 2

  local tempdir="$(mktemp -d "${output_dir}"/rebot-XXXXXX)"
  #Should be writeable from the docker containers where user is different.
  chmod a+wx "${tempdir}"
  if docker run --rm -v "${input_dir}":/rebot-input -v "${tempdir}":/rebot-output -w /rebot-input \
      $(get_runner_image_spec) \
      bash -c "rebot --nostatusrc -d /rebot-output $@"; then
    mv -v "${tempdir}"/* "${output_dir}"/
  fi
  rmdir "${tempdir}"
}
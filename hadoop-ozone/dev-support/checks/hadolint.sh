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
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1
REPO_DIR="$DIR/../../.."

ERROR=0


for Dockerfile in $(find "$REPO_DIR/hadoop-ozone" "$REPO_DIR/hadoop-hdds" -name Dockerfile | sort); do
  echo "Checking $Dockerfile"

  result=$( hadolint $Dockerfile )
  if [ ! -z "$result" ]
  then
    echo "$result"
    echo ""
    ERROR=1
  fi
done

if [ "$ERROR" = 1 ]
then
  echo ""
  echo ""
  echo "Hadolint errors were found. Exit code: 1."
  exit 1
fi

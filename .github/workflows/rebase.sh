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
set -e
if [ "$GITHUB_EVENT_NAME" == "pull_request" ]; then
   echo "This is a pull request build. Trying to merge to the target branch."
   TARGET_BRANCH=$(jq -r '.pull_request.base.ref' "$GITHUB_EVENT_PATH")
   PR_HEAD=$(git rev-parse HEAD)
   git checkout -b $TARGET_BRANCH origin/$TARGET_BRANCH
   TARGET_HEAD=$(git rev-parse HEAD)
   echo "PR head $PR_HEAD will be merged to the origin $TARGET_BRANCH ($TARGET_HEAD):"
   git log -1
   git merge --squash $PR_HEAD
fi

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

#doc: add new empty commit to trigger new CI build

PR_URL=$(jq -r '.issue.pull_request.url' "$GITHUB_EVENT_PATH")
read -r REPO_URL BRANCH <<<"$(curl "$PR_URL" | jq -r '.head.repo.ssh_url + " " + .head.ref')"

git fetch "$REPO_URL" "$BRANCH"
git checkout FETCH_HEAD

git config --global user.email "noreply@github.com"
git config --global user.name "GitHub"

git commit --allow-empty -m "retest build"
git push origin
git push $REPO_URL HEAD:$BRANCH

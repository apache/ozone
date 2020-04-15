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

#doc: Close pending pull request temporary
# shellcheck disable=SC2124
MESSAGE="Thank you very much for the patch. I am closing this PR __temporarily__ as there was no 
activity recently and it is waiting for response from its author.

It doesn't mean that this PR is not important or ignored: feel free to reopen the PR at any time.

It only means that attention of committers is not required. We prefer to keep the review queue clean. This ensures PRs in need of review are more visible, which results in faster feedback for all PRs.

If you need ANY help to finish this PR, please [contact the community](https://github.com/apache/hadoop-ozone#contact) on the mailing list or the slack channel.

set +x #GITHUB_TOKEN
curl -s -o /dev/null \
  -X POST \
  --data "$(jq --arg body "$MESSAGE" -n '{body: $body}')" \
  --header "authorization: Bearer $GITHUB_TOKEN" \
  --header 'content-type: application/json' \
  "$(jq -r '.issue.comments_url' "$GITHUB_EVENT_PATH")"

curl -s -o /dev/null \
  -X PATCH \
  --data '{"state": "close"}' \
  --header "authorization: Bearer $GITHUB_TOKEN" \
  --header 'content-type: application/json' \
  "$(jq -r '.issue.pull_request.url' "$GITHUB_EVENT_PATH")"

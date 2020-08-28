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

#doc: Add a REQUESTED_CHANGE type review to mark issue non-mergeable: `/pending <reason>`
# shellcheck disable=SC2124
MESSAGE="Marking this issue as un-mergeable as requested.

Please use \`/ready\` comment when it's resolved.

Please note that the PR will be closed after 21 days of inactivity from now. (But can be re-opened anytime later...)
> $@"

URL="$(jq -r '.issue.pull_request.url' "$GITHUB_EVENT_PATH")/reviews"
set +x #GITHUB_TOKEN
curl -s -o /dev/null \
  --data "$(jq --arg body "$MESSAGE" -n '{event: "REQUEST_CHANGES", body: $body}')" \
  --header "authorization: Bearer $GITHUB_TOKEN" \
  --header 'content-type: application/json' \
  "$URL"

curl -s -o /dev/null \
  -X POST \
  --data '{"labels": [ "pending" ]}' \
  --header "authorization: Bearer $GITHUB_TOKEN" \
  "$(jq -r '.issue.url' "$GITHUB_EVENT_PATH")/labels"


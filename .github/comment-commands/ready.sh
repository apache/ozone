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

#doc: Dismiss all the blocking reviews by github-actions bot
MESSAGE="Blocking review request is removed."
URL="$(jq -r '.issue.pull_request.url' "$GITHUB_EVENT_PATH")/reviews"
set +x #GITHUB_TOKEN
curl -s "$URL" |
  jq -r '.[] | [.user.login, .id] | @tsv' |
  grep github-actions |
  awk '{print $2}' |
  xargs -n1 -IISSUE_ID curl -s -o /dev/null \
    -X PUT \
    --data "$(jq --arg message "$MESSAGE" -n '{message: $message}')" \
    --header "authorization: Bearer $GITHUB_TOKEN" \
    "$URL"/ISSUE_ID/dismissals

curl -s -o /dev/null \
  -X DELETE \
  --header "authorization: Bearer $GITHUB_TOKEN" \
  "$(jq -r '.issue.url' "$GITHUB_EVENT_PATH")/labels/pending"


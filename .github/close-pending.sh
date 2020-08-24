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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
MESSAGE=$(cat $SCRIPT_DIR/closing-message.txt)

while IFS= read -r number &&
      IFS= read -r title; do
      echo "Closing PR ($number): $title"
      curl -s -o /dev/null \
        -X POST \
        --data "$(jq --arg body "$MESSAGE" -n '{body: $body}')" \
        --header "authorization: Bearer $GITHUB_TOKEN" \
        --header 'content-type: application/json' \
        "https://api.github.com/repos/apache/hadoop-ozone/issues/$number/comments"

      curl -s -o /dev/null \
        -X PATCH \
        --data '{"state": "close"}' \
        --header "authorization: Bearer $GITHUB_TOKEN" \
        --header 'content-type: application/json' \
        "https://api.github.com/repos/apache/hadoop-ozone/pulls/$number"
done < <(curl -H "Content-Type: application/json" \
     --header "authorization: Bearer $GITHUB_TOKEN" \
     "https://api.github.com/search/issues?q=repo:apache/hadoop-ozone+type:pr+updated:<$(date -d "-21 days" +%Y-%m-%d)+label:pending+is:open" \
     | jq -r '.items[] | (.number,.title)')

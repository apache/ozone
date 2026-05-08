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

# Assembles integration test failure summaries (matching check.yml "Summary of failures")
# and opens a GitHub Discussion via GraphQL. Intended for GitHub Actions only.
#
# Required env: GH_TOKEN, REPOSITORY_NODE_ID, RUN_URL, COMMIT_SHA
#   (REPOSITORY_NODE_ID is github.event.repository.node_id)
# Optional env: ARTIFACT_DOWNLOAD_DIR (default artifact-download),
#               DISCUSSION_BODY_FILE (default discussion_body.md)

set -euo pipefail

# GraphQL ID for the "General" category on github.com/apache/ozone (from
# repository.discussionCategories; change only if GitHub recreates categories).
readonly GENERAL_DISCUSSION_CATEGORY_ID='DIC_kwDODKiyxs4CWMZB'

: "${GH_TOKEN:?GH_TOKEN must be set}"
: "${REPOSITORY_NODE_ID:?REPOSITORY_NODE_ID must be set}"
: "${RUN_URL:?RUN_URL must be set}"
: "${COMMIT_SHA:?COMMIT_SHA must be set}"

ARTIFACT_DOWNLOAD_DIR="${ARTIFACT_DOWNLOAD_DIR:-artifact-download}"
DISCUSSION_BODY_FILE="${DISCUSSION_BODY_FILE:-discussion_body.md}"
SUMMARY_HELPER="${SUMMARY_HELPER:-hadoop-ozone/dev-support/checks/_summary.sh}"

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd -P)"
cd "${ROOT}"

{
  echo "**Workflow run:** ${RUN_URL}"
  echo "**Commit:** ${COMMIT_SHA}"
  echo
  echo 'Flaky test Jiras should be filed as **subtasks of [HDDS-5626](https://issues.apache.org/jira/browse/HDDS-5626)**.'
  echo
  echo ---
  echo
} > "${DISCUSSION_BODY_FILE}"

integration_dirs=()
if [[ -d "${ARTIFACT_DOWNLOAD_DIR}" ]]; then
  mapfile -t integration_dirs < <(find "${ARTIFACT_DOWNLOAD_DIR}" -mindepth 1 -maxdepth 1 -type d -name 'integration-*' | sort)
fi
if [[ ${#integration_dirs[@]} -eq 0 ]]; then
  echo '_No `integration-*` artifacts were downloaded; see the workflow run logs._' >> "${DISCUSSION_BODY_FILE}"
else
  for dir in "${integration_dirs[@]}"; do
    split=$(basename "${dir}")
    {
      echo "## ${split}"
      echo
      if [[ -s "${dir}/summary.md" ]]; then
        cat "${dir}/summary.md"
        echo
      fi
      set +e
      bash "${SUMMARY_HELPER}" "${dir}/summary.txt"
      set -e
      echo
    } >> "${DISCUSSION_BODY_FILE}"
  done
fi

short_sha=$(echo -n "${COMMIT_SHA}" | cut -c1-7)
title="[CI] JUnit failure on master (${short_sha})"

# Construct GraphQL to create the discussion.
create_json=$(jq -n \
  --arg q 'mutation($input: CreateDiscussionInput!) { createDiscussion(input: $input) { discussion { url } } }' \
  --arg rid "${REPOSITORY_NODE_ID}" \
  --arg cid "${GENERAL_DISCUSSION_CATEGORY_ID}" \
  --arg ttl "${title}" \
  --rawfile body "${DISCUSSION_BODY_FILE}" \
  '{query: $q, variables: {input: {repositoryId: $rid, categoryId: $cid, title: $ttl, body: $body}}}')

# Create the discussion and parse the response.
create_resp=$(echo "${create_json}" | gh api graphql --input -)
if echo "${create_resp}" | jq -e '(.errors // []) | length > 0' >/dev/null 2>&1; then
  echo "${create_resp}" | jq .
  exit 1
fi

# Extract the URL of the discussion for logging.
disc_url=$(echo "${create_resp}" | jq -r '.data.createDiscussion.discussion.url // empty')
if [[ -z "${disc_url}" ]]; then
  echo "${create_resp}" | jq .
  exit 1
fi
echo "Created discussion: ${disc_url}"

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

# Generate PR body for configuration documentation update
# Usage: pr_body_config_doc.sh <repo> <workflow_name> <workflow_run_id> <branch_name> <commit_sha> <jira_id>

set -euo pipefail

REPO="${1}"
WORKFLOW_NAME="${2}"
WORKFLOW_RUN_ID="${3}"
BRANCH_NAME="${4}"
COMMIT_SHA="${5}"
JIRA_ID="${6:-}"

cat <<EOF
## What changes were proposed in this pull request?

This is an automated pull request triggered by the [$WORKFLOW_NAME](https://github.com/${REPO}/actions/runs/${WORKFLOW_RUN_ID}) workflow run from [\`${COMMIT_SHA}\`](https://github.com/${REPO}/commit/${COMMIT_SHA}) on [\`${BRANCH_NAME}\`](https://github.com/${REPO}/tree/${BRANCH_NAME}).

The configuration documentation has been automatically regenerated from the latest \`ozone-default.xml\` files.

EOF

if [ -n "${JIRA_ID}" ]; then
  cat <<EOF
## What is the link to the Apache JIRA?

[${JIRA_ID}](https://issues.apache.org/jira/browse/${JIRA_ID})

EOF
fi

cat <<EOF
## How was this patch tested?

This is an auto-generated documentation update. Reviewers should verify:
- The markdown file is properly formatted
- Configuration keys are correctly extracted
- No unexpected changes in existing configurations
EOF

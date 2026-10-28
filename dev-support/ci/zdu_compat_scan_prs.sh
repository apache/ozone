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

# Runs zdu_compat_check.sh on open pull requests and posts a comment with its warnings on each one. A new comment
# is only posted if the pull request was updated since the last comment and the warnings are different from it.
# Old comments are never edited or deleted, even if the warnings no longer apply.
# Called by .github/workflows/zdu-compat-pr.yml.
#
# Environment:
#   GH_TOKEN            token for gh, needs to read and write pull request comments
#   GITHUB_REPOSITORY   repository to scan, set by GitHub Actions
#   RUNNER_TEMP         directory for temporary files, set by GitHub Actions
#   SCAN_ALL            "true" to scan all open pull requests instead of only recently updated ones
#   WINDOW              how far back to look for updated pull requests, as understood by `date -d`

# Fail a pipeline if any command in it fails, so that a failed `gh api` is not hidden by the `jq` after it.
set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# The first line of every comment is this marker, then the head commit it was generated for, then ` -->`.
COMMENT_MARKER='<!-- zdu-compat-check sha='

process_pr() {
  local number="$1"
  local head_sha="$2"

  # The text of the latest comment this script posted on the PR, or empty if there is none.
  # GitHub may return comment text with CRLF line endings, so the carriage returns are removed.
  local latest
  latest=$(gh api --paginate "repos/${GITHUB_REPOSITORY}/issues/${number}/comments" \
      | jq --slurp --raw-output --arg marker "$COMMENT_MARKER" '[.[][] | select(.user.login == "github-actions[bot]"
          and (.body | startswith($marker)))] | last | .body // empty' \
      | tr -d '\r') || return 1

  # The PR comment already contains the sha of the latest commit, meaning it has not been updated since our last comment.
  if [[ "$latest" == "${COMMENT_MARKER}${head_sha} -->"* ]]; then
    echo "#${number}: already commented for this commit"
    return 0
  fi

  # Capture the PR diff in a file.
  local diff_file="${RUNNER_TEMP}/pr-${number}.diff"
  # gh refuses diffs over 20000 lines; such a PR is too large for a heads-up anyway
  if ! gh pr diff "$number" --repo "$GITHUB_REPOSITORY" > "$diff_file"; then
    echo "::warning::#${number}: skipping, cannot get diff" >&2
    return 1
  fi

  # Feed the diff into the check script. Do nothing if it has no warnings.
  local findings
  findings=$("${DIR}/zdu_compat_check.sh" < "$diff_file") || return 1
  if [[ -z "$findings" ]]; then
    echo "#${number}: no findings"
    return 0
  fi

  # Save the comment to a file: the marker for this commit, then the check script output.
  local comment_file="${RUNNER_TEMP}/pr-${number}.md"
  {
    echo "${COMMENT_MARKER}${head_sha} -->"
    echo
    echo "$findings"
  } > "$comment_file"

  # If the PR was updated but the warnings are the same as the last comment, don't add a new comment.
  # The marker line has the commit in it, so it is left out of the comparison along with any other HTML comment lines.
  local old_content=$(grep -v '^<!--' <<< "$latest")
  local new_content=$(grep -v '^<!--' "$comment_file")
  if [[ "$old_content" == "$new_content" ]]; then
    echo "#${number}: findings unchanged since the last comment"
    return 0
  fi

  echo "#${number}: adding comment"
  gh pr comment "$number" --repo "$GITHUB_REPOSITORY" --body-file "$comment_file" || return 1
}

# Collect the list of PRs to scan.
if [[ "$SCAN_ALL" == "true" ]]; then
  search=""
else
  search="updated:>=$(date -u -d "$WINDOW" +%Y-%m-%dT%H:%M:%SZ)"
fi

# Get one PR number and sha per line.
prs=$(gh pr list --repo "$GITHUB_REPOSITORY" --state open --limit 1000 --search "$search" \
    --json number,headRefOid --jq '.[] | "\(.number) \(.headRefOid)"')

# Run the scan on each selected PR.
checked=0
failures=0
while read -r number head_sha; do
  [[ -n "$number" ]] || continue
  checked=$((checked + 1))
  if ! process_pr "$number" "$head_sha"; then
    echo "::warning::Failed to check #${number}" >&2
    failures=$((failures + 1))
  fi
done <<< "$prs"

echo "Checked ${checked} pull requests, ${failures} failed"

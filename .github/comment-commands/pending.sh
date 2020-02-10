#!/usr/bin/env bash
#doc: Add a REQUESTED_CHANGE type review to mark issue non-mergeable: `/pending <reason>`
MESSAGE="Marking this issue as un-mergeable as requested.

Please use \`/ready\` comment when it's resolved.

> $@"

URL="$(jq -r '.issue.pull_request.url' $GITHUB_EVENT_PATH)/reviews"
set +x #GITHUB_TOKEN
curl -s -o /dev/null \
   --data "$(jq --arg body "$MESSAGE" -n '{event: "REQUEST_CHANGES", body: $body}')" \
   --header "authorization: Bearer $GITHUB_TOKEN" \
   --header 'content-type: application/json' \
   $URL

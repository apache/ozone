#!/usr/bin/env bash
#doc: add new label to the issue: `/label <label>`
LABEL="$1"
URL="$(jq -r '.issue.url' $GITHUB_EVENT_PATH)/labels"
curl -s -o /dev/null \
       -X POST \
       --data "$(jq --arg value "$LABEL" -n '{labels: [ $value ]}')" \
       --header "authorization: Bearer $GITHUB_TOKEN" \
       $URL


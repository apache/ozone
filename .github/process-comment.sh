#!/usr/bin/env bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BODY=$(jq -r .comment.body $GITHUB_EVENT_PATH)
LINES=$(printf "$BODY" | wc -l)
if [ "$LINES" == "0" ]; then
   if  [[ "$BODY" == /* ]]; then
      echo "Command $BODY is received"
      COMMAND=$(echo $BODY | awk '{print $1}' | sed 's/\///')
      ARGS=$(echo $BODY | cut -d ' ' -f2-)
      if [ -f "$SCRIPT_DIR/comment-commands/$COMMAND.sh" ]; then
         RESPONSE=$("$SCRIPT_DIR/comment-commands/$COMMAND.sh" $ARGS 2>1)
      else
         RESPONSE="No such command. \`$COMMAND\` $($SCRIPT_DIR/comment-commands/help.sh)"
      fi
      set +x #do not display the GITHUB_TOKEN
      COMMENTS_URL=$(jq -r .issue.comments_url $GITHUB_EVENT_PATH)
      curl -s \
            --data "$(jq --arg body "$RESPONSE" -n '{body: $body}')" \
            --header "authorization: Bearer $GITHUB_TOKEN" \
            --header 'content-type: application/json' \
            $COMMENTS_URL
   fi
fi

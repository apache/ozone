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

# Reads a git diff on stdin and prints one Markdown bullet for each file and reason that may affect
# compatibility with Zero Downtime Upgrade (ZDU), followed by a link to the ZDU developer guide.
# Prints nothing if there is no match.
# These are plain regex matches meant as a heads-up for reviewers: they have false positives and misses.
#
# Usage: git diff BASE HEAD | dev-support/ci/zdu_compat_check.sh

GUIDE_URL='https://ozone.apache.org/docs/next/developer-guide/develop/upgrade-compatibility'

# Files, matched against the path of the changed file.
OM_REQUEST_FILE='^hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/.*\.java$'
CLIENT_TRANSLATOR_FILE='ClientSideTranslatorPB\.java$'
SERVER_TRANSLATOR_FILE='ServerSideTranslatorPB\.java$'
PROTO_FILE='/src/main/proto/.*\.proto$'
PEER_PROTOCOL_FILE='/(OMAdminProtocol|OMInterServiceProtocol|ScmBlockLocationProtocol|StorageContainerLocationProtocol)\.java$'
DB_DEFINITION_FILE='DBDefinition\.java$'
CONFIG_FILE='(/ozone-default\.xml|Config(uration)?\.java)$'
TEST_FILE='/src/test/'

# Added or removed lines, matched against the diff line including its leading + or -.
DB_WRITE_LINE='^[+-].*(addCacheEntry|addCacheEntries|putWithBatch|deleteWithBatch)\('
THROW_LINE='^[+-].*throw new '
# Protolock covers guaranteed incompatible changes: removed or renamed RPCs and fields, changed field numbers, and
# changed types. These checks flag potentially incompatible changes that need manual review: added RPCs, added fields or
# any enum change.
PROTO_RPC_LINE='^\+ *rpc '
PROTO_ENUM_LINE='^[+-] *[A-Za-z_][A-Za-z0-9_]* *= *-?[0-9]+ *(\[.*\])? *;'
PROTO_FIELD_LINE='^\+ *((optional|required|repeated) +)?[A-Za-z_][A-Za-z0-9_.]*(<[^>]*>)? +[A-Za-z_][A-Za-z0-9_]* *= *[0-9]+'

# Matches either `defaultValue` in a Java configuration object, or a `<value>` xml tag in ozone-default.xml.
CONFIG_DEFAULT_LINE='^[+-].*(defaultValue *=|<value>)'

warn() {
  echo "- \`${file}\`: $*"
}

# Every rule applies to every line, so a file is reported once per reason by sort -u below.
while IFS= read -r line; do
  if [[ "$line" == "diff --git "* ]]; then
    # Update the file name we are currently looking at.
    file="${line##* b/}"
  fi
  # If the current section of the diff is not within a file, or within a test file, skip the checks until we reach a
  # file that needs checking.
  if [[ -z "$file" || "$file" =~ $TEST_FILE ]]; then
    continue
  fi

  # Checks for OM Request files
  if [[ "$file" =~ $OM_REQUEST_FILE ]]; then
    warn "OM request changed. If validateAndUpdateCache was modified such that an old and new OM would arrive at" \
      "different DB state when replaying the same transaction, then gate the new behavior with an \`OzoneManagerVersion\`." \
      "If this adds a new request, block it until its feature is finalized with" \
      "\`@DisallowedUntilOmVersion(OzoneManagerVersion.X)\` on its \`preExecute\` method."
    if [[ "$line" =~ $DB_WRITE_LINE ]]; then
      warn "RocksDB write changed in an OM request. Changes to what is persisted must be gated with an \`OzoneManagerVersion\`."
    fi
    if [[ "$line" =~ $THROW_LINE ]]; then
      warn "Exception added or removed in an OM request. OMs at different apparent versions must reach the same" \
        "result when replaying a transaction, so a new failure path must be gated with an \`OzoneManagerVersion\`."
    fi
  fi

  # Checks for client side translator files.
  if [[ "$file" =~ $CLIENT_TRANSLATOR_FILE ]]; then
    warn "Client side translator changed. An internal client that calls a version gated API needs a gate of its own, so it" \
      "does not call the API and fail before the server has finalized it. An external client that calls a version gated" \
      "API must handle a pre-finalized server gracefully."
  fi

  # Checks for server side translator files.
  if [[ "$file" =~ $SERVER_TRANSLATOR_FILE ]]; then
    warn "Server side translator changed. The server must remain backwards compatible with older clients," \
      "and new APIs that modify disk state that could impact downgrade must be version gated."
  fi

  # Checks for Proto files. These only flag additional cases that protolock may not.
  if [[ "$file" =~ $PROTO_FILE ]]; then
    warn "Proto file changed. Protolock alone does not guarantee full ZDU compatibility, so ensure that mixed reader/writer" \
      "scenarios like old client/new server and disk state on downgrade are handled correctly."
    if [[ "$line" =~ $PROTO_RPC_LINE ]]; then
      warn "RPC added. Any new APIs that modify disk state that could impact downgrade must be version gated."
    fi
    if [[ "$line" =~ $PROTO_ENUM_LINE ]]; then
      warn "Enum value added, removed or changed. Removing an enum value is incompatible, so its usage must be changed" \
        "to a no-op. Usage of new enum values must be version gated so they are not written" \
        "until finalization, since old readers have no correct branch for them."
    fi
    if [[ "$line" =~ $PROTO_FIELD_LINE ]]; then
      warn "Field added. In new writer/old reader scenarios, if it is not safe for the reader to ignore the field," \
      "writers must use a version gate before depending on the field's behavior." \
      "In old writer/new reader scenarios, the default value must be safe for the reader to consume."
    fi
  fi

  # Checks for Ratis peer protocol files.
  if [[ "$file" =~ $PEER_PROTOCOL_FILE ]]; then
    warn "Internal OM peer, SCM peer, or OM to SCM protocol changed. Callers must not use a new method until its version is" \
      "finalized, and servers must stay backwards compatible with older clients."
  fi

  # Checks for database definition/schema files.
  if [[ "$file" =~ $DB_DEFINITION_FILE ]]; then
    warn "RocksDB database layout changed. Modified column families and codecs must remain readable by the previous" \
      "version until finalization."
  fi

  # Checks for configuration files.
  if [[ "$file" =~ $CONFIG_FILE && "$line" =~ $CONFIG_DEFAULT_LINE ]]; then
    warn "Configuration default changed. For clusters where this configuration is unset, different mixed version" \
      "components will use different values during an upgrade. If this is not safe, usage of the new default value must be version gated." \
      "Additionally, ensure that any configuration change taking affect on startup does not alter disk state in a way that is incompatible with downgrade. "
  fi
done | sort -u | {
  warnings=$(cat)
  if [[ -n "$warnings" ]]; then
    echo "# This Change May Have Compatibility Issues"
    echo
    echo "Reviewers should manually check the warnings below to ensure the" \
      "change is compatible with zero-downtime upgrade and downgrade."
    echo "See the [ZDU compatibility developer guide](${GUIDE_URL}) for details on handling these changes."
    echo
    echo "$warnings"
  fi
}

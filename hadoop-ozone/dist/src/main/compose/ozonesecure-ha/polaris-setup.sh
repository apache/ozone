#!/usr/bin/env sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

apk add --no-cache jq >/dev/null

realm="${POLARIS_REALM:-POLARIS}"
catalog_name="${POLARIS_CATALOG_NAME:-quickstart_catalog}"
storage_location="${POLARIS_STORAGE_LOCATION:-s3://iceberg-obs/polaris-smoke}"
s3_endpoint="${POLARIS_S3_ENDPOINT:-http://s3g:9878}"
sts_endpoint="${POLARIS_STS_ENDPOINT:-http://s3g:9880/sts}"
role_arn="${POLARIS_ROLE_ARN:-arn:aws:iam::123456789012:role/iceberg-data-all-access-obs}"

if [ -z "${POLARIS_AWS_ACCESS_KEY_ID:-}" ] || [ -z "${POLARIS_AWS_SECRET_ACCESS_KEY:-}" ]; then
  echo "POLARIS_AWS_ACCESS_KEY_ID and POLARIS_AWS_SECRET_ACCESS_KEY must be set"
  exit 1
fi

echo "Waiting for S3 gateway at ${s3_endpoint}..."
attempt=0
while [ "${attempt}" -lt 30 ]; do
  if curl --silent --show-error --include \
      --user "${POLARIS_AWS_ACCESS_KEY_ID}:${POLARIS_AWS_SECRET_ACCESS_KEY}" \
      --aws-sigv4 "aws:amz:us-west-2:s3" \
      "${s3_endpoint}/" >/dev/null 2>&1; then
    echo "${s3_endpoint} is available"
    break
  fi
  attempt=$((attempt + 1))
  sleep 2
done
if [ "${attempt}" -ge 30 ]; then
  echo "Timed out waiting for S3 gateway at ${s3_endpoint}"
  exit 1
fi

echo "Obtaining Polaris OAuth token..."
token="$(
  curl --fail-with-body --silent \
    --user "${CLIENT_ID}:${CLIENT_SECRET}" \
    -H "Polaris-Realm: ${realm}" \
    -d grant_type=client_credentials \
    -d scope=PRINCIPAL_ROLE:ALL \
    "http://polaris:8181/api/catalog/v1/oauth/tokens" \
    | jq -r .access_token
)"
if [ -z "${token}" ] || [ "${token}" = "null" ]; then
  echo "Failed to obtain access token."
  exit 1
fi

storage_config_info="$(
  jq -n \
    --arg endpoint "${s3_endpoint}" \
    --arg endpointInternal "${s3_endpoint}" \
    --arg stsEndpoint "${sts_endpoint}" \
    --arg roleArn "${role_arn}" \
    '{
      storageType: "S3",
      endpoint: $endpoint,
      endpointInternal: $endpointInternal,
      stsEndpoint: $stsEndpoint,
      roleArn: $roleArn,
      stsUnavailable: false,
      pathStyleAccess: true,
      region: "us-west-2"
    }'
)"

payload="$(
  jq -n \
    --arg name "${catalog_name}" \
    --arg location "${storage_location}" \
    --argjson storageConfigInfo "${storage_config_info}" \
    '{
      catalog: {
        name: $name,
        type: "INTERNAL",
        readOnly: false,
        properties: {
          "default-base-location": $location
        },
        storageConfigInfo: $storageConfigInfo
      }
    }'
)"

echo "Creating catalog ${catalog_name} in realm ${realm}..."
curl --fail-with-body --silent \
  -H "Authorization: Bearer ${token}" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: ${realm}" \
  "http://polaris:8181/api/management/v1/catalogs" \
  -d "${payload}"

echo
echo "Granting catalog_admin CATALOG_MANAGE_CONTENT on ${catalog_name}..."
curl --fail-with-body --silent \
  -H "Authorization: Bearer ${token}" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: ${realm}" \
  -X PUT \
  "http://polaris:8181/api/management/v1/catalogs/${catalog_name}/catalog-roles/catalog_admin/grants" \
  -d '{"type":"catalog", "privilege":"CATALOG_MANAGE_CONTENT"}'

echo
echo "Polaris catalog setup complete."

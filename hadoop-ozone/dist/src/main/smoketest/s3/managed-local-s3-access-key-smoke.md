<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Managed Local S3 Access Key Smoke Checklist

This checklist is an interim smoke-test artifact for managed local S3 access
keys. It is intentionally not a Robot test yet because CLI support is not part
of the initial implementation. Managed-key lifecycle operations must be driven
through the OM client-protocol test harness or another admin RPC harness until
the CLI is added in a follow-up.

## Environment

Use a secure OM HA S3G compose environment with managed local S3 access keys
enabled, local JSON policy evaluation disabled, and a KMS key available for
managed secret encryption.

The existing secure HA S3G/haproxy smoke can be used as the closest Docker
baseline. This is a Linux/WSL example; adjust `DOCKER_HOST` for other Docker
installations.

```bash
env DOCKER_HOST=unix:///var/run/docker.sock \
  OZONE_TEST_SELECTOR=ozonesecure-ha/test-haproxy-s3g.sh \
  hadoop-ozone/dev-support/checks/acceptance.sh
```

## Checklist

1. Create two test buckets through the existing Ozone admin path:
   `managed-allowed` and `managed-denied`.
2. Grant Ozone ACLs so effective user `alice` can access only
   `managed-allowed`.
3. Through the OM admin RPC/test harness, create a managed local S3 access key
   with `effectiveUser=alice` and no `policyDocument`.
4. Retrieve the one-time plaintext secret returned by the managed-key create
   flow.
5. Configure AWS CLI or boto3 with the issued managed `accessKeyId` and
   `secretKey`.
6. Use S3 SigV4 through S3G to put and get an object in `managed-allowed`.
7. Verify S3 access to `managed-denied` fails with an access-denied style
   response.
8. Disable the managed access key through the OM admin RPC/test harness.
9. Retry the previously successful S3 request with the disabled key and verify
   it fails closed.
10. Verify the disabled managed key does not fall back to legacy
    `S3SecretManager` credentials.

## Expected Result

The managed key authenticates S3 requests as `alice` while enabled, Ozone ACLs
control object access through the existing authorization path, and disabled or
invalid managed keys fail closed without legacy fallback.

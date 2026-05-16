---
title: Managed Local S3 Access Keys for Ozone S3 Gateway
summary: Design for OM-managed local S3 access keys with expiration and optional local JSON policy authorization.
date: 2026-05-16
status: design
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Managed Local S3 Access Keys for Ozone S3 Gateway

## Scope

This document designs managed local S3 access keys for Apache Ozone S3
Gateway. The feature is intended for legacy S3 and MinIO-compatible clients
that support only static `accessKey` / `secretKey` AWS Signature Version 4
credentials and cannot use OIDC WebIdentity or AWS session tokens.

This design is separate from `HDDS-15273` AssumeRoleWithWebIdentity.

This is a design document only. It does not include implementation code.

## Goals

- Allow Ozone admins to create managed S3 access keys with expiration.
- Support legacy S3 clients using normal AWS SigV4 without
  `AWS_SESSION_TOKEN`.
- Keep OM as the source of truth for credentials and authorization context.
- Keep S3G stateless.
- Preserve existing STS, WebIdentity, and legacy `getsecret` behavior.
- Provide local JSON authorization for managed access keys where configured.
- Fail closed on unsafe or ambiguous configurations.

## Non-Goals

- Do not replace `HDDS-15273` WebIdentity STS.
- Do not replace Kerberos daemon authentication.
- Do not implement OFS or CLI OIDC login.
- Do not implement official Ozone Multi-Tenancy.
- Do not replace Ranger-based multi-tenancy.
- Do not store credentials in S3G.
- Do not make S3G the final authentication or authorization decision point.

## Configuration v1

```text
ozone.s3.accesskey.enabled=false
ozone.s3.accesskey.default.lifetime=90d
ozone.s3.accesskey.max.lifetime=365d
ozone.s3.accesskey.allow.custom.secret=false
ozone.s3.accesskey.secret.min.length=32
ozone.s3.accesskey.insecure.cluster.admin.allowed=false
ozone.s3.accesskey.local.policy.enabled=false
ozone.s3.accesskey.local.policy.max.size=128KB
ozone.s3.accesskey.local.policy.max.statements=50
ozone.s3.accesskey.local.policy.max.actions.per.statement=100
ozone.s3.accesskey.local.policy.max.resources.per.statement=100
```

## Non-Secure Startup Gates

OM must fail fast if:

```text
ozone.security.enabled=false
ozone.s3.accesskey.enabled=true
ozone.s3.accesskey.insecure.cluster.admin.allowed=false
```

OM must also fail fast if:

```text
ozone.security.enabled=false
ozone.s3.accesskey.enabled=true
ozone.s3.accesskey.local.policy.enabled=false
```

Reason:

- In non-secure clusters there is no base Ozone authorization.
- Local JSON is mandatory for managed access-key mode.
- Without local JSON, a managed access key would authenticate the caller but
  would not restrict S3 access.

`ozone.s3.accesskey.insecure.cluster.admin.allowed=true` only permits unsafe
admin lifecycle operations in non-secure clusters. It does not permit
unrestricted managed access keys.

## Architecture

```text
Legacy S3 client
  -> SigV4 request with accessKey/secretKey
  -> S3G parses request into S3Authentication
  -> S3G forwards request to OM
  -> OM resolves credentials
  -> OM validates SigV4
  -> OM resolves effectiveUser/groups
  -> OM applies base Ozone authorization when available
  -> OM applies local JSON policy when enabled
  -> OM executes or denies request
```

S3G does not store credentials and does not make final authentication or
authorization decisions.

## Data Model v1

Create a new OM table:

```text
s3ManagedAccessKeyTable:
  accessKeyId -> S3ManagedAccessKeyInfo
```

`S3ManagedAccessKeyInfo` fields:

```text
accessKeyId
encryptedSecretKey
secretKeyId
effectiveUser
groups
description
createdAt
expiresAt
disabled
createdBy
policyDocument
```

`secretKeyId` identifies the OM/SCM-managed encryption key used to encrypt
`encryptedSecretKey`. Existing rows retain the key reference required to
decrypt them, while new rows can use the current encryption key.

`groups` is resolved by OM from the existing group mapping for `effectiveUser`
at access-key creation time and stored as a snapshot. Groups are not
re-resolved per request in v1. Arbitrary admin-supplied groups are not
supported.

## Access Key and Effective Identity Semantics

`accessKeyId` is credential lookup material only.

`effectiveUser` plus the stored group snapshot is the authorization identity.
Base Ozone authorization and local JSON evaluation must use
`effectiveUser/groups`, not `accessKeyId`.

Audit entries should include both `accessKeyId` and `effectiveUser`.

## Disable and Delete Semantics

- `disable` is the revocation mechanism.
- `delete` physically removes the row from `s3ManagedAccessKeyTable`.
- If the admin's intent is to stop access, they should call `disable`.
- `delete` is cleanup/removal of the managed row, not the primary revocation
  operation.

If a managed access key row exists but is disabled or expired:

```text
fail closed
do not fall through to legacy S3SecretManager
do not fall through to non-secure dummy credential behavior
```

If no managed row exists, fallback depends on cluster mode as defined in the
credential resolution section.

## Credential Resolution

If `x-amz-security-token` is present and non-empty, the request must enter STS
session-token handling. If STS validation fails, the request fails closed. OM
must not reinterpret the request as a managed static access key after failed
STS validation.

Empty or whitespace-only `x-amz-security-token` is rejected as an invalid
token.

### Secure Clusters

In secure clusters, OM resolves credentials as:

```text
1. If x-amz-security-token is present:
     resolve as STS session credentials.

2. Else:
     check s3ManagedAccessKeyTable.

3. Else:
     fall back to legacy S3SecretManager/getsecret.
```

STS session credentials always take precedence when `x-amz-security-token` is
present.

### Non-Secure Clusters With Managed Access Keys Enabled

In non-secure clusters with:

```text
ozone.s3.accesskey.enabled=true
```

OM resolves credentials as:

```text
1. Check s3ManagedAccessKeyTable.

2. If no managed access key exists for accessKeyId:
     fail closed with InvalidAccessKeyId or equivalent.

3. Do not fall back to legacy non-secure S3 behavior that accepts arbitrary
   credentials.
```

This prevents arbitrary `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` values
from bypassing the managed access-key table.

## HA and Read Freshness

Managed access-key credential validation must use leader-fresh or linearizable
metadata.

A stale follower must not accept a disabled, deleted, expired, or rotated key.
If the current OM request path can hit a follower, managed access-key
validation must either be forwarded to the leader or use an existing Ozone
linearizable-read / leader-read mechanism equivalent to the existing S3 secret
or STS validation path.

Disable, delete, and rotate operations must become authoritative for subsequent
managed-key validation.

Required freshness behavior:

```text
after rotate:
  old secret fails even if the request reaches a follower

after disable:
  key fails even if the request reaches a follower

after delete:
  behavior follows the delete/fallback rules, but no stale managed row may be
  used
```

`LocalJsonPolicyEvaluator` must evaluate the policy from the fresh managed key
row, not stale cached policy.

Any per-request managed-key authentication context or thread-local state must
be cleared after request handling, like existing S3/STS auth contexts.

## Namespace Collision Rules

Managed access keys and legacy S3 secrets share the same `accessKeyId`
namespace for SigV4 lookup.

In secure clusters:

```text
managed access-key create:
  reject accessKeyId already present in legacy s3SecretTable / S3SecretManager

legacy secret creation / setsecret / getsecret creation:
  reject accessId already present in s3ManagedAccessKeyTable
```

In non-secure managed-access-key mode:

```text
unknown accessKeyId fails closed
legacy non-secure arbitrary credentials are not accepted
```

Managed access-key create and legacy S3 secret creation / `setsecret` /
`getsecret` creation must use a shared per-`accessKeyId` namespace lock or an
equivalent OM write lock. Collision checks and writes must be atomic relative
to each other.

Collision prevention must account for the configured legacy
`S3SecretStoreProvider`, not only the local `s3SecretTable`. If an external
legacy S3 secret provider cannot answer existence checks reliably, managed
access-key create must fail closed or require an explicit unsupported-provider
warning/gate.

Validation and use of existing legacy S3 secrets remains unchanged.

If a collision exists because of older data or migration:

```text
managed access key resolution wins
OM logs a warning
admin diagnostic path should identify the conflict
```

## Secret Generation

`--secret-key auto` means OM-side generation.

```text
CLI sends create request with secretMode=AUTO
OM preExecute() generates secret with SecureRandom
OM preExecute() encrypts secret
Ratis request contains encrypted secret only
create/rotate response returns plaintext secret once
```

Custom secret is allowed only if:

```text
ozone.s3.accesskey.allow.custom.secret=true
```

Custom secrets must satisfy minimum length and safe character validation.

Plaintext secret is never logged, audited, persisted, or included in
`toString()` or error messages.

## Secret Encryption Key Lifecycle

`encryptedSecretKey` is encrypted by OM before the Ratis-applied request is
created.

`secretKeyId` identifies the encryption key used to encrypt
`encryptedSecretKey`.

The encryption key source must be durable across OM restart and HA failover. It
must work in both secure clusters and explicitly-enabled non-secure
managed-access-key mode.

Failure rules:

```text
encryption key provider unavailable:
  create/rotate fail closed

current encryption key unavailable:
  create/rotate fail closed

existing row references missing or unavailable secretKeyId:
  credential validation fails closed
```

OM must never fall back to plaintext secret storage. OM must never accept
unencrypted managed access-key secrets.

Encryption keys must be retained for at least as long as any
`S3ManagedAccessKeyInfo` row references their `secretKeyId`.

Key rotation semantics:

```text
new create/rotate operations use the current encryption key
existing rows remain decryptable through their stored secretKeyId
old encryption keys cannot be retired while referenced by any non-expired,
  present managed access key
```

`ozone.s3.accesskey.max.lifetime` and encryption-key retention must be
compatible. A managed access key must not outlive the ability to decrypt its
encrypted secret.

Checkpoints, snapshots, RocksDB, and Ratis logs contain only encrypted secret
material and `secretKeyId`, never plaintext secret.

Non-secure mode does not weaken secret encryption. If the required
encryption-key infrastructure is not initialized, OM must fail fast when
`ozone.s3.accesskey.enabled=true`.

## Create and Rotate Plaintext Response Semantics

Create and rotate may return the plaintext secret once in the response.

If OM retry cache replays the same client request, it may return the same
plaintext secret only for the same idempotent request according to existing OM
retry-cache semantics.

Plaintext secret must never be written to Ratis log, RocksDB, checkpoints,
audit logs, `toString()`, or errors.

If retry-cache behavior cannot safely return plaintext, create/rotate retry
must return a clear non-secret response and force the admin to rotate again.

## Admin CLI

Proposed commands:

```bash
ozone s3 accesskey create
ozone s3 accesskey list
ozone s3 accesskey info
ozone s3 accesskey disable
ozone s3 accesskey rotate
ozone s3 accesskey delete
```

Example:

```bash
ozone s3 accesskey create \
  --access-key tomato \
  --secret-key auto \
  --user tomato-s3 \
  --expiry-duration 365d \
  --policy tomato-files-rw.json
```

All lifecycle operations are admin-only in v1, including `list` and `info`.
Users cannot query the status of their own keys in v1. End-user self-service
can be considered in a later version.

## Policy Mutability v1

Immutable fields:

```text
accessKeyId
effectiveUser
groups snapshot
policyDocument
```

`rotate` changes only:

```text
encryptedSecretKey
expiresAt, if explicitly supplied
```

To change policy, create a new key and disable/delete the old key.

## Local JSON Policy Semantics

Existing Ozone authorization remains the base authorization path when
available:

```text
Ranger
Native ACL
```

Local JSON policy is an explicit OM-side authorization step for managed access
keys when enabled.

If:

```text
ozone.s3.accesskey.local.policy.enabled=false
```

then:

```text
policyDocument is rejected on create/rotate
no local JSON policy is applied
```

If:

```text
ozone.s3.accesskey.local.policy.enabled=true
```

then:

```text
every create request must include a valid policyDocument
local JSON policy is enforced
missing policy fails before Ratis replication
```

Secure clusters:

```text
if ozone.s3.accesskey.local.policy.enabled=false:
  final decision = base authorization only

if ozone.s3.accesskey.local.policy.enabled=true:
  final decision = base authorization allows AND local-json allows
```

Non-secure clusters with:

```text
ozone.s3.accesskey.enabled=true
```

require:

```text
ozone.s3.accesskey.local.policy.enabled=true
```

Final decision in this mode:

```text
final decision = local-json allows
```

In non-secure clusters, local JSON is not merely an overlay over
`OmMetadataReader.checkAcls(...)`.

For managed-access-key S3 requests, `LocalJsonPolicyEvaluator` is an explicit
OM-side authorization step.

When:

```text
ozone.security.enabled=false
ozone.s3.accesskey.enabled=true
```

local JSON is the authoritative S3G-scoped authorization check for managed
access-key requests.

This still protects only S3 requests through S3G.

## Local JSON Policy Schema

Supported v1 shape:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::bucket/prefix/*"
    }
  ]
}
```

`Action` may be a non-empty string or non-empty array of strings.
`Resource` may be a non-empty string or non-empty array of strings.

Top-level object must contain only:

```text
Version
Statement
```

`Version` must be exactly `2012-10-17`.

`Statement` must be a non-empty array.

Each statement object must contain only:

```text
Effect
Action
Resource
```

`Effect` must be exactly `Allow` or `Deny`.

Action names must exist in the canonical OM S3 action registry.

Resource strings must match the supported resource grammar.

Default decision is `Deny`. Explicit `Deny` overrides `Allow`.

Unknown fields are rejected.

Unsupported IAM constructs are rejected, not ignored:

```text
Principal
NotPrincipal
Condition
NotAction
NotResource
Sid
```

Empty statement arrays are rejected. Empty action/resource arrays are rejected.

Duplicate or contradictory statements are allowed only if deterministic
Deny-overrides-Allow behavior is preserved.

Policy is parsed, validated, normalized, and canonicalized in OM `preExecute()`
before Ratis replication. `policySha256` is computed over the canonicalized
policy document.

## Policy Validation

Policy documents are parsed, validated, normalized, and bounded in OM
`preExecute()` before Ratis replication.

Bounds:

```text
size <= 128KB
statements <= 50
actions per statement <= 100
resources per statement <= 100
```

These fail before the Ratis-applied request is created:

```text
malformed JSON
unknown actions
invalid resources
missing required policy
bound violations
```

## Canonical Action Registry

The supported S3 action set is defined as a static enum in OM.

Each entry maps:

```text
AWS-style action name, for example s3:GetObject
category: bucket-level or object-level
corresponding Ozone authorizer ACL type
```

Policy parsing rejects action names not present in this enum.

The enum is updated as S3G adds support for new S3 operations.

## Policy Grammar

Supported resource forms:

```text
arn:aws:s3:::<exact-bucket>
arn:aws:s3:::<exact-bucket>/*
arn:aws:s3:::<exact-bucket>/<prefix>*
arn:aws:s3:::<exact-bucket>/<prefix>/*
```

Rejected in v1:

```text
arn:aws:s3:::*
arn:aws:s3:::bucket-*
arn:aws:s3:::*/prefix/*
```

Bucket-name wildcards are not supported in v1.

## Action and Resource Matching

Bucket-level actions match only bucket ARNs:

```text
arn:aws:s3:::bucket
```

Object-level actions match only object/prefix ARNs:

```text
arn:aws:s3:::bucket/*
arn:aws:s3:::bucket/prefix*
arn:aws:s3:::bucket/prefix/*
```

A bucket ARN must not grant object access. An object ARN must not grant bucket
access.

| Category | Actions |
|---|---|
| Bucket-level | `s3:ListBucket`, `s3:GetBucketLocation`, `s3:CreateBucket`, `s3:DeleteBucket`, `s3:GetBucketAcl`, `s3:PutBucketAcl`, `s3:ListBucketMultipartUploads` |
| Object-level | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:AbortMultipartUpload`, `s3:ListMultipartUploadParts`, `s3:CreateMultipartUpload`, `s3:CompleteMultipartUpload`, `s3:GetObjectAcl`, `s3:PutObjectAcl` |

If Ozone S3G action names differ, implementation should use Ozone's exact
names while preserving bucket-vs-object semantics.

## Rotation Semantics

```text
rotate replaces the secret atomically
old secret is invalid immediately
no grace period in v1
rotate returns new plaintext secret once
```

Rotation may set `expiresAt` to any value in:

```text
(now, now + max.lifetime]
```

The max-lifetime window is measured from rotation time, not from `createdAt`.
This permits renewal while keeping any single credential bounded by
`ozone.s3.accesskey.max.lifetime`.

No separate extend operation exists in v1.

## Audit Requirements

These operations must emit OM audit entries:

```text
create
list
info
disable
rotate
delete
```

Audit entries include:

```text
admin identity
operation
target accessKeyId
effectiveUser
expiry if relevant
whether local policy is present
policySha256
```

`policySha256` is computed over the canonicalized policy document. Audit
entries do not include the full policy document by default.

Audit entries must not include:

```text
plaintext secret
encryptedSecretKey
signing material
```

For create/rotate, the plaintext secret is returned only in the response and
must be redacted from logs, audit, `toString()`, and errors.

## Security Boundary and Direct Access Warning

Managed local JSON access-key authorization protects only S3 requests through
S3G.

It does not protect:

```text
OFS
Ozone CLI
OM RPC
SCM
DNs
direct backend access
```

Operators relying on this model must expose only S3G to clients and restrict
backend Ozone endpoints using Kubernetes NetworkPolicy, firewall rules, or
service policy.

This is not official Ozone Multi-Tenancy and does not replace Ranger-based
multi-tenancy.

## Relationship to Existing S3SecretManager

Existing `getsecret` / `S3SecretManager` remains unchanged except for
collision prevention during new legacy secret creation.

Managed access keys are separate because they require:

```text
expiration
disabled state
metadata
encrypted secret material
local JSON policy
admin lifecycle operations
```

## Relationship to HDDS-15273

`HDDS-15273` handles modern clients:

```text
OIDC JWT -> STS temp credentials -> SigV4 + x-amz-security-token
```

Managed access keys handle legacy clients:

```text
accessKey/secretKey -> SigV4 without session token
```

Both use OM-side SigV4 validation plumbing.

STS path must always take precedence when `x-amz-security-token` is present.

`HDDS-15273` WebIdentity behavior must remain unchanged.

## Implementation Plan

1. Phase 1: design doc, config keys, encryption-key lifecycle, local JSON
   schema, HA/follower freshness invariant, and fail-closed startup gates.
2. Phase 2: proto/model/table for `S3ManagedAccessKeyInfo` with
   layout/checkpoint awareness.
3. Phase 3: OM admin lifecycle requests with shared namespace locking:
   create/list/info/disable/rotate/delete.
4. Phase 4: OM-side secret generation, encryption, and redaction.
5. Phase 5: credential resolution, STS precedence, non-secure fail-closed
   behavior, and effective identity.
6. Phase 6: `LocalJsonPolicyEvaluator`.
7. Phase 7: CLI shell commands.
8. Phase 8: docs and E2E tests.

## Likely Implementation Areas

```text
OmClientProtocol.proto
OMDBDefinition
OmMetadataManagerImpl
S3SecurityUtil
OzoneDelegationTokenSecretManager / S3 auth validation path
OzoneManager effective S3 identity helpers
RequestContext if credential policy context needs a field
new S3ManagedAccessKeyInfo model
new managed-access-key OM requests/responses
cli-shell ozone s3 accesskey commands
docs/security S3 docs
```

## Test Plan

1. Feature disabled by default.
2. OM fails startup if `security=false`, `accesskey.enabled=true`, and
   `insecure.cluster.admin.allowed=false`.
3. OM fails startup if `security=false`, `accesskey.enabled=true`, and
   `local.policy.enabled=false`.
4. OM starts in non-secure mode only when unsafe admin mode and local policy
   are both explicitly enabled.
5. `insecure.cluster.admin.allowed=true` does not bypass local-policy
   requirement.
6. Admin-only create/list/info/disable/rotate/delete in secure mode.
7. Non-admin cannot create key.
8. Users cannot list/info their own keys in v1.
9. Create key with OM-generated secret.
10. Create/rotate fails if encryption key provider is unavailable.
11. Create/rotate fails if current encryption key is unavailable.
12. Credential validation fails closed if referenced `secretKeyId` is missing.
13. OM restart preserves ability to validate managed keys.
14. Old encryption key is retained while rows reference it.
15. Plaintext secret is absent from Ratis logs, RocksDB, checkpoints, audit,
    `toString()`, and errors.
16. Create key with custom secret only if enabled.
17. Reject custom secret if disabled.
18. Enforce secret min length.
19. Enforce max lifetime.
20. Create rejects collision with managed table.
21. Create rejects collision with legacy `s3SecretTable`.
22. Legacy secret creation rejects collision with managed table.
23. Concurrent managed create and legacy secret create for the same
    `accessKeyId` cannot both succeed.
24. External/legacy provider collision check path is exercised or explicitly
    gated.
25. Non-secure + managed access keys + local policy rejects unknown
    `accessKeyId`.
26. Non-secure + managed access keys does not fall back to arbitrary legacy
    credentials.
27. Expired managed key denied.
28. Disabled managed key denied.
29. Disabled managed row does not fall through to legacy path.
30. Expired managed row does not fall through to legacy path.
31. Disabled/expired managed row does not fall through to non-secure dummy
    credential behavior.
32. HA/follower request after disable fails.
33. HA/follower request after rotate with old secret fails.
34. HA/follower request after delete does not use stale managed row.
35. Managed auth context is cleared between requests.
36. After delete, the same `accessKeyId` can be created again as a managed key
    because the managed row no longer exists.
37. After delete, if a legacy `getsecret` entry with the same `accessKeyId`
    exists, secure-cluster resolver falls through to legacy `S3SecretManager`
    on subsequent requests.
38. Secure cluster fallback to legacy `S3SecretManager` still works when no
    managed key exists.
39. Wrong secret returns `SignatureDoesNotMatch` or equivalent.
40. Non-empty invalid session token fails and does not fall back to managed key.
41. Session token present with managed `accessKeyId` still uses STS path.
42. `local.policy.enabled=true` requires policy.
43. `local.policy.enabled=false` rejects `policyDocument`.
44. Policy size bound enforced in `preExecute()`.
45. Max statements/actions/resources enforced in `preExecute()`.
46. Malformed JSON policy fails before Ratis-applied request.
47. Unknown top-level policy field rejected.
48. Unknown statement field rejected.
49. `Principal` rejected.
50. `Condition` rejected.
51. `NotAction` rejected.
52. `NotResource` rejected.
53. Invalid `Effect` rejected.
54. Empty `Statement` rejected.
55. Empty `Action` / `Resource` rejected.
56. Unknown action fails before Ratis-applied request.
57. Invalid resource syntax fails before Ratis-applied request.
58. Canonicalized policy hash is stable.
59. Prefix resource patterns work.
60. Bucket wildcard rejected.
61. Bucket ARN does not grant object action.
62. Object ARN does not grant bucket action.
63. Deny overrides allow.
64. Allowed bucket Put/Get/List succeeds with local JSON policy.
65. Denied bucket fails.
66. Local JSON policy runs in non-secure mode for managed-access-key S3
    requests.
67. `accessKeyId != effectiveUser`; authorization uses `effectiveUser/groups`.
68. Groups snapshot semantics.
69. Rotate invalidates old secret immediately.
70. Rotate returns new plaintext secret once.
71. Rotate may set expiry relative to rotation time.
72. Rotate cannot exceed max lifetime.
73. Rotate does not change `policyDocument`.
74. Policy change requires new key in v1.
75. Create/rotate retry behavior does not persist plaintext and does not leak
    the secret.
76. Audit logs include `policySha256`.
77. Audit logs redact plaintext secret.
78. Create/rotate `toString()` and errors do not contain plaintext secret.
79. STS session token path takes precedence in secure mode.
80. Existing `S3SecretManager/getsecret` validation path unchanged.
81. `HDDS-15273` WebIdentity STS path unchanged.
82. S3G does not store credentials.
83. S3G does not make final authorization decisions.

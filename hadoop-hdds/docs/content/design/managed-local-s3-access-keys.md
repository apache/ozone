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
ozone.s3.accesskey.encryption.key.name=
ozone.s3.accesskey.retrieval.handle.ttl=60s
ozone.s3.accesskey.retrieval.handle.max.entries=1024
ozone.s3.accesskey.insecure.cluster.admin.allowed=false
ozone.s3.accesskey.local.policy.enabled=false
ozone.s3.accesskey.local.policy.max.size=128KB
ozone.s3.accesskey.local.policy.max.statements=50
ozone.s3.accesskey.local.policy.max.actions.per.statement=100
ozone.s3.accesskey.local.policy.max.resources.per.statement=100
```

The managed access-key encryption provider is configured through the standard
Hadoop key provider path:

```text
hadoop.security.key.provider.path
```

`ozone.s3.accesskey.encryption.key.name` is empty by default. Empty is legal
only when `ozone.s3.accesskey.enabled=false`.

If `ozone.s3.accesskey.enabled=true`:

```text
ozone.s3.accesskey.encryption.key.name must be non-empty
hadoop.security.key.provider.path must configure a usable KeyProviderCryptoExtension
the provider must be durable, not transient
the configured key must already exist
OM must fail fast if any requirement is missing or unavailable
```

OM must not auto-create the encryption key. Key creation and key-version
retention are operator / KMS-admin responsibilities. A future operator guide may
show an example key name such as `ozone-s3-managed-access-keys`, but that is an
example value, not an implicit default.

`ozone.s3.accesskey.retrieval.handle.ttl` defaults to 60 seconds. It must be
positive and must not exceed 300 seconds.
`ozone.s3.accesskey.retrieval.handle.max.entries` defaults to 1024 and must be
positive. These retrieval-handle limits are inert until create/rotate and
`RetrieveManagedS3AccessKeySecret` are implemented.

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

Non-secure admin identity is not strong. This mode is not recommended for
production admin lifecycle operations. It is intended only for explicitly
opted-in standalone or Kubernetes S3G-only deployments where operators expose
only S3G and restrict backend Ozone endpoints such as OM RPC, SCM, Datanodes,
and administrative APIs.

Explicitly enabled non-secure managed-key mode still requires the same Hadoop
`KeyProviderCryptoExtension` / KMS-style provider. Non-secure mode does not
permit plaintext storage or unencrypted managed secrets. For standalone or
development Kubernetes deployments, a local durable provider such as a
JCEKS-backed Hadoop key provider may be used if configured through
`hadoop.security.key.provider.path` and if it supports the required envelope
operations. If no usable provider exists, OM startup fails when
`ozone.s3.accesskey.enabled=true`.

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

`encryptedSecretKey` is a serialized self-contained encrypted secret envelope
blob, not raw ciphertext. The Phase 2 data model remains unchanged; no new
top-level envelope metadata fields are added in v1.

`secretKeyId` identifies the durable encryption-key version used for that row.
It is diagnostic and quick-reference encryption metadata, not identity or
policy state. `secretKeyId` is not sufficient by itself for decryption; the
envelope blob is authoritative for decryption metadata. Existing rows retain
the key-version reference required to decrypt them, while new rows can use the
current encryption-key version.

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
legacy S3 secret provider can reliably answer existence checks, OM may use
that provider's existence check under the shared namespace lock. If the
provider cannot reliably answer existence checks, managed access-key create
must fail closed or be explicitly unsupported with that provider. Best-effort
collision checking is not allowed.

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
OM preExecute() obtains envelope material from KeyProviderCryptoExtension
OM preExecute() encrypts secret into encryptedSecretKey envelope blob
Ratis request contains encryptedSecretKey, secretKeyId, and non-secret metadata
create/rotate Ratis response returns non-secret metadata plus retrievalHandle
RetrieveManagedS3AccessKeySecret(retrievalHandle) returns plaintext once
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
created. It is a serialized, versioned, self-contained encrypted secret
envelope blob, not raw ciphertext.

`secretKeyId` identifies the encryption-key version used to encrypt
`encryptedSecretKey` for that row. It is top-level metadata for diagnostics and
quick reference. It is not sufficient by itself for decryption; the envelope
blob is authoritative for durable decryption metadata.

Only `encryptedSecretKey` and `secretKeyId` are persisted. Plaintext managed
secrets are never persisted.

The envelope blob must contain all durable metadata needed to decrypt the
managed S3 secret later. At minimum, as applicable to the chosen provider, the
envelope contains:

```text
envelope format version
encryption algorithm
key provider type/name
key name
key version name / secretKeyId
encrypted data encryption key / EDEK or equivalent wrapped key material
EDEK IV or provider-specific IV if required
data IV / nonce
ciphertext
authentication tag if not embedded in ciphertext
AAD / encryption context version
```

Managed access-key encryption uses Ozone/OM's existing Hadoop
`KeyProviderCryptoExtension` / KMS-style provider initialized from
`hadoop.security.key.provider.path`.

The configured encryption key is named by:

```text
ozone.s3.accesskey.encryption.key.name
```

The configured key must already exist. OM must not silently create it.
Key creation is an operator / KMS-admin responsibility.

The implementation must not invent custom crypto and must not depend on the
SCM rotating secret-key service for this feature.

When `ozone.s3.accesskey.enabled=true`, OM startup validation must verify:

```text
ozone.s3.accesskey.encryption.key.name is configured
KeyProviderCryptoExtension can be initialized from hadoop.security.key.provider.path
the provider is durable, not transient
the configured key exists
the current key version is available
the provider supports generating/decrypting encrypted keys or the equivalent
  envelope operation required by the implementation
```

Phase 1b startup validation checks the configured key name, provider
initialization, durable provider status, configured key existence, and current
key version availability. Operation-level envelope support is validated by Phase
3/4 create/rotate before using the provider, because Phase 1b does not execute
envelope operations.

Failure rules:

```text
feature enabled but encryption key name unset:
  OM startup fails closed

encryption key provider unavailable:
  OM startup fails closed when ozone.s3.accesskey.enabled=true, or
  create/rotate fail closed if provider loss is detected at request time

current encryption key unavailable:
  OM startup fails closed when ozone.s3.accesskey.enabled=true
  create/rotate fail closed if provider loss is detected at request time

configured key does not exist:
  OM startup fails closed

existing row references missing or unavailable secretKeyId:
  credential validation fails closed

existing row has missing or invalid envelope metadata:
  credential validation fails closed
```

OM must never fall back to plaintext secret storage. OM must never accept
unencrypted managed access-key secrets.

Encryption keys must be retained for at least as long as any
non-deleted `S3ManagedAccessKeyInfo` row references their `secretKeyId`.
Managed access keys must not outlive the ability to decrypt their encrypted
secret.

Operator requirement:

```text
Do not retire or delete KMS key versions while managed access-key rows
reference them.
```

OM cannot safely validate a managed key if its referenced KMS key version has
been deleted or is unavailable. Validation fails closed.

Key rotation semantics:

```text
create operations use the current encryption-key version
rotate operations may use the current encryption-key version
rotate updates secretKeyId if the current encryption-key version changed
existing rows remain decryptable through their stored secretKeyId
old encryption-key versions cannot be retired while referenced by any
  non-deleted managed access key
```

`ozone.s3.accesskey.max.lifetime` and encryption-key retention must be
compatible. A managed access key must not outlive the ability to decrypt its
encrypted secret.

Checkpoints, snapshots, RocksDB, and Ratis logs contain only the encrypted
secret envelope blob and `secretKeyId`, never plaintext secret.

Non-secure mode does not weaken secret encryption. It still requires the same
`KeyProviderCryptoExtension` / KMS-style provider. If the required
encryption-key infrastructure is not initialized, OM must fail fast when
`ozone.s3.accesskey.enabled=true`.

Create semantics:

```text
OM preExecute() generates plaintext S3 secret in memory
OM obtains encrypted data key / envelope material from KeyProviderCryptoExtension
  for ozone.s3.accesskey.encryption.key.name
OM encrypts plaintext S3 secret into encryptedSecretKey envelope blob
Ratis-applied request contains encryptedSecretKey, secretKeyId, and non-secret
  metadata only
Ratis-applied request must not contain plaintext secret
```

Rotate semantics:

```text
OM generates a new plaintext S3 secret in memory
OM uses the current encryption key version from the configured
  KeyProviderCryptoExtension
OM writes a new encryptedSecretKey envelope blob
OM updates secretKeyId if the KMS key version changed
OM may update expiresAt only if explicitly supplied and bounded by max lifetime
accessKeyId, effectiveUser, groups snapshot, and policyDocument remain immutable
```

`secretKeyId` is encryption metadata, not identity or policy state.

Decrypt / validation semantics:

```text
OM parses encryptedSecretKey envelope blob
OM resolves the required key provider, key, and key version from the envelope
OM decrypts or unwraps as needed using KeyProviderCryptoExtension
missing provider, key, key version, or envelope metadata fails closed
invalid provider, key, key version, or envelope metadata fails closed
no plaintext fallback
no unencrypted managed secrets
no best-effort decryption
```

## Create, Rotate, And Secret Retrieval Semantics

Create and rotate must not return plaintext secret material in the normal
Ratis-applied `OMResponse`. The normal create/rotate Ratis response contains
only non-secret metadata plus a `retrievalHandle`.

The plaintext secret is retrieved through a separate non-Ratis read RPC:

```text
RetrieveManagedS3AccessKeySecret(retrievalHandle)
```

The create/rotate request path is:

```text
OM preExecute() may generate plaintext secret in memory
OM encrypts the secret before creating the Ratis-applied request
Ratis-applied request contains only encrypted envelope blob and metadata
normal create/rotate OMResponse contains non-secret metadata plus retrievalHandle
normal Ratis retry cache replays retrievalHandle, never plaintext
leader stores plaintext in a leader-local in-memory retrieval map
```

The retrieval RPC semantics are:

```text
RetrieveManagedS3AccessKeySecret is leader-routed / LINEARIZABLE_LEADER_ONLY
RetrieveManagedS3AccessKeySecret reads only the leader-local in-memory map
retrievalHandle is single-use
retrievalHandle expires by ozone.s3.accesskey.retrieval.handle.ttl
retrieval map is bounded by ozone.s3.accesskey.retrieval.handle.max.entries
retrievalHandle is bound to caller identity and accessKeyId
```

Unknown, expired, already-consumed, wrong-caller, or failover-lost handles
return `MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE`.

Leader failover loses the plaintext retrieval map. If the handle is lost after
a successful create or rotate, OM must not reveal the old plaintext secret
again. The admin must rotate again if the plaintext response was lost.

Plaintext secret must never be written to Ratis log, RocksDB, checkpoints,
audit logs, server logs, normal `OMResponse`, normal retry-cache objects,
`toString()`, exceptions, or TRACE/debug output.

If existing OM read-RPC routing or retry-cache architecture cannot support this
safely, implementation must stop and seek approval before coding.

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
secretKeyId, if the current encryption-key version changed
expiresAt, if explicitly supplied
```

`secretKeyId` is encryption metadata. It is not identity state and it is not
policy state.

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
rotate may update secretKeyId when the encryption-key version changes
old secret is invalid immediately
no grace period in v1
rotate returns a new retrievalHandle for one-time secret retrieval
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

For create/rotate, the plaintext secret is returned only by the one-time
retrieval RPC and must be redacted from logs, audit, `toString()`, and errors.

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

This warning is especially important in explicitly enabled non-secure mode:
`ozone.s3.accesskey.insecure.cluster.admin.allowed=true` permits unsafe admin
lifecycle operations, but it does not provide strong admin authentication and
does not make backend Ozone endpoints safe to expose.

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
3. Combined Phase 3/4: OM admin lifecycle requests with shared namespace
   locking, OM-side secret generation, encryption, one-time retrieval-handle
   semantics, and redaction: create/list/info/disable/rotate/delete.
4. Phase 5: credential resolution, STS precedence, non-secure fail-closed
   behavior, and effective identity.
5. Phase 6: `LocalJsonPolicyEvaluator`.
6. Phase 7: CLI shell commands.
7. Phase 8: docs and E2E tests.

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
6. OM fails startup if `accesskey.enabled=true` and
   `ozone.s3.accesskey.encryption.key.name` is unset.
7. OM fails startup if `accesskey.enabled=true` and
   `hadoop.security.key.provider.path` is missing or unusable.
8. OM fails startup if the configured KMS key does not exist.
9. OM fails startup if the current KMS key version is unavailable.
10. Non-secure managed-key mode still requires a usable key provider.

Phase 1b config tests also cover retrieval-handle defaults and bounds:
`ozone.s3.accesskey.retrieval.handle.ttl` defaults to 60 seconds and rejects
zero, negative, and values above 300 seconds.
`ozone.s3.accesskey.retrieval.handle.max.entries` defaults to 1024 and rejects
zero or negative values.

11. Non-secure admin lifecycle mode is explicitly unsafe, is not recommended
   for production, and requires backend Ozone endpoints to be restricted.
12. Admin-only create/list/info/disable/rotate/delete in secure mode.
13. Non-admin cannot create key.
14. Users cannot list/info their own keys in v1.
15. Create key with OM-generated secret.
16. Create/rotate fails if encryption key provider is unavailable at request
    time.
17. Create/rotate uses `ozone.s3.accesskey.encryption.key.name`.
18. Create/rotate fails if current encryption key version is unavailable.
19. Rotate updates `secretKeyId` when the current KMS key version changes.
20. Envelope blob round-trip contains enough metadata to decrypt after OM
    restart.
21. Missing envelope metadata fails closed.
22. Missing or invalid envelope metadata fails closed.
23. Credential validation fails closed if referenced `secretKeyId` is missing.
24. Missing key version or key material fails closed.
25. OM restart preserves ability to validate managed keys.
26. Old encryption-key versions are retained while non-deleted rows reference
    them.
27. Plaintext secret is absent from Ratis logs, RocksDB, checkpoints, audit,
    server logs, TRACE/debug output, `toString()`, and errors.
28. Ratis-applied request does not contain plaintext.
29. Normal `OMResponse` and normal retry-cache object do not contain plaintext.
30. Create/rotate response contains retrievalHandle, not plaintext.
31. Normal Ratis retry cache replays retrievalHandle, never plaintext.
32. `RetrieveManagedS3AccessKeySecret(retrievalHandle)` returns plaintext
    once.
33. Retrieval handle is single-use and bound to caller identity and
    `accessKeyId`.
34. Unknown, expired, already-consumed, wrong-caller, or failover-lost
    retrieval handle returns `MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE`.
35. Leader failover loses the plaintext retrieval map and does not reveal the
    old plaintext secret.
36. Create key with custom secret only if enabled.
37. Reject custom secret if disabled.
38. Enforce secret min length.
39. Enforce max lifetime.
40. Rotate may update `secretKeyId` when the current encryption-key version
    changed.
41. Rotate does not change `accessKeyId`, `effectiveUser`, groups snapshot, or
    `policyDocument`.
42. Create rejects collision with managed table.
43. Create rejects collision with legacy `s3SecretTable`.
44. Managed create rejects collision with a reliable external legacy
    `S3SecretStoreProvider` existence check.
45. Managed create fails closed or is unsupported if an external legacy
    provider cannot reliably answer existence checks.
46. Legacy secret creation rejects collision with managed table.
47. Concurrent managed create and legacy secret create for the same
    `accessKeyId` cannot both succeed.
48. External/legacy provider collision check path is exercised or explicitly
    gated.
49. Non-secure + managed access keys + local policy rejects unknown
    `accessKeyId`.
50. Non-secure + managed access keys does not fall back to arbitrary legacy
    credentials.
51. Expired managed key denied.
52. Disabled managed key denied.
53. Disabled managed row does not fall through to legacy path.
54. Expired managed row does not fall through to legacy path.
55. Disabled/expired managed row does not fall through to non-secure dummy
    credential behavior.
56. HA/follower request after disable fails.
57. HA/follower request after rotate with old secret fails.
58. HA/follower request after delete does not use stale managed row.
59. Managed auth context is cleared between requests.
60. After delete, the same `accessKeyId` can be created again as a managed key
    because the managed row no longer exists.
61. After delete, if a legacy `getsecret` entry with the same `accessKeyId`
    exists, secure-cluster resolver falls through to legacy `S3SecretManager`
    on subsequent requests.
62. Secure cluster fallback to legacy `S3SecretManager` still works when no
    managed key exists.
63. Wrong secret returns `SignatureDoesNotMatch` or equivalent.
64. Non-empty invalid session token fails and does not fall back to managed key.
65. Session token present with managed `accessKeyId` still uses STS path.
66. `local.policy.enabled=true` requires policy.
67. `local.policy.enabled=false` rejects `policyDocument`.
68. Policy size bound enforced in `preExecute()`.
69. Max statements/actions/resources enforced in `preExecute()`.
70. Malformed JSON policy fails before Ratis-applied request.
71. Unknown top-level policy field rejected.
72. Unknown statement field rejected.
73. `Principal` rejected.
74. `Condition` rejected.
75. `NotAction` rejected.
76. `NotResource` rejected.
77. Invalid `Effect` rejected.
78. Empty `Statement` rejected.
79. Empty `Action` / `Resource` rejected.
80. Unknown action fails before Ratis-applied request.
81. Invalid resource syntax fails before Ratis-applied request.
82. Canonicalized policy hash is stable.
83. Prefix resource patterns work.
84. Bucket wildcard rejected.
85. Bucket ARN does not grant object action.
86. Object ARN does not grant bucket action.
87. Deny overrides allow.
88. Allowed bucket Put/Get/List succeeds with local JSON policy.
89. Denied bucket fails.
90. Local JSON policy runs in non-secure mode for managed-access-key S3
    requests.
91. `accessKeyId != effectiveUser`; authorization uses `effectiveUser/groups`.
92. Groups snapshot semantics.
93. Rotate invalidates old secret immediately.
94. Rotate returns a new retrievalHandle for one-time secret retrieval.
95. Rotate may set expiry relative to rotation time.
96. Rotate cannot exceed max lifetime.
97. Rotate does not change `policyDocument`.
98. Policy change requires new key in v1.
99. Create/rotate retry behavior persists only retrievalHandle and does not
    leak plaintext.
100. Audit logs include `policySha256`.
101. Audit logs redact plaintext secret.
102. Create/rotate `toString()` and errors do not contain plaintext secret.
103. Create/rotate logs and TRACE/debug output redact plaintext secret.
104. STS session token path takes precedence in secure mode.
105. Existing `S3SecretManager/getsecret` validation path unchanged.
106. `HDDS-15273` WebIdentity STS path unchanged.
107. S3G does not store credentials.
108. S3G does not make final authorization decisions.

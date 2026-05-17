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
  -> OM resolves effectiveUser
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

Phase 5 separates credential lookup, S3 namespace routing, and Ozone
authorization identity. Existing Ozone S3 call sites do not all use the S3
access ID for the same purpose, so `effectiveUser` must not be treated as a
drop-in replacement for the access ID everywhere.

```text
credentialAccessKeyId:
  AWS access key from SigV4
  used for lookup in s3ManagedAccessKeyTable
  used for signature credential scope / credential lookup
  audited as the credential identifier

s3NamespaceAccessId:
  S3 access identity used by existing S3 namespace / volume / bucket-link
  routing where current Ozone code expects an S3 access ID
  v1 managed access keys use:
    s3NamespaceAccessId = accessKeyId
  unless code inspection proves a specific call site requires effectiveUser

effectiveUser:
  Ozone authorization identity from S3ManagedAccessKeyInfo.effectiveUser
  used for OMClientRequest.getUserInfo()
  used for RequestContext authorization user
  used for OmMetadataReader.checkAcls(...)
  used for ACL UGI / authorization effects
  used for owner / user principal effects
  audited as the effective authorization user
```

Base Ozone authorization must use `effectiveUser`, not `accessKeyId`.
S3 namespace / volume / bucket-link routing must continue to use
`s3NamespaceAccessId` where current Ozone code expects an S3 access ID.
Call sites that combine namespace lookup with authorization or user-principal
effects must split those responsibilities rather than applying either identity
globally.

The stored group snapshot is metadata in Phase 5. Existing base authorization
may continue to resolve groups through the existing Ozone/Hadoop/Ranger group
mapping for `effectiveUser`. Phase 5 must not claim stored groups are enforced
by base authorization unless a real `RequestContext` / `UserInfo` propagation
path is implemented.

`LocalJsonPolicyEvaluator` in Phase 6 may use the stored group snapshot.

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

If `x-amz-security-token` is visible to OM and non-empty, the request must
enter STS session-token handling. If STS validation fails, the request fails
closed. OM must not reinterpret the request as a managed static access key
after failed STS validation.

Blank or whitespace-only `x-amz-security-token` values visible to OM are
rejected as invalid tokens. If the current S3G serialization drops a truly
empty session token before OM sees it, Phase 5 treats that as absent under the
existing behavior. Preserving truly empty token presence through S3G is future
hardening and is out of scope for Phase 5.

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

Phase 5 keeps the managed-key S3 data path fail-closed until
`LocalJsonPolicyEvaluator` is implemented in Phase 6.

Reason:

```text
non-secure managed-key mode requires local JSON policy
LocalJsonPolicyEvaluator is Phase 6
```

Therefore, in Phase 5:

```text
managed-key S3 request:
  PERMISSION_DENIED / fail closed

unknown accessKeyId:
  PERMISSION_DENIED / fail closed

legacy arbitrary non-secure credential fallback:
  never allowed when ozone.s3.accesskey.enabled=true
```

Secure clusters with `ozone.s3.accesskey.local.policy.enabled=false` may use
managed-key authentication plus existing base authorization in Phase 5.

If `ozone.s3.accesskey.local.policy.enabled=true`, managed-key S3 data-path
authorization fails closed with `PERMISSION_DENIED` in Phase 5 until
`LocalJsonPolicyEvaluator` exists.

Do not silently ignore `policyDocument`. If a stored managed row has a non-empty
`policyDocument`, Phase 5 managed-key S3 data-path authorization fails closed
with `PERMISSION_DENIED` unless the policy can be evaluated.

## HA and Read Freshness

Managed access-key credential validation is leader-only in Phase 5.

A stale follower must not accept a disabled, deleted, expired, or rotated key.
If `ozone.s3.accesskey.enabled=true` and the S3 request has no non-empty
session token, OM must perform an explicit leader-ready / not-leader check
before managed table lookup. If the request reaches a follower or not-ready OM,
the request must return an existing leader / failover / not-leader style error
so the client can retry against the leader.

Do not authorize managed access keys from stale follower metadata.

This leader-only check applies to managed-key validation. Existing legacy S3
auth behavior remains unchanged when managed access keys are disabled, or when
secure-cluster fallback to legacy validation is reached after a leader-fresh
managed lookup confirms no managed row exists.

Stale follower metadata must not decide:

```text
managed row absent -> legacy fallback
managed row active -> allow
managed row not disabled
old encrypted secret after rotate
```

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
`S3SecretStoreProvider`, not only the local `s3SecretTable`.

For the local/default legacy provider backed by `s3SecretTable`:

```text
managed access-key create:
  check s3ManagedAccessKeyTable
  check legacy s3SecretTable / S3SecretManager

legacy secret creation / setsecret / getsecret creation:
  check s3ManagedAccessKeyTable

all checks and writes:
  use S3_SECRET_LOCK or an equivalent shared per-accessKeyId namespace lock
```

For external or non-local `S3SecretStoreProvider` implementations:

```text
if provider can reliably answer accessKeyId existence:
  use that existence check under the same namespace-lock semantics

if provider cannot reliably answer accessKeyId existence:
  managed access-key create fails closed or is unsupported with that provider
```

No best-effort collision checking is allowed. No silent coexistence is allowed.
Phase 3/4 may initially support managed access-key create only with the
local/default `S3SecretStoreProvider`; if a non-local provider is configured
and cannot prove collision safety, create must fail with
`NOT_SUPPORTED_OPERATION` or an explicit managed access-key
operation-not-supported status.

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
managed S3 secret later. Phase 3/4 must define a concrete v1 serialized
envelope message before request handlers are implemented.

`ManagedS3AccessKeySecretEnvelope` v1:

```text
envelopeVersion: integer, required, currently 1
algorithm: string or enum, required, currently AES/GCM/NoPadding
keyProviderPathHash or keyProviderId: optional diagnostic metadata, non-secret
keyName: string, required, from ozone.s3.accesskey.encryption.key.name
keyVersionName: string, required, also copied to top-level secretKeyId
encryptedDataKey / EDEK: bytes, required, provider-wrapped data encryption key
edekIv: bytes, optional if required by KeyProviderCryptoExtension
dataIv / nonce: bytes, required
ciphertext: bytes, required
authTag: bytes, optional if separate from ciphertext
aadContextVersion: integer, required
createdAt: optional diagnostic timestamp
providerMetadata: optional map only if needed by KeyProviderCryptoExtension
```

The envelope is stored as bytes in
`S3ManagedAccessKeyInfo.encryptedSecretKey`. No new top-level
`S3ManagedAccessKeyInfo` envelope metadata fields are added in v1.
`secretKeyId` remains quick-reference metadata only. The envelope is
authoritative for durable decryption metadata.

AAD is required. The v1 AAD context binds the encrypted secret to immutable row
metadata:

```text
purpose: ozone-managed-s3-access-key
envelopeVersion
accessKeyId
effectiveUser
createdAt
keyName
keyVersionName / secretKeyId
```

The implementation must define a deterministic canonical serialization for the
AAD context, or delegate it to a helper with round-trip and tamper tests.
Changing the canonical AAD serialization requires a new `aadContextVersion`.

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

## Hadoop KeyProviderCryptoExtension Envelope Flow

When using Hadoop `KeyProviderCryptoExtension` / KMS-style provider,
create/rotate use envelope encryption as follows:

1. OM `preExecute()` generates the managed S3 secret plaintext in memory.
2. OM calls:

   ```java
   KeyProviderCryptoExtension.generateEncryptedKey(
       ozone.s3.accesskey.encryption.key.name)
   ```

   to obtain an `EncryptedKeyVersion` / EDEK for the configured key.
3. OM calls:

   ```java
   KeyProviderCryptoExtension.decryptEncryptedKey(edek)
   ```

   to recover plaintext data-encryption-key material in memory.
4. OM uses the plaintext DEK to AES/GCM-encrypt the managed S3 secret.
5. OM builds the serialized `encryptedSecretKey` envelope blob.
6. The Ratis-applied request contains only:

   ```text
   encryptedSecretKey envelope blob
   secretKeyId
   non-secret metadata
   ```

7. The Ratis-applied request must not contain:

   ```text
   managed S3 secret plaintext
   plaintext DEK
   unwrapped key material
   ```

8. OM zeroizes or otherwise clears plaintext S3 secret material and plaintext
   DEK material in a `finally` block as soon as the envelope is built.
9. `validateAndUpdateCache()` must not call KMS, RNG,
   `generateEncryptedKey(...)`, or `decryptEncryptedKey(...)`. It only applies
   already-encrypted metadata from the Ratis-applied request.

Required KMS permissions:

```text
create/rotate:
  require permission to generate or wrap encrypted key material:
    generateEncryptedKey(...)

  require permission to decrypt or unwrap the EDEK to plaintext DEK:
    decryptEncryptedKey(...)

reason:
  OM must encrypt the managed S3 secret before Ratis submission. With the
  KeyProviderCryptoExtension envelope pattern, generateEncryptedKey(...)
  produces an EDEK, but OM still needs plaintext DEK material in memory to
  AES/GCM-encrypt the managed secret.

validation/decryption:
  require permission to decrypt or unwrap the EDEK referenced by the envelope
    decryptEncryptedKey(...)

permission denied or operation unavailable:
  fail closed

either create/rotate operation unavailable or denied:
  create/rotate fail closed before Ratis submission
```

The serialized `encryptedSecretKey` envelope blob must include enough durable
metadata to decrypt after restart/failover:

```text
envelopeVersion
algorithm, for example AES/GCM/NoPadding
keyName
keyVersionName / secretKeyId
serialized EDEK / encrypted data key material
data IV / nonce
ciphertext
auth tag, if not embedded in ciphertext
AAD context version
provider metadata, if required by KeyProviderCryptoExtension
```

The envelope must not include plaintext DEK.

`secretKeyId` remains diagnostic / quick-reference metadata. The envelope is
authoritative for decryption metadata.

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

malformed envelope:
  credential validation fails closed

unsupported envelope version:
  credential validation fails closed

missing or unsupported algorithm:
  credential validation fails closed

missing keyName or keyVersionName:
  credential validation fails closed

missing encryptedDataKey / EDEK:
  credential validation fails closed

missing data nonce / IV:
  credential validation fails closed

missing ciphertext:
  credential validation fails closed

authentication tag failure:
  credential validation fails closed

KMS permission failure:
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
1. OM parses encryptedSecretKey envelope.
2. OM extracts EDEK and key metadata.
3. OM calls decryptEncryptedKey(...) to unwrap the DEK.
4. OM decrypts the managed S3 secret in memory.
5. OM validates SigV4.
6. OM clears plaintext secret and plaintext DEK material as soon as possible.

provider unavailable:
  fail closed

key name missing:
  fail closed

key version missing:
  fail closed

EDEK missing or malformed:
  fail closed

decryptEncryptedKey(...) fails:
  fail closed

AES/GCM authentication fails:
  fail closed

envelope version unsupported:
  fail closed

algorithm unsupported:
  fail closed

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

`RetrieveManagedS3AccessKeySecret` uses a command-specific server path. It must
not use the generic `LINEARIZABLE_LEADER_ONLY` read path if that path can route
through Ratis read handling or fall back based on server config.

The selected HA semantics are Option A:

```text
non-leader or not-ready leader:
  return the existing OM leader / failover error
  client may retry against the leader

leader that owns the in-memory retrieval map:
  perform the command-specific direct read handling

leader that does not have the handle:
  fail closed with MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE
```

This matches existing OM HA client conventions while ensuring plaintext is
never returned from a follower or from a generic read path.

The retrieve command-specific path must:

```text
perform an explicit leader-ready check
run only on the leader that owns the in-memory retrieval map
call the direct request handler path, such as handler.handleReadRequest, after
  the leader-ready check
never call omExecutionFlow.submit(..., false)
never enter the Ratis write path
never enter the Ratis retry-cache path
never put plaintext into normal OMResponse or normal Ratis response objects
```

The retrieval RPC semantics are:

```text
RetrieveManagedS3AccessKeySecret reads only the leader-local in-memory map
retrievalHandle is single-use
retrievalHandle expires by ozone.s3.accesskey.retrieval.handle.ttl
retrieval map is bounded by ozone.s3.accesskey.retrieval.handle.max.entries
retrievalHandle is bound to caller identity and accessKeyId
```

Handle-specific failures all return
`MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE`:

```text
unknown handle
expired handle
already-consumed handle
wrong caller
accessKeyId mismatch
failover-lost handle
leader-local map missing entry
```

The leader-local retrieval map semantics are:

```text
map location:
  leader-local, in-memory only

successful retrieval:
  atomically remove the entry before returning plaintext

expiration:
  entries expire after ozone.s3.accesskey.retrieval.handle.ttl

capacity:
  max entries is ozone.s3.accesskey.retrieval.handle.max.entries

create/rotate before storing a new entry:
  purge expired entries
  if still full, fail before Ratis submission

commit rule:
  do not commit managed key create/rotate if OM cannot store the retrieval
  handle entry needed for the admin to retrieve the generated secret

handle entropy:
  generated with SecureRandom
  at least 128 bits of entropy

handle binding:
  caller identity
  accessKeyId
  operation type create/rotate
  creation timestamp and expiry

Ratis submission failure after pending handle creation:
  remove the pending handle if possible
  TTL expiry is fallback cleanup
```

Leader failover loses the plaintext retrieval map. If the handle is lost after
a successful create or rotate, OM must not reveal the old plaintext secret
again. The admin must rotate again if the plaintext response was lost.

Plaintext secret must never be written to Ratis log, RocksDB, checkpoints,
audit logs, server logs, normal `OMResponse`, normal retry-cache objects,
`toString()`, exceptions, or TRACE/debug output.

If existing OM read-RPC routing or retry-cache architecture cannot support this
safely, implementation must stop and seek approval before coding.

## Redaction Requirements

The following values are sensitive and must be redacted from logs, audit,
`toString()`, errors, debug output, and TRACE output:

```text
plaintext managed S3 secret
custom plaintext secret in CreateManagedS3AccessKeyRequest
custom plaintext secret in RotateManagedS3AccessKeyRequest
RetrieveManagedS3AccessKeySecretResponse plaintextSecret
encryptedSecretKey envelope blob
```

`retrievalHandle` is a short-lived bearer handle. It must not be logged in
full. Audit may include only a hash or short prefix if needed for diagnostics.

`policyDocument` must not be logged by default. Audit uses `policySha256`
computed over the canonical policy document.

`OMPBHelper.processForDebug` or the equivalent debug/TRACE preprocessing must
redact:

```text
CreateManagedS3AccessKeyRequest custom plaintext secret
RotateManagedS3AccessKeyRequest custom plaintext secret
RetrieveManagedS3AccessKeySecretResponse plaintextSecret
full retrievalHandle
encryptedSecretKey envelope blob
policyDocument
```

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
4. Phase 5: OM-side managed-key S3 credential resolution, STS precedence,
   leader-fresh lookup, byte-array SigV4 validation, secure-cluster legacy
   fallback, non-secure fail-closed behavior, and separated credential /
   namespace / authorization identity.
5. Phase 6: `LocalJsonPolicyEvaluator`.
6. Phase 7: CLI shell commands.
7. Phase 8: docs and E2E tests.

## Phase 5 Credential Data-Path Plan

Phase 5 adds OM-side managed access-key authentication for normal S3 requests.
It does not implement local JSON policy evaluation, CLI commands, E2E tests,
admin lifecycle changes, retrieval-handle changes, or HDDS-15273 / WebIdentity
/ STS behavior changes. It also does not change S3G runtime serialization
behavior; preserving truly empty session-token presence is future hardening.

### Phase 5 Insertion Point

Phase 5 inserts a `ManagedAccessKeyAuthenticator` in
`S3SecurityUtil.validateS3Credential(...)`:

```text
1. If a session token is visible to OM:
     blank / whitespace-only token fails as invalid
     truly empty token dropped by current S3G serialization is treated as absent
       under existing behavior

2. If a non-empty x-amz-security-token is present:
     run the existing STS path
     STS success returns
     STS failure fails closed
     do not fall back to managed access key

3. Else, if ozone.s3.accesskey.enabled=true:
     perform explicit leader-ready / not-leader check
     perform managed-key lookup only on the leader
     this branch runs even when ozoneManager.isSecurityEnabled() is false

4. If a managed row exists:
     authenticate using the managed row
     row-state, policy, KMS, envelope, decrypt, or signature failure is terminal
     do not fall through to legacy S3SecretManager

5. If no managed row exists:
     secure cluster -> existing legacy S3SecretManager/getsecret fallback
     non-secure managed-key mode -> PERMISSION_DENIED / fail closed
```

If the feature is disabled, existing S3/STS/legacy behavior is unchanged.
When the feature is enabled, Phase 5 must not return early only because
`ozoneManager.isSecurityEnabled()` is false; non-secure managed mode must fail
closed before any non-secure "accept arbitrary credentials" behavior.

### Phase 5 Managed Row Checks

For a found managed row, Phase 5 must:

```text
require MANAGED_LOCAL_S3_ACCESS_KEYS layout feature to be available
reject disabled rows
reject expired rows
reject rows with non-empty policyDocument until Phase 6
reject local.policy.enabled=true until Phase 6
decrypt encryptedSecretKey with ManagedS3AccessKeySecretEnvelopeCodec.decrypt
validate SigV4 using the decrypted managed secret
install an OM-local managed request identity context
clear the managed request identity context in finally
clear plaintext secret bytes in finally
```

KMS provider failure, missing key material, malformed envelope, tampered
envelope, unsupported envelope version, decrypt failure, or GCM authentication
failure must fail closed. These failures must not fall back to legacy
`S3SecretManager`.

In Phase 5, non-secure clusters with `ozone.s3.accesskey.enabled=true`, rows
with non-empty `policyDocument`, and clusters with
`ozone.s3.accesskey.local.policy.enabled=true` all fail closed with
`OMException.ResultCodes.PERMISSION_DENIED` because `LocalJsonPolicyEvaluator`
is not implemented until Phase 6.

### Phase 5 Leader-Only Freshness Requirement

Managed-aware S3 credential resolution is leader-only in Phase 5.

When `ozone.s3.accesskey.enabled=true` and the S3 request has no non-empty
session token, `S3SecurityUtil.validateS3Credential(...)` or the managed
authenticator must perform an explicit leader-ready / not-leader check before
any managed table lookup. The check must run before deciding managed-row
absence, managed-row state, managed authentication success, or secure-cluster
legacy fallback.

If the request reaches a follower or not-ready OM, the request must fail with an
existing OM leader / failover / not-leader style error and be retried against
the leader.

When `ozone.s3.accesskey.enabled=false`, Phase 5 does not add this
managed-key leader-only check; existing S3/STS/legacy behavior is unchanged.
When secure-cluster fallback to legacy `S3SecretManager` is reached after a
leader-fresh managed lookup confirms no managed row exists, the existing legacy
behavior remains unchanged.

Phase 5 must not allow stale follower metadata to:

```text
decide managed row absence and fall back to legacy
accept an active-looking row after disable or delete
accept an old encrypted secret after rotate
ignore expiration
```

### Phase 5 Managed Identity Context

Phase 5 adds an OM-local managed identity context named
`ManagedS3AccessKeyAuthContext`.

The context contains:

```text
credentialAccessKeyId
s3NamespaceAccessId
effectiveUser
groupsSnapshot
policySha256 or policyDocumentPresent
credentialType = MANAGED_S3_ACCESS_KEY
```

Contract:

```text
credentialAccessKeyId:
  AWS access key from SigV4
  used for lookup in s3ManagedAccessKeyTable
  used for signature credential scope / credential lookup
  audited as the credential identifier

s3NamespaceAccessId:
  S3 access identity used by existing S3 namespace / volume / bucket-link
  routing where current Ozone code expects an S3 access ID
  v1 managed access keys use s3NamespaceAccessId = accessKeyId unless code
  inspection proves a specific call site requires effectiveUser
  v1 managed access keys are not tenant-bound; a managed access-key ID should
  not be treated as a tenant access ID unless a later phase explicitly adds
  that binding

effectiveUser:
  Ozone authorization identity from S3ManagedAccessKeyInfo.effectiveUser
  used for OMClientRequest.getUserInfo()
  used for RequestContext authorization user
  used for OmMetadataReader.checkAcls(...)
  used for ACL UGI / authorization effects
  used for owner / user principal effects
  audited as the effective authorization user

groupsSnapshot:
  stored metadata for audit and future Phase 6 LocalJsonPolicyEvaluator

credentialType:
  distinguishes managed access-key auth from legacy S3 secret and STS auth
```

Phase 5 base authorization and request user construction must use
`effectiveUser`, not `credentialAccessKeyId` or `s3NamespaceAccessId`.
`effectiveUser` is an Ozone principal / base-authorization identity; it is not
automatically an S3 access ID.
For managed access keys, `effectiveUser` is used directly as the Ozone
principal for base authorization unless the implementation has an explicit,
reviewed reason to apply `accessIdToUserPrincipal(...)`.

Phase 5 must not blindly change `OzoneManager.getS3AuthEffectiveAccessId()` to
return `effectiveUser` for all managed-key paths, because existing consumers may
use that value for S3 namespace routing. If the existing helper is ambiguous,
Phase 5 should add explicit OM-internal helpers or equivalent separation, for
example:

```text
getS3AuthCredentialAccessId()
getS3AuthNamespaceAccessId()
getS3AuthEffectiveUser()
getS3AuthAuthorizationPrincipal()
getS3AuthClientUserPrincipal()
```

Required call-site semantics:

```text
OMClientRequest.getUserInfo():
  uses effectiveUser

OmMetadataReader.checkAcls(...):
  uses effectiveUser as the authorization identity

OzoneManager.getS3VolumeContext(...):
  namespace / tenant / S3 access lookup uses s3NamespaceAccessId
  returned S3VolumeContext.userPrincipal uses effectiveUser
  owner identity uses effectiveUser
  KMS / user principal effects use effectiveUser
  ACL UGI, RequestContext, and authorization effects use effectiveUser
  do not use effectiveUser for S3 namespace lookup by assumption
  do not use accessKeyId for authorization by assumption

OzoneManager.resolveBucketLink(...):
  bucket-link namespace resolution uses existing S3 namespace semantics /
    s3NamespaceAccessId where the current code expects an S3 access ID
  bucket names and requested namespace are not rewritten to effectiveUser
  bucket-link ACL checks use effectiveUser
  UGI, RequestContext, owner / user effects, and authorization decisions use
    effectiveUser
```

Audit records both `credentialAccessKeyId` and `effectiveUser`.

The stored group snapshot is metadata in Phase 5. Existing base authorization
may continue to use the existing group mapping for `effectiveUser`. Phase 5
must not claim stored groups are enforced by base authorization unless a real
group propagation path is added. The stored group snapshot is available for
audit and for Phase 6 local JSON policy evaluation.

Required consumers / integration points:

```text
OzoneManager.getS3AuthEffectiveAccessId() or explicit replacements for
  credential access ID, namespace access ID, and effective user
OMClientRequest.getUserInfo()
OmMetadataReader.checkAcls(...)
OzoneManager.getS3VolumeContext(...)
OzoneManager.resolveBucketLink(...)
```

`ManagedS3AccessKeyAuthContext` must be cleared in a `finally` block after each
request, alongside existing S3 and STS request context cleanup.

### Phase 5 Byte-Array SigV4 Validation

The managed path must not convert the decrypted managed secret to immutable
`String`.

Phase 5 must add or use a byte-array SigV4 validation path:

```text
legacy String-based AWSV4AuthValidator path:
  unchanged

managed path:
  accepts byte[] or equivalent mutable secret representation
  derives HMAC/signing keys from byte arrays
  does not reuse any AWSV4AuthValidator debug logging that logs derived
    signing material, canonical secret material, or plaintext-dependent
    intermediate values
  does not log derived signing material
  does not log plaintext secret
  clears plaintext managed secret bytes in finally
  clears intermediate HMAC/signing-key byte arrays where practical
  acknowledges standard JCA limitations such as SecretKeySpec defensive copies
    that are not directly zeroable
```

Plaintext managed secrets must not appear in logs, audit, `toString()`,
exceptions, TRACE/debug output, normal `OMResponse`, retry cache, Ratis WAL,
RocksDB, or checkpoints.

If `AWSV4AuthValidator` cannot be safely extended without broad behavior
change, Phase 5 implementation must stop and report the blocker.

### Phase 5 Package and API Boundary

`ManagedAccessKeyAuthenticator` should live in the same OM-side S3
security/auth package as `S3SecurityUtil`, or the closest existing OM-side S3
authentication package if the implementation proves that is cleaner.

It should reuse the existing Phase 3/4
`ManagedS3AccessKeySecretEnvelopeCodec.decrypt(...)` helper. If current
visibility is insufficient, expose only a minimal public decrypt-to-bytes helper
on the existing codec, for example `decryptToBytes(...)`. The decrypt API must
return a mutable `byte[]` or equivalent clearable secret representation. Do not
move broad helper packages, duplicate KMS/envelope logic, or introduce package
churn beyond what Phase 5 needs.

### Phase 5 S3 Error Semantics

Prefer existing S3-compatible mappings and avoid exposing precise managed-key
state to clients.

External data-path behavior:

```text
non-empty x-amz-security-token:
  STS path wins
  STS failure fails closed
  no managed fallback

blank or whitespace-only session token visible to OM:
  OMException.ResultCodes.INVALID_TOKEN through the existing STS /
    session-token failure path
  no managed fallback
  no legacy fallback

truly empty session token dropped by current S3G serialization:
  treat as absent under existing behavior
  do not change S3G serialization in Phase 5

managed row not found:
  secure cluster -> legacy S3SecretManager fallback
  non-secure managed mode -> PERMISSION_DENIED

managed row disabled:
  PERMISSION_DENIED
  no legacy fallback

managed row expired:
  PERMISSION_DENIED
  no legacy fallback

KMS / provider / envelope / decrypt / malformed envelope failure:
  PERMISSION_DENIED
  no fallback

wrong secret / SigV4 mismatch:
  PERMISSION_DENIED

local.policy.enabled=true and evaluator absent:
  PERMISSION_DENIED

stored non-empty policyDocument and evaluator absent:
  PERMISSION_DENIED

non-secure managed data-path in Phase 5:
  PERMISSION_DENIED
```

Concrete Phase 5 OM result-code planning:

```text
leader / failover / not-ready:
  use the existing OM leader / failover / not-leader exception path

blank or whitespace-only session token visible to OM:
  OMException.ResultCodes.INVALID_TOKEN through the existing STS /
    session-token failure path
  no managed fallback
  no legacy fallback

truly empty session token dropped by current S3G serialization:
  treated as absent under existing behavior

managed row absent in non-secure mode:
  OMException.ResultCodes.PERMISSION_DENIED

managed row disabled / expired:
  OMException.ResultCodes.PERMISSION_DENIED

KMS / provider / envelope / decrypt / malformed envelope failure:
  OMException.ResultCodes.PERMISSION_DENIED

wrong secret / SigV4 mismatch:
  OMException.ResultCodes.PERMISSION_DENIED

local.policy.enabled=true or policyDocument present before Phase 6:
  OMException.ResultCodes.PERMISSION_DENIED

non-secure managed data-path in Phase 5:
  OMException.ResultCodes.PERMISSION_DENIED
```

Internal audit and logs may record a more precise reason, but they must not
log plaintext secrets, full encrypted envelope blobs, full retrieval handles,
or signing material. External S3 responses must not reveal whether the managed
row was absent, disabled, expired, malformed, undecryptable, or present with an
incorrect secret.

Phase 5 must not use `INVALID_TOKEN` for managed-key data-path failures unless
implementation proves it maps to safe S3 403 behavior across normal S3 endpoint
families, including streaming PUT and multipart paths. The default Phase 5
terminal managed-auth failure result is `PERMISSION_DENIED`.
For managed-key data-path requests, all terminal managed-auth failures use
`PERMISSION_DENIED` unless the request is in the STS-token branch, where
existing STS behavior applies.

### Phase 5 Implementation Scope

Allowed after the Phase 5 planning review passes:

```text
OM-side ManagedAccessKeyAuthenticator
insert in S3SecurityUtil.validateS3Credential(...) after non-empty STS token
  handling and before legacy S3SecretManager fallback
enforce STS precedence
enforce leader-only managed-key lookup
decrypt with ManagedS3AccessKeySecretEnvelopeCodec.decrypt(...)
validate SigV4 with byte-array secret path
do not reuse AWSV4AuthValidator debug logging of derived signing material on
  the managed byte-array path
resolve managed identity context:
  credentialAccessKeyId
  s3NamespaceAccessId
  effectiveUser
  groupsSnapshot
ensure authorization identity uses effectiveUser
ensure namespace routing uses s3NamespaceAccessId where required
clear managed identity context in finally
enforce disabled / expiresAt fail-closed
fail closed on KMS / provider / envelope / decrypt errors
preserve legacy fallback only when no managed row exists in secure cluster
keep non-secure managed data-path fail-closed until Phase 6
keep local.policy.enabled=true fail-closed until Phase 6
```

Out of scope for Phase 5:

```text
LocalJsonPolicyEvaluator
JSON policy parsing / evaluation
CLI
E2E tests
admin lifecycle changes
retrieval handle changes
HDDS-15273 / WebIdentity changes
STS behavior changes except preserving precedence
S3G runtime serialization changes
```

## Phase 3/4 Proto And API Plan

Phase 3/4 must define new managed access-key proto messages before any
create/rotate handler code is written:

```text
CreateManagedS3AccessKeyRequest
CreateManagedS3AccessKeyResponse
UpdateCreateManagedS3AccessKeyRequest
ListManagedS3AccessKeysRequest
ListManagedS3AccessKeysResponse
InfoManagedS3AccessKeyRequest
InfoManagedS3AccessKeyResponse
DisableManagedS3AccessKeyRequest
DisableManagedS3AccessKeyResponse
RotateManagedS3AccessKeyRequest
RotateManagedS3AccessKeyResponse
UpdateRotateManagedS3AccessKeyRequest
DeleteManagedS3AccessKeyRequest
DeleteManagedS3AccessKeyResponse
RetrieveManagedS3AccessKeySecretRequest
RetrieveManagedS3AccessKeySecretResponse
ManagedS3AccessKeySecretEnvelope
```

The proto/API rules are:

```text
Create/rotate external request:
  may contain plaintext only before preExecute

UpdateCreate/UpdateRotate Ratis request:
  contains encrypted envelope only
  contains secretKeyId and non-secret metadata
  never contains plaintext

Create/rotate normal response:
  contains non-secret metadata plus retrievalHandle
  never contains plaintext

RetrieveManagedS3AccessKeySecretResponse:
  only response allowed to contain plaintext
  must not be Ratis-submitted
  must use the command-specific direct leader-only path
```

New status/error planning:

```text
MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE
S3_ACCESS_KEY_ALREADY_EXISTS or equivalent
MANAGED_S3_ACCESS_KEY_NOT_FOUND or equivalent
MANAGED_S3_ACCESS_KEY_DISABLED or equivalent invalid-key status
MANAGED_S3_ACCESS_KEY_EXPIRED or equivalent invalid-key status
MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED, if needed for unsupported
  external providers
```

Status additions must be append-only and proto-lock compatible. Matching
`OMException.ResultCodes` additions must preserve the existing ordinal mapping
between `OMException.ResultCodes` and proto `Status`.

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
49. Phase 5 non-secure + managed access keys returns `PERMISSION_DENIED` until
    `LocalJsonPolicyEvaluator` exists.
50. Non-secure + managed access keys does not fall back to arbitrary legacy
    credentials.
51. Expired managed key denied.
52. Disabled managed key denied.
53. Disabled managed row does not fall through to legacy path.
54. Expired managed row does not fall through to legacy path.
55. Disabled/expired managed row does not fall through to non-secure dummy
    credential behavior.
56. Managed auth on a follower returns a leader / failover / not-leader style
    error before managed table lookup.
57. HA/follower request after disable does not authorize from stale metadata.
58. HA/follower request after rotate with old secret does not authorize from
    stale metadata.
59. HA/follower request after delete does not use a stale managed row or decide
    legacy fallback from stale metadata.
60. Managed auth context is cleared between requests.
61. After delete, the same `accessKeyId` can be created again as a managed key
    because the managed row no longer exists.
62. After delete, if a legacy `getsecret` entry with the same `accessKeyId`
    exists, secure-cluster resolver falls through to legacy `S3SecretManager`
    on subsequent requests.
63. Secure cluster fallback to legacy `S3SecretManager` still works when no
    managed key exists.
64. Wrong managed secret returns `PERMISSION_DENIED`. It does not fall back to
    legacy.
65. Non-empty invalid session token fails and does not fall back to managed key.
66. Session token present with managed `accessKeyId` still uses STS path.
67. `local.policy.enabled=true` requires policy.
68. `local.policy.enabled=false` rejects `policyDocument`.
69. Policy size bound enforced in `preExecute()`.
70. Max statements/actions/resources enforced in `preExecute()`.
71. Malformed JSON policy fails before Ratis-applied request.
72. Unknown top-level policy field rejected.
73. Unknown statement field rejected.
74. `Principal` rejected.
75. `Condition` rejected.
76. `NotAction` rejected.
77. `NotResource` rejected.
78. Invalid `Effect` rejected.
79. Empty `Statement` rejected.
80. Empty `Action` / `Resource` rejected.
81. Unknown action fails before Ratis-applied request.
82. Invalid resource syntax fails before Ratis-applied request.
83. Canonicalized policy hash is stable.
84. Prefix resource patterns work.
85. Bucket wildcard rejected.
86. Bucket ARN does not grant object action.
87. Object ARN does not grant bucket action.
88. Deny overrides allow.
89. Allowed bucket Put/Get/List succeeds with local JSON policy.
90. Denied bucket fails.
91. Local JSON policy runs in non-secure mode for managed-access-key S3
    requests.
92. `accessKeyId != effectiveUser`; Phase 5 credential lookup uses
    `credentialAccessKeyId`, namespace routing uses `s3NamespaceAccessId`, and
    authorization identity uses `effectiveUser`.
93. Stored groups are metadata in Phase 5 and may be used by Phase 6 local JSON
    policy evaluation.
94. Rotate invalidates old secret immediately.
95. Rotate returns a new retrievalHandle for one-time secret retrieval.
96. Rotate may set expiry relative to rotation time.
97. Rotate cannot exceed max lifetime.
98. Rotate does not change `policyDocument`.
99. Policy change requires new key in v1.
100. Create/rotate retry behavior persists only retrievalHandle and does not
    leak plaintext.
101. Audit logs include `policySha256`.
102. Audit logs redact plaintext secret.
103. Create/rotate `toString()` and errors do not contain plaintext secret.
104. Create/rotate logs and TRACE/debug output redact plaintext secret.
105. STS session token path takes precedence in secure mode.
106. Existing `S3SecretManager/getsecret` validation path unchanged.
107. `HDDS-15273` WebIdentity STS path unchanged.
108. S3G does not store credentials.
109. S3G does not make final authorization decisions.

Additional Phase 5 readiness tests:

1. STS token present -> STS path wins.
2. Invalid non-empty STS token -> fail closed, no managed fallback.
3. Blank/whitespace session token visible to OM -> `INVALID_TOKEN`, no
   fallback.
4. Empty session token dropped by S3G, if current behavior, is treated as
   absent and documented by test.
5. Managed success in secure cluster with `local.policy.enabled=false`.
6. Managed absent in secure cluster -> legacy fallback.
7. Managed absent in non-secure managed mode -> `PERMISSION_DENIED` / fail
   closed.
8. Managed disabled -> `PERMISSION_DENIED` / fail closed, no legacy fallback.
9. Managed expired -> `PERMISSION_DENIED` / fail closed, no legacy fallback.
10. Managed wrong secret -> `PERMISSION_DENIED`, no fallback.
11. KMS decrypt failure -> `PERMISSION_DENIED` / fail closed.
12. Malformed or tampered envelope -> `PERMISSION_DENIED` / fail closed.
13. Managed auth on follower -> leader / failover / not-leader error before
    managed table lookup; stale follower does not authorize.
14. Rotate or disable followed by follower request does not allow stale key.
15. `accessKeyId != effectiveUser`:
    credential lookup uses `credentialAccessKeyId`;
    namespace routing uses `s3NamespaceAccessId`;
    authorization identity uses `effectiveUser`.
16. `OzoneManager.getS3VolumeContext(...)`:
    namespace lookup uses `s3NamespaceAccessId`;
    returned `userPrincipal`, KMS user, owner, and authorization effects use
    `effectiveUser`.
17. `OzoneManager.resolveBucketLink(...)`:
    namespace routing uses `s3NamespaceAccessId` where required;
    ACL / UGI / RequestContext authorization effects use `effectiveUser`.
18. Stored groups are metadata only in Phase 5.
19. Managed plaintext secret is never converted to immutable `String` in the
    managed validation path.
20. Managed byte-array AWSV4 validation does not reuse debug logging of derived
    signing material.
21. Byte-array AWSV4 validation clears plaintext and intermediate bytes as far
    as Java allows.
22. Managed request context / thread-local cleared on success.
23. Managed request context / thread-local cleared on failure.
24. Feature disabled -> existing behavior unchanged.
25. `local.policy.enabled=true` -> `PERMISSION_DENIED` until Phase 6.
26. Stored non-empty `policyDocument` with no evaluator -> `PERMISSION_DENIED`.
27. Non-secure managed mode -> `PERMISSION_DENIED` until Phase 6.
28. Existing `S3SecretManager/getsecret` path unchanged.
29. `HDDS-15273` WebIdentity STS path unchanged.

Additional Phase 3/4 readiness tests:

1. `RetrieveManagedS3AccessKeySecret` does not call
   `omExecutionFlow.submit(..., false)`.
2. Retrieve is rejected or redirected on non-leader / not-ready leader
   according to the selected Option A leader/failover semantics.
3. Retrieve never enters the Ratis retry-cache path.
4. Retrieve never enters the Ratis write path.
5. Concrete envelope round-trip decrypts after OM restart.
6. Malformed envelope fails closed.
7. Unsupported envelope version fails closed.
8. Missing algorithm, keyName, keyVersionName, EDEK, data IV/nonce, or
   ciphertext fails closed.
9. Authentication tag failure fails closed.
10. KMS permission, decrypt, unwrap, provider, key, or key-version failure
    fails closed.
11. `OMPBHelper.processForDebug` does not contain custom plaintext secret from
    create or rotate requests.
12. `OMPBHelper.processForDebug` does not contain retrieve response plaintext.
13. Audit/log/`toString`/errors do not contain plaintext, encrypted envelope,
    full policy document, or full retrievalHandle.
14. Retrieval map full purges expired entries.
15. Retrieval map full after purge causes create/rotate to fail before Ratis
    submission.
16. Retrieval entry is removed after successful retrieve.
17. Retrieval entry expires after TTL.
18. Wrong caller, failover-lost, unknown, and already-used handles return
    `MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE`.
19. External `S3SecretStoreProvider` without reliable existence check is
    unsupported or fails closed.
20. Create/rotate calls `generateEncryptedKey(...)` before building the
    envelope.
21. Create/rotate calls `decryptEncryptedKey(...)` to obtain plaintext DEK.
22. Create/rotate fails closed if `generateEncryptedKey(...)` fails.
23. Create/rotate fails closed if `decryptEncryptedKey(...)` fails.
24. Create/rotate Ratis request does not contain plaintext S3 secret.
25. Create/rotate Ratis request does not contain plaintext DEK.
26. `validateAndUpdateCache()` does not call KMS or RNG.
27. Malformed EDEK fails closed.
28. Missing EDEK fails closed.
29. Missing key version fails closed.
30. AES/GCM auth failure fails closed.
31. Plaintext S3 secret and plaintext DEK are cleared on success.
32. Plaintext S3 secret and plaintext DEK are cleared on exception path.

---
title: Custos — Protocol-Neutral Pluggable Authentication for Ozone
summary: Custos, a shared service that validates client identity for all Ozone access paths on the server side
date: 2026-07-13
jira: HDDS-15845
status: proposed
author: Abhishek Pal
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

# Custos — Ozone Auth Service Design Doc

## Table of Contents
1. [Problem Statement](#1-problem-statement)
2. [Scope](#2-scope)
3. [Architecture Overview](#3-architecture-overview)
   * [3.1 Request flow](#31-request-flow)
   * [3.2 Module structure](#32-module-structure)
   * [3.3 Deployment and scaling](#33-deployment-and-scaling)
4. [Background: Existing Authentication Mechanisms](#4-background-existing-authentication-mechanisms)
   * [4.1 OzoneFS delegation token path](#41-ozonefs-delegation-token-path)
   * [4.2 S3 static-key authentication path](#42-s3-static-key-authentication-path)
5. [Client Use Cases](#5-client-use-cases)
   * [5.1 Spark job with many executors (OzoneFS)](#51-spark-job-with-many-executors-ozonefs)
   * [5.2 Hive / HiveServer2](#52-hive--hiveserver2)
   * [5.3 S3 clients with static keys](#53-s3-clients-with-static-keys)
6. [Custos Provider Interface](#6-custos-provider-interface)
7. [CustosToken and UserInfo](#7-custostoken-and-userinfo)
   * [7.1 Token format](#71-token-format)
   * [7.2 Token field semantics](#72-token-field-semantics)
   * [7.3 Token size](#73-token-size)
   * [7.4 UserInfo change and UGI construction](#74-userinfo-change-and-ugi-construction)
   * [7.5 OMRequest wire extension](#75-omrequest-wire-extension)
8. [Token Signing and Verification](#8-token-signing-and-verification)
   * [8.1 Signing algorithm](#81-signing-algorithm)
   * [8.2 Signing input](#82-signing-input)
   * [8.3 Verification at OM](#83-verification-at-om)
9. [Phase 1 — OzoneFS / Hadoop-RPC Clients](#9-phase-1--ozonefs--hadoop-rpc-clients)
10. [Phase 2 — S3 Gateway](#10-phase-2--s3-gateway)
11. [Edge Cases and Security Considerations](#11-edge-cases-and-security-considerations)
12. [Compatibility and Upgrade](#12-compatibility-and-upgrade)
13. [Future Work](#13-future-work)

---

> *Custos* is Latin for "guardian" or "watchman".

## 1. Problem Statement

Ozone has several ways for a client to prove who it is.
Each one is written separately, and there is no shared place to plug in a new one.
Three concrete problems follow:

**(a) Non-Kerberos credentials cannot get a token from OM.**
When security is on, OM only issues a delegation token if the RPC connection used Kerberos or a certificate.
This check lives in `OzoneManager.isAllowedDelegationTokenOp()`:

```java
private boolean isAllowedDelegationTokenOp() throws IOException {
  AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
  return !UserGroupInformation.isSecurityEnabled()
      || (authMethod == AuthenticationMethod.KERBEROS)
      || (authMethod == AuthenticationMethod.KERBEROS_SSL)
      || (authMethod == AuthenticationMethod.CERTIFICATE);
}
```

A client that authenticated with an OIDC token (from a cloud identity provider) has no Kerberos ticket.
So it cannot obtain a delegation token and cannot use the OzoneFS path at all.

**(b) Every credential type is coded on its own, with no shared hook.**
Adding a new credential type today means editing several unrelated places: `OMClientRequest.getUserInfo()`, `OzoneManager.isAllowedDelegationTokenOp()`, and the S3 validation path.
There is no single interface where a new provider can be registered.

**(c) The identity carried to OM has no group information.**
`OMClientRequest.createUGI()` builds a `UserGroupInformation` from the user name only:

```java
userGroupInformation = UserGroupInformation.createRemoteUser(
    getUserInfo().getUserName());
```

The `UserInfo` protobuf message has no `groups` field.
When an authorization check needs group membership, it must be resolved out of band (for example by Ranger's own group provider).
The token the client presents already knows the groups, but that information is dropped before it reaches OM.

---

## 2. Scope

Custos is a server-side component that validates client credentials through pluggable providers and issues a signed `CustosToken`.
OM verifies the token locally.
This design covers **authentication only**.
Authorization (policy evaluation) is future work — see [Section 13](#13-future-work).
Custos does **not** replace or skip OM's existing ACL checks; a request verified from a `CustosToken` still runs through the same `checkAcls()` path as any other request.
This keeps authorization exactly where it is today and avoids introducing a second policy decision point in this design.

Custos is meant to be the single authentication entry point for every Ozone client access path.
This design works two paths through concretely: OzoneFS / Hadoop-RPC clients (Phase 1) and the S3 gateway (Phase 2).
Other access paths follow the same provider model and are not designed here.

The work is split into two phases.

**Phase 1 — OzoneFS / Hadoop-RPC clients:**
- Let a driver that authenticated with OIDC (or any configured provider) get a session-scoped `CustosToken` in place of the Kerberos-gated delegation token.
- Keep the executor path unchanged. Executors carry the token in YARN `Credentials` and never contact Custos.
- Keep legacy Kerberos clients working with no change.
- Carry groups inside the token so OM can build a full `UserGroupInformation`.

**Phase 2 — S3 Gateway:**
- Make Custos the single validation point for S3 credentials, starting with long-lived static keys.
- Route static-key validation through the same provider contract so any other credential type can plug in later without new gateway code.

**Out of scope:**
- Authorization policy evaluation (Ranger, native ACLs).
- Temporary (session-token) credential validation — see [Section 13](#13-future-work).
- Inter-service authentication (SCM ↔ OM ↔ Datanode).

---

## 3. Architecture Overview

### 3.1 Request flow

A client presents a credential to Custos.
Custos validates it through the matching provider and returns a signed `CustosToken`.
The client carries that token to OM on each request.
OM verifies the token locally — no network call — and then runs its existing ACL checks.

```
┌────────────┐     ┌───────────────┐     ┌────────────────┐
│  Client    │────▶│    Custos     │────▶│ Ozone Manager  │
│            │     │               │     │                │
│ Presents   │     │ 1. Pick       │     │ 1. Verify      │
│ credential │     │    provider   │     │    token sig    │
│            │     │ 2. Validate   │     │ 2. Check        │
│            │     │    credential │     │    audience     │
│            │     │ 3. Issue      │     │    + expiry     │
│            │     │    CustosToken│     │ 3. Build UGI    │
│            │     │               │     │    + run ACLs   │
└────────────┘     └───────────────┘     └────────────────┘
```

The token is a bearer of a verified identity, not a proxy for an authorization decision.
OM trusts the *identity* the token asserts (subject and groups) because the signature proves Custos issued it, but OM still evaluates its own ACLs against that identity.

### 3.2 Module structure

- **`hadoop-ozone/ozone-custos-common`**: shared library containing the `CustosTokenProto` definition, `CustosTokenVerifier`, and the provider plugin interfaces (`CustosProvider`, `CustosCredential`, `CustosIdentity`, `CustosException`). Depended on by both Custos and OM. OM needs only the verifier and the token type; it does not depend on the provider implementations.
- **`hadoop-ozone/ozone-custos`**: the standalone Custos service process — the gRPC server, the provider implementations, provider loading, token issuance and signing, audit, and metrics.

Splitting the verifier and token type into a common module keeps OM's dependency surface small: OM links only against `ozone-custos-common`, so provider dependencies (for example a JWKS/OIDC client) never enter the OM classpath.

### 3.3 Deployment and scaling

Custos is stateless.
It holds no per-client session state: a token is self-describing and is verified from its signature alone.
All signing and verification use the symmetric key that SCM already distributes through `SecretKeyClient` / `ManagedSecretKey`, so any Custos instance can issue a token that any OM can verify, and OM never has to reach a *specific* Custos instance.

Because there is no shared state to coordinate, Custos can run as a single instance for a small cluster or as several independent instances behind a load balancer for availability and throughput.
Instance count is independent of the OM count; Custos does not join the OM Ratis ring.

---

## 4. Background: Existing Authentication Mechanisms

This section describes the authentication paths that exist in Ozone today.
No judgment — just the paths as they are, so the phased changes later are clear.

### 4.1 OzoneFS delegation token path

This is how Spark, Hive, and other Hadoop-RPC clients authenticate.

1. The client calls `FileSystem.getDelegationToken(renewer)`. In Ozone this reaches `BasicOzoneClientAdapterImpl.getDelegationToken()`:

   ```java
   public Token<OzoneTokenIdentifier> getDelegationToken(String renewer)
       throws IOException {
     if (!securityEnabled) {
       return null;
     }
     Token<OzoneTokenIdentifier> token =
         ozoneClient.getObjectStore().getDelegationToken(new Text(renewer));
     token.setKind(OzoneTokenIdentifier.KIND_NAME);
     return token;
   }
   ```

2. The request reaches OM as `OMGetDelegationTokenRequest`. OM checks `isAllowedDelegationTokenOp()` (shown in [Section 1](#1-problem-statement)) and, if allowed, the `OzoneDelegationTokenSecretManager` signs and returns the token.
3. On every later RPC, the client sends the token. OM validates it with `OzoneDelegationTokenSecretManager.retrievePassword()`.
4. Token renewal is handled by `OzoneDelegationTokenRenewer`, a background thread that YARN runs so the token stays valid even after the driver exits.

The token identifier kind is `OzoneTokenIdentifier.KIND_NAME` (`"OzoneToken"`).

There are in fact **two** tokens in this path, and only the first one changes with Custos:

- `Token<OzoneTokenIdentifier>` — the session-scoped delegation token above. YARN distributes it to every executor and it is used for OM RPCs.
- `Token<OzoneBlockTokenIdentifier>` — a per-block token OM issues during key operations and the client carries to the datanodes for the actual data transfer.

Custos changes only how the driver bootstraps the first token (Kerberos → OIDC or another provider).
Block tokens, executor distribution, and the datanode data path are untouched.

### 4.2 S3 static-key authentication path

This is how S3 clients (boto3, the AWS Java SDK, s5cmd) authenticate with a long-lived access key and secret.

1. The client signs the request with AWS Signature Version 4 and sends an `Authorization: AWS4-HMAC-SHA256 Credential=<accessId>/...` header.
2. The gateway parses it with `AuthorizationV4HeaderParser` and rebuilds the string-to-sign with `StringToSignProducer`.
3. The gateway forwards the access key, the signature, and the string-to-sign to OM inside the `S3Authentication` part of the request. The gateway does **not** verify the signature itself.
4. OM looks up the secret in `s3SecretTable` and verifies the HMAC with `AWSV4AuthValidator.validateRequest()`.

So the gateway trusts the client's string-to-sign and OM does the actual HMAC check.
The gateway holds no secret.

---

## 5. Client Use Cases

Each use case describes the client today and what changes with Custos.

### 5.1 Spark job with many executors (OzoneFS)

**Today:** The driver gets a `Token<OzoneTokenIdentifier>` using Kerberos.
YARN distributes it to every executor inside `Credentials`.
Each executor finds the token with `OzoneDelegationTokenSelector` and uses it for every OM RPC.
`OzoneDelegationTokenRenewer` renews it before expiry, even if the driver has exited.
Executors never fetch their own token.

**With Custos (Phase 1):** The driver authenticates to Custos with its OIDC JWT and receives a session-scoped `CustosToken`.
It is multi-use, has a longer TTL, and carries the driver's groups.
The token is session-scoped rather than per-operation on purpose: a single filesystem operation already spans several OM RPCs (open, get key info, commit, and so on), and a job issues many such operations, so a per-operation token would force a Custos round trip on the hot path.
A session-scoped multi-use token matches how the delegation token works today, and how AWS STS session credentials work.
YARN distributes it to executors exactly as before — it is still a `Token<>` inside `Credentials`.
Executors put it in `OMRequest.custosToken` (field 150, a high field number chosen to avoid collision with other in-flight `OMRequest` fields).
OM verifies the signature locally.
No executor contacts Custos.
The renewer calls the Custos renew endpoint instead of the delegation token renew path.

### 5.2 Hive / HiveServer2

**Today:** HiveServer2 is a long-running daemon.
At startup it calls `fs.addDelegationTokens()` and holds one token for the cluster lifetime, renewing it through `OzoneDelegationTokenRenewer`.
This requires Kerberos.

**With Custos:** HiveServer2 authenticates once with OIDC and gets a token with a bounded `maxLifetimeMs` (for example 7 days).
It renews daily through the Custos renew endpoint until the maximum lifetime is reached, after which it must re-authenticate.

### 5.3 S3 clients with static keys

**Today:** boto3 signs with SigV4.
The gateway builds the string-to-sign and forwards it to OM, which verifies the HMAC against `s3SecretTable`.
This works.

**With Custos (Phase 2):** The verification moves into Custos (through `S3SecretProvider`, see [Section 6](#6-custos-provider-interface)), which then issues a `CustosToken`.
The client sees no difference — the same keys and the same requests keep working.

---

## 6. Custos Provider Interface

The pluggable provider model is the core of Custos.
Every credential type is validated by a `CustosProvider`, and Custos picks the provider from the credential type.
Custos owns this contract; it is not tied to any single feature or credential type.

```java
public interface CustosProvider {
  CustosIdentity authenticate(CustosCredential credential)
      throws CustosException;
}
```

- `CustosCredential` is the input. It carries the credential type and the raw credential material — a bearer token, a SPNEGO token, or S3 SigV4 material. A single input type lets Custos route on type without special-casing any provider.
- `CustosIdentity` is the result. Its fields are `subject`, `groups`, `roles`, `issuer`, `authMethod`, `authenticatedAt`, and `expiresAt`.

Custos ships these providers:

| Provider class | Credential type | Validation |
|:---------------|:----------------|:-----------|
| `OidcProvider` | OIDC JWT (bearer token) | Validate the JWT against a JWKS endpoint (RSA keys only, reject `alg=none`). Groups and roles come from the token claims. |
| `KerberosProvider` | SPNEGO token | Accept the client SPNEGO token with the Custos service keytab (`ozone.custos.kerberos.keytab`). The service principal travels in the credential from the client; Custos does not read a principal config locally. Group resolution is handled by `KerberosIdentityProvider`. |
| `S3SecretProvider` | SigV4 static key | Look up the secret with `S3SecretManager.getSecretString(accessId)` and verify the HMAC with `AWSV4AuthValidator.validateRequest()`. |
| `DelegationTokenProvider` | `Token<OzoneTokenIdentifier>` | `OzoneDelegationTokenSecretManager.retrievePassword()`. Lets existing delegation-token holders work through the same contract. |

Two pieces of machinery hold this together:

- **Config-driven provider loading.** Custos loads providers from a config key `ozone.custos.providers` (a comma-separated list of class names), following the same pattern Ozone already uses for `ozone.acl.authorizer.class`. A `ServiceLoader`-based mechanism can be added later ([Section 13](#13-future-work)).
- **One credential input type.** `CustosCredential` must be able to carry every credential type — a bearer token, a SPNEGO token, or S3 SigV4 material — so all providers share a single input and Custos can route on type without special-casing.

The whole feature is gated behind `ozone.custos.enabled`.
When it is off, none of the paths below are active and Ozone behaves exactly as it does today.

---

## 7. CustosToken and UserInfo

### 7.1 Token format

The `CustosToken` is the signed proof of identity Custos issues.
It uses `proto2` syntax like the rest of Ozone, and every field is `optional` — `proto2` field presence is enforced in application code at construction and verification time.

```protobuf
message CustosTokenProto {
  optional string subject = 1;              // the authenticated user
  optional string authProvider = 2;         // which provider validated it
  optional string issuer = 3;               // the Custos instance that issued it
  optional string audience = 4;             // the OM cluster this token is valid for
  optional uint64 expiryMs = 8;             // absolute expiry, epoch millis
  optional string tokenId = 9;              // unique id (UUID) for replay/revocation
  optional bytes  signature = 10;           // HMAC over all other fields
  repeated string groups = 15;              // groups for UGI construction
  optional string signatureAlgorithm = 16;  // default "HMAC-SHA256"
  optional string signingKeyId = 17;        // SCM key id used to sign (enables rotation)
  optional uint64 issuedAtMs = 18;          // issue time, epoch millis
  optional uint64 maxLifetimeMs = 19;       // hard ceiling on renewal
  optional bool   multiUse = 20;            // session-scoped multi-use token
}
```

The `CustosIdentity` returned by a provider maps onto these fields directly:
`subject` → `subject`, `groups` → `groups`, `issuer` → `issuer`, `authMethod` → `authProvider`, `authenticatedAt` → `issuedAtMs`, `expiresAt` → `expiryMs`.

The token carries an *identity*, not an authorization decision.
There is deliberately no `cmdType`, `resourceScope`, `allowedAction`, or policy field: OM evaluates ACLs itself against the identity, so binding the token to one operation or resource would add nothing and would break the session-scoped multi-use model the clients above rely on.

### 7.2 Token field semantics

| Field | Verification / use at OM | Purpose |
|:------|:-------------------------|:--------|
| `subject` | Becomes the request identity | Maps to `UserInfo.userName` |
| `groups` | Set on the constructed UGI | Group membership without an out-of-band lookup |
| `audience` | Must match this OM's service/cluster id | Prevents cross-cluster token reuse |
| `expiryMs` | Must be in the future vs. the leader transaction timestamp (with clock skew tolerance) | Natural expiry |
| `issuedAtMs` / `maxLifetimeMs` | Renewal is refused past `issuedAtMs + maxLifetimeMs` | Bounds total token lifetime |
| `tokenId` | Not replayed (write ops); revocation lookup | Duplicate-mutation and revocation control |
| `signature` | HMAC verified with the SCM key named by `signingKeyId` | Integrity and authenticity |
| `signingKeyId` | Selects which SCM key to verify against | Enables key rotation |

### 7.3 Token size

A typical token with a handful of groups serializes to roughly 350–500 bytes.
It is added to every `OMRequest` as an optional field, and on write requests it is replicated through Ratis (see [Section 11](#11-edge-cases-and-security-considerations)), so the field set is kept deliberately small.

### 7.4 UserInfo change and UGI construction

So OM can build a `UserGroupInformation` that includes groups, add a `groups` field to `UserInfo`.
Field number 2 is currently free:

```protobuf
message UserInfo {
    optional string userName = 1;
    repeated string groups = 2;       // NEW
    optional string remoteAddress = 3;
    optional string hostName = 4;
}
```

Then extend `OMClientRequest.createUGI()` so that, when the request was verified from a `CustosToken`, it sets these groups on the UGI instead of leaving them empty.
Today the method sets only the user name:

```java
userGroupInformation = UserGroupInformation.createRemoteUser(
    getUserInfo().getUserName());
```

With the change, the groups carried in the token flow into the UGI, so the existing ACL-check path has group membership available without a separate Ranger group lookup.

### 7.5 OMRequest wire extension

`OMRequest` gains one new optional field:

```protobuf
message OMRequest {
  // ... existing fields unchanged ...

  // Token issued by Custos. When present, OM uses it for identity.
  // When absent, OM falls back to the existing Kerberos / S3Authentication paths.
  optional CustosTokenProto custosToken = 150;
}
```

Field 150 is a deliberately high number, chosen to stay clear of other in-flight `OMRequest` fields so the two can merge in either order.
The field is optional, so a client that never sets it behaves exactly as today.

---

## 8. Token Signing and Verification

### 8.1 Signing algorithm

Custos signs with **HMAC-SHA256**.

| Aspect | Choice | Rationale |
|:-------|:-------|:----------|
| Algorithm | HMAC-SHA256 | Fast to verify; OM verifies it on every request, including the Ratis apply path |
| Key distribution | SCM `SecretKeyClient` / `ManagedSecretKey` | Already exists and already distributes symmetric keys to Ozone services |
| Key rotation | SCM-managed, grace-period overlap | Both the old and the new key are accepted during the rotation window (see [Section 11](#11-edge-cases-and-security-considerations)) |

The token records `signingKeyId`, so a verifier looks up the exact key that signed it rather than trying keys.
This also removes any incentive to make the key guessable and lets revocation target a specific key id.

### 8.2 Signing input

The signing input is the protobuf serialization of the token with the signature-related fields cleared:

```java
public byte[] computeSigningInput(CustosTokenProto.Builder token) {
  return token.clone()
      .clearSignature()
      .clearSigningKeyId()
      .clearSignatureAlgorithm()
      .build()
      .toByteArray();
}
```

### 8.3 Verification at OM

`CustosTokenVerifier` runs entirely locally and makes no network calls, so it is safe on the Ratis apply path:

```java
public VerifiedIdentity verify(CustosTokenProto token, OMRequest request)
    throws InvalidCustosTokenException {
  verifySignature(token);   // HMAC with the SCM key named by signingKeyId
  verifyExpiry(token);      // expiryMs in the future vs. the leader transaction timestamp
  verifyAudience(token);    // audience matches this OM
  // (write ops only) replay / revocation check on tokenId
  return new VerifiedIdentity(token.getSubject(), token.getGroupsList(),
      token.getAuthProvider());
}
```

Key points:

- **Expiry is checked against the leader-assigned transaction timestamp**, not the local wall clock of whichever follower is applying the log. Every OM applies the same log entry with the same timestamp, so all replicas reach the same expiry decision deterministically.
- **Signature comparison uses `MessageDigest.isEqual()`** (constant-time). `Arrays.equals()` short-circuits on the first mismatched byte and leaks timing information about the expected signature.
- A verification failure — expired, bad signature, wrong audience, unknown or revoked signing key, replay — returns a single new result code `INVALID_CUSTOS_TOKEN` on `OMException.ResultCodes`.

After verification, OM builds the UGI from `subject` + `groups` ([Section 7.4](#74-userinfo-change-and-ugi-construction)) and runs its existing ACL checks against it.

---

## 9. Phase 1 — OzoneFS / Hadoop-RPC Clients

**Goal:** Let a driver that authenticated with Kerberos, OIDC, or any configured provider obtain a session-scoped `CustosToken` that replaces the delegation token.
Executor behavior does not change.

### 9.1 New Custos gRPC endpoint

```protobuf
service CustosService {
  rpc GetSessionToken(GetSessionTokenRequest) returns (GetSessionTokenResponse);
  rpc RenewSessionToken(RenewSessionTokenRequest) returns (RenewSessionTokenResponse);
}
```

`GetSessionTokenRequest` carries the credential bytes, the credential type, and a requested TTL.
`GetSessionTokenResponse` carries a serialized `CustosTokenProto`.

### 9.2 OzoneFS getDelegationToken() change

In `BasicOzoneClientAdapterImpl.getDelegationToken()`: when `ozone.custos.enabled=true`, call the Custos `GetSessionToken` instead of `ObjectStore.getDelegationToken()`.
Wrap the returned `CustosToken` in a `Token<>` with a new kind name (`CustosToken`).
YARN distributes it the same way it distributes a delegation token.

### 9.3 Executor path (no change)

Executors receive the token in `Credentials`.
A new `CustosTokenSelector` (modeled on `OzoneDelegationTokenSelector`) selects tokens of kind `CustosToken`.
On the client side, `OzoneManagerProtocolClientSideTranslatorPB.submitRequest()` reads the token from the UGI and sets `OMRequest.custosToken` (field 150).

### 9.4 Token renewal

`OzoneDelegationTokenRenewer` calls `RenewSessionToken` on Custos.
Custos checks that the token has not passed `issuedAtMs + maxLifetimeMs` and issues a new token with an extended `expiryMs`.
The presented token itself is proof of prior authentication for renewal.
Past the maximum lifetime, renewal is refused and the client must re-authenticate.

### 9.5 Legacy Kerberos path unchanged

When `ozone.custos.enabled=false`, all existing paths work with no change.
When it is enabled but a request carries no `CustosToken`, OM falls back to the existing Kerberos / `S3Authentication` path.

### 9.6 OM verification changes

In the OM server-side translator, before the existing S3 auth check, add a token-verification step:

```java
if (request.hasCustosToken()) {
  custosTokenVerifier.verify(request.getCustosToken(), request);
  // build UserInfo (with groups) from the verified token
}
```

`CustosTokenVerifier` runs entirely locally (see [Section 8.3](#83-verification-at-om)): it checks the HMAC signature, the expiry against the leader-assigned transaction timestamp, and the audience.
It makes no network calls, so it is safe on the Ratis apply path.

### 9.7 UGI construction with groups

Extend `OMClientRequest.createUGI()` so that a token-verified request builds a UGI that includes the token's groups, rather than a UGI with the user name only.
The groups come from the `CustosToken.groups` field, not from a Kerberos ticket.
This UGI is used for the existing ACL checks — Custos does not skip them.

---

## 10. Phase 2 — S3 Gateway

**Goal:** Make Custos the single validation point for S3 credentials.
Static keys go through it first; the same provider contract lets other credential types plug in later with no new gateway code.

### 10.1 Flow for a static S3 key

1. Client sends `Authorization: AWS4-HMAC-SHA256 Credential=<accessId>/...`.
2. `AuthorizationV4HeaderParser` extracts the access ID and signature.
3. The gateway calls Custos with the static-key credential.
4. `S3SecretProvider` looks up the secret with `S3SecretManager.getSecretString(accessId)`, verifies the HMAC with `AWSV4AuthValidator.validateRequest()`, and returns a `CustosIdentity`.
5. Custos issues a `CustosToken`. The gateway sets it on `OMRequest.custosToken`.

Any additional S3 credential type — for example temporary (session-token) credentials — plugs in as another provider behind the same gateway call, without changing steps 1–5.
See [Section 13](#13-future-work).

### 10.2 What changes in the S3 gateway

- New call: `custosClient.authenticate(...)` returns a `CustosToken`.
- When a Custos token is present, set `OMRequest.custosToken` instead of `S3Authentication`.
- If Custos is unreachable, fall back to the legacy `S3Authentication` path.

### 10.3 What stays the same

- `S3GetSecretRequest` / `S3RevokeSecretRequest`: the secret lifecycle is unchanged.
- `AWSV4AuthValidator`: still used, now inside `S3SecretProvider` rather than in the OM request path.
- Multi-tenancy lookups (`OMMultiTenantManager`, access-ID-to-principal mapping): unchanged. Custos uses the same lookups.

### 10.4 STS session-token credentials

AWS-style temporary credentials use an access key that starts with `ASIA` and carry a session token in the `x-amz-security-token` header (and, for presigned URLs, in the `X-Amz-Security-Token` query parameter).
The distinguishing property is that the secret used to verify the SigV4 signature is **derived from the session token itself**, not looked up in `s3SecretTable`.

**The shift this introduces.**
For a static key, the gateway trusts the client's string-to-sign and OM (or, in Phase 2, `S3SecretProvider`) does the HMAC check.
For a session token, the credential is self-describing and must be decrypted and validated before any signature check.
Rather than teaching the S3 gateway how to do that — or spreading STS validation across the gateway and OM — the gateway stays a pure credential extractor and Custos becomes the single point that authenticates the STS credential.
The S3 gateway is no longer where the session token is validated.

**Flow.**
This reuses the Phase 2 flow ([Section 10.1](#101-flow-for-a-static-s3-key)) unchanged in shape; only the provider differs.

1. Client sends the SigV4 `Authorization` header plus `x-amz-security-token`.
2. The gateway extracts the access ID, the signature, the string-to-sign, and the session token. It parses; it does not validate.
3. The gateway calls Custos with a session-token credential (the `x-amz-security-token` value plus the SigV4 material). The gateway does not decrypt or verify anything.
4. A session-token `CustosProvider` validates it: decrypt the token, check its expiry, check that it has not been revoked (and that its originating principal is not revoked), confirm the presented access key matches the one bound inside the token, and verify the SigV4 signature using the secret derived from the token. On success it returns a `CustosIdentity`.
5. Custos issues a `CustosToken`. The gateway sets it on `OMRequest.custosToken`, exactly as for a static key.

**What this means for the moving pieces.**
- The gateway change is only credential *extraction*: read `x-amz-security-token` from the header and the presigned-URL query, and forward it as part of the S3 credential. No new validation logic lands in the gateway.
- The session-token provider reuses the STS validation (decrypt, expiry, revocation, access-key binding, signature) rather than re-implementing it. Custos is where that logic is invoked from.
- OM does not gain an STS-specific path. It verifies the resulting `CustosToken` the same way it verifies every other token ([Section 8.3](#83-verification-at-om)); the STS specifics are fully resolved inside Custos before the token is issued.
- Fallback ([Section 10.2](#102-what-changes-in-the-s3-gateway)) still applies: if Custos is unreachable, the gateway falls back to the legacy path that carries the credential to OM.

---

## 11. Edge Cases and Security Considerations

**SCM signing key unavailable at OM restart.**
Token verification needs the signing key from SCM.
During OM log replay after a restart, that key must be reachable, or verification of replayed requests fails.
The verifier caches keys by `signingKeyId` and must tolerate a temporary SCM outage rather than fail replay outright.

**Token expiry during Ratis replication (clock skew).**
A token can be within its lifetime when the leader accepts a write and slightly past it by the time a follower applies the log entry.
Because expiry is checked against the *leader-assigned transaction timestamp* rather than each node's wall clock, all replicas decide expiry identically.
On top of that, OM applies a configurable clock-skew tolerance (`ozone.custos.token.clock.skew.ms`, default 30000), and token TTLs should be set comfortably longer than the maximum expected replication latency.

**Key rotation overlap.**
During signing-key rotation both the old and the new key must verify:

```
Time: ─────────────────────────────────────────────────────▶
Key A: ═══════════════╗
                      ║ grace period
Key B:          ╔═════╩═══════════════════════════════════
                ↑ rotation point
```

SCM distributes the new key; Custos signs new tokens with it immediately, while OM accepts tokens signed with either key during the grace window.
After the grace period the old key is retired and tokens signed with it are rejected.
Because each token names its `signingKeyId`, no key guessing is involved.

**Replay protection sizing.**
Only write operations need replay protection; reads are idempotent.
For those, OM keeps a bounded, self-expiring set of recently seen `tokenId` values.
The set is bounded by roughly `maxTokenTTL / averageWriteInterval`, and entries auto-expire after the token TTL.
For a session-scoped multi-use token this set stays small because a job reuses one `tokenId` across many requests rather than presenting a fresh id per request.

**Custos partially down (fallback divergence).**
When some requests go through Custos and others fall back to the legacy path, two code paths make identity decisions at once.
Both must resolve the same client to the same identity, or behavior becomes inconsistent.
The gateway logs a warning whenever it falls back so operators can see the divergence.

**Token size in the Ratis log.**
Every write request that carries a `CustosToken` replicates its bytes through Ratis.
A few hundred bytes at a high write rate is real replication bandwidth, so the token field set is kept small.

**Token not logged.**
The signature bytes must never appear in log output.
Only `tokenId` and `subject` are safe to log for debugging and audit.
Audit records may add fields such as the authorized operation, client id, or source IP, but the signature bytes and any raw credential material stay on a must-not-log list.

**OM ACLs are not skipped.**
A `CustosToken` authenticates; it does not authorize.
OM runs the same `checkAcls()` path for token-verified requests as for any other, so there is no batch-scope-versus-per-key gap and no second policy decision point to keep consistent with OM's own ACLs.

---

## 12. Compatibility and Upgrade

### Phased rollout

| Phase | Change | Backward compatibility |
|:------|:-------|:-----------------------|
| Phase 0 | Deploy Custos alongside OM; no client uses it. | All existing paths unchanged; Custos is idle. |
| Phase 1 | OzoneFS drivers optionally exchange a credential for a `CustosToken`. | Kerberos still works as fallback. |
| Phase 2 | S3 gateway delegates static-key validation to Custos. | Fallback to the legacy `S3Authentication` path. |
| Phase 3 | Legacy-only auth paths deprecated once all clients are migrated. | Opt-in; requires every client to have moved. |

### Backward compatibility

- **Custos disabled (`ozone.custos.enabled=false`):** `OMRequest.custosToken` is never set; clients and OM behave exactly as today.
- **Custos enabled, legacy clients:** OM accepts both a `CustosToken` and the existing Kerberos UGI / `S3Authentication`, choosing the token only when the field is present.
- **Mixed cluster during a rolling upgrade:** an OM that has not yet been upgraded ignores the unknown `custosToken` field (protobuf unknown-field handling) and falls back to the legacy path; an upgraded OM uses it. A client should therefore only rely on Custos once the cluster has finalized.

### Finalization gating

A new OM layout feature gates acceptance of the field so behavior is uniform across the ring:

```java
// New OMLayoutFeature:
CUSTOS_TOKEN_SUPPORT(N, "Accept CustosToken in OMRequest for authentication");
```

Before finalization, OM rejects a request that carries `custosToken` with `NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION`.
After finalization, OM accepts and verifies it when `ozone.custos.enabled=true`.

---

## 13. Future Work

- **Temporary (session-token) credential support.** AWS-style temporary (STS) credentials plug in as one more provider behind the same Phase 2 gateway call. Custos, not the S3 gateway, becomes the point that validates the session token. This is an in-progress feature; its intended shape is defined in [Section 10.4](#104-sts-session-token-credentials).
- **Authorization.** Move policy evaluation (Ranger or native ACLs) behind Custos so it becomes the single policy decision point. Authorization is done in-process at OM today, and this design keeps it there.
- **More access paths.** Extend the same provider model to other Ozone access paths beyond OzoneFS and the S3 gateway — for example typed gRPC services (token carried in call metadata), Recon HTTP, and admin RPC — so authentication is validated the same way everywhere.
- **Inter-service mTLS.** The SCM CA already issues X.509 certificates to services, so extending mutual TLS to all gRPC service connections needs no new CA infrastructure.
- **Custom providers via ServiceLoader.** Beyond config-driven loading, let operators drop a JAR implementing `CustosProvider` on the classpath and have it discovered automatically.
- **Client-side token caching.** For read-heavy workloads, a client-side cache of session tokens keyed by identity avoids re-fetching, respecting the token TTL and maximum lifetime.
- **Asymmetric token signing.** HMAC is enough inside one cluster, and it is preferred there because it is fast and needs no key distribution beyond what SCM already does. For cross-cluster federation, where sharing a symmetric key across a trust boundary is not acceptable, an asymmetric algorithm (for example RSA-PSS or Ed25519) can be added as an alternative `signatureAlgorithm`.

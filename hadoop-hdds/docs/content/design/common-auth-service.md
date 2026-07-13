---
title: Ozone Auth Service — Protocol-Neutral Pluggable Authentication and Authorization
summary: A standalone service that unifies all identity validation and policy evaluation for Ozone
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

# Ozone Auth Service — Design Doc

## Table of Contents
1. [Motivation](#1-motivation)
2. [Proposal](#2-proposal)
   * [2.1 Token Format](#21-token-format)
   * [2.2 OMRequest Wire Extension](#22-omrequest-wire-extension)
   * [2.3 Authentication Provider Plugin Interface](#23-authentication-provider-plugin-interface)
   * [2.4 Authorization Backend Interface](#24-authorization-backend-interface)
   * [2.5 Token Signing and Verification](#25-token-signing-and-verification)
   * [2.6 OM Integration](#26-om-integration)
   * [2.7 RPC Protocol Support](#27-rpc-protocol-support)
   * [2.8 S3 Gateway Migration](#28-s3-gateway-migration)
   * [2.9 REST/gRPC Gateway for Lightweight Clients](#29-restgrpc-gateway-for-lightweight-clients)
   * [2.10 Edge Cases and Security Considerations](#210-edge-cases-and-security-considerations)
3. [Upgrades](#3-upgrades)
4. [Industry Patterns](#4-industry-patterns-request-bound-tokens-in-distributed-systems)
5. [Future Work](#5-future-work)

---

## 1. Motivation

Ozone currently fragments authentication and authorization across multiple client/protocol paths with no unified control plane:

| Access Path | Authentication | Authorization |
|:------------|:---------------|:--------------|
| Hadoop RPC (CLI, OzoneFS) | Kerberos SASL at transport layer (`UserGroupInformation`) | `IAccessAuthorizer.checkAccess()` in OM |
| S3 Gateway | AWS SigV4 parsed in `AWSSignatureProcessor`, re-verified in OM | Same `IAccessAuthorizer` via `RequestContext` |
| Delegation Tokens | `OzoneDelegationTokenSecretManager` inside OM | Token carries identity; same authorizer |
| STS/AssumeRole | `AssumeRoleRequest` processed in OM | Session policy intersected with ACLs |

### Problems with the current architecture

#### a) Authentication is locked to transport
Kerberos is negotiated at the Hadoop RPC connection level. gRPC does not support Kerberos/SASL natively, so gRPC-based services require a different authentication mechanism. Clients without a Kerberos ticket or S3 keys have no path to authenticate.

#### b) No extensible provider model
Supporting OAuth/OIDC or any non-Kerberos credential requires modifying `OMClientRequest.getUserInfo()` and the OM request handling pipeline. There is no plugin interface for authentication providers.

#### c) S3 signature verification is duplicated
S3 Gateway parses the signature (`AWSSignatureProcessor`), then OM re-verifies it against the stored secret via `S3SecretManager`. The signature is computed twice for every S3 request.

#### d) OM performs identity validation in the Ratis state machine path
This constrains OM's ability to call external identity stores or policy engines during request processing. External calls from the Ratis apply path would break determinism.

#### e) Policy enforcement is fragmented
S3 Gateway, OM, and delegation token managers each log authentication events separately. There is no unified audit stream for all authentication decisions across all access paths.

---

## 2. Proposal

Introduce a standalone **Ozone Auth Service** — a separate process that becomes the single owner of client-facing authentication and authorization for all Ozone metadata operations.

```
┌────────────┐     ┌───────────────┐     ┌────────────────┐
│  Client    │────▶│ Auth Service  │────▶│ Ozone Manager  │
│            │     │               │     │                │
│ Presents   │     │ 1. Validate   │     │ 1. Verify      │
│ credential │     │    credential │     │    token sig   │
│ + target   │     │ 2. Evaluate   │     │ 2. Check       │
│ operation  │     │    policy     │     │    binding     │
│            │     │ 3. Issue      │     │ 3. Execute     │
│            │     │    token      │     │    operation   │
└────────────┘     └───────────────┘     └────────────────┘
```

The Auth Service validates any supported credential type, evaluates authorization policy (Ranger or native ACLs), and issues a short-lived cryptographically-signed `OzoneAuthToken` bound to the specific operation and resource. OM verifies the token locally — no network calls, preserving Ratis determinism.

### Module Structure

- **`hadoop-ozone/ozone-auth-common`**: Shared library containing the `OzoneAuthToken` protobuf, `TokenVerifier`, and provider plugin interfaces. Depended on by both Auth Service and OM.
- **`hadoop-ozone/ozone-auth-service`**: Standalone service process with gRPC/REST server, provider implementations, authorization backends, and audit logging.

---

### 2.1 Token Format

The `OzoneAuthToken` is a protobuf message that binds an authenticated identity to a specific operation and resource.

```protobuf
syntax = "proto2";
option java_package = "org.apache.hadoop.ozone.protocol.proto";
option java_outer_classname = "OzoneAuthTokenProtos";
package hadoop.ozone;

message OzoneAuthTokenProto {
  // The authenticated principal (e.g., "alice@EXAMPLE.COM")
  optional string subject = 1;

  // Which auth provider authenticated this principal
  // Values: "kerberos", "s3", "oidc", "sts"
  optional string authProvider = 2;

  // The Auth Service instance that issued this token
  optional string issuer = 3;

  // The target OM cluster this token is valid for
  optional string audience = 4;

  // The operation type this token authorizes (matches OMRequest.Type enum name)
  optional string cmdType = 5;

  // The resource path this token covers
  // Supports trailing /* for prefix scopes (e.g., "/vol1/bucket1/*")
  optional string resourceScope = 6;

  // The ACL action permitted: "READ", "WRITE", "CREATE", "DELETE", "LIST"
  optional string allowedAction = 7;

  // Expiration time in milliseconds since epoch
  optional uint64 expiryMs = 8;

  // Unique token ID (UUID) for replay protection
  optional string tokenId = 9;

  // Cryptographic signature over all fields above
  optional bytes signature = 10;

  // SHA-256 hash of the canonical request parameters
  // Prevents parameter tampering after token issuance
  optional string canonicalRequestHash = 11;

  // Session policy for STS-style scoped tokens (JSON)
  optional string sessionPolicy = 12;

  // Identity of the calling service (e.g., "mcp-server-prod")
  optional string sourceService = 13;

  // Policy version at the time of authorization
  // OM can reject tokens with epoch older than a configured revocation point
  optional uint64 policyEpoch = 14;

  // Resolved group memberships for the subject
  repeated string groups = 15;

  // Signature algorithm used (default: "HMAC-SHA256")
  optional string signatureAlgorithm = 16;

  // Key ID used for signing (enables key rotation)
  optional string signingKeyId = 17;
}
```

```
Note: All fields are optional because proto3 does not support the required keyword.
Field presence is enforced at the application level during token construction and
verification.
```

#### Token field semantics

| Field | Verification at OM | Purpose |
|:------|:-------------------|:--------|
| `subject` | Used as request identity | Maps to `UserInfo.userName` |
| `audience` | Must match this OM's cluster ID | Prevents cross-cluster token reuse |
| `cmdType` | Must match `OMRequest.cmdType` | Prevents operation substitution |
| `resourceScope` | Must cover the requested resource path | Prevents resource escalation |
| `expiryMs` | Must be in the future (with clock skew tolerance) | Natural revocation |
| `tokenId` | Must not be replayed (write ops) | Prevents duplicate mutations |
| `signature` | HMAC verified with shared key | Integrity and authenticity |
| `policyEpoch` | Must be ≥ configured floor | Invalidates tokens from stale policies |

#### Token size estimate

A typical token with 3 groups and no session policy is ~350-500 bytes serialized. This is added to every `OMRequest` as an optional field.

---

### 2.2 OMRequest Wire Extension

The existing `OMRequest` protobuf gains one new optional field:

```protobuf
// In OmClientProtocol.proto
import "OzoneAuthToken.proto";

message OMRequest {
  // ... existing fields (1-95) unchanged ...

  // Auth token from the Ozone Auth Service.
  // When present, OM uses this for identity and skips re-authentication.
  // When absent, OM falls back to existing Kerberos/S3 auth mechanisms.
  optional hadoop.ozone.OzoneAuthTokenProto authToken = 120;
}
```

**Backward compatibility:** The field is optional. Existing clients that do not set it continue to work via Kerberos UGI or `S3Authentication`. OM checks for token presence before falling back.

---

### 2.3 Authentication Provider Plugin Interface

Each provider handles one credential type and is loaded by the Auth Service at startup.

```java
/**
 * Plugin interface for authentication providers.
 * Implementations MUST be thread-safe after init() returns.
 *
 * Lifecycle: construct → init(conf) → authenticate() [concurrent] → close()
 */
public interface AuthenticationProvider extends Closeable {

    /** Human-readable name (e.g., "kerberos", "s3", "oidc"). */
    String name();

    /** Which credential types this provider handles. */
    Set<CredentialType> supportedTypes();

    /** One-time initialization (load keytabs, configure JWKS, etc.). */
    void init(OzoneConfiguration conf) throws AuthenticationException;

    /** Authenticate a credential. Returns identity on success. */
    AuthResult authenticate(CredentialEnvelope envelope)
        throws AuthenticationException;
}
```

**Supported providers:**

| Provider | Credential | Validation |
|:---------|:-----------|:-----------|
| `KerberosAuthProvider` | SPNEGO token | GSSContext via JAAS with Auth Service keytab |
| `S3SignatureAuthProvider` | accessId + signature + stringToSign | Recompute SigV4 against stored S3 secret |
| `OIDCAuthProvider` | JWT (access or ID token) | Validate signature via JWKS, check iss/aud/exp |
| `STSTokenAuthProvider` | Primary credential + role identifier | Validate primary, check role permission, scope |

**Provider loading:** Configured via `ozone.auth.providers` (comma-separated class names), following the same pattern as `OZONE_ACL_AUTHORIZER_CLASS` for the `IAccessAuthorizer`.

---

### 2.4 Authorization Backend Interface

After authentication, the Auth Service evaluates policy before issuing a token.

```java
/**
 * Authorization backend for the Auth Service.
 * Wraps the existing IAccessAuthorizer interface internally.
 */
public interface AuthorizationBackend {

    /** Evaluate whether the principal can perform the action on the resource. */
    AuthzDecision authorize(OzonePrincipal principal,
                            OzoneResource resource,
                            ACLType action);
}
```

**Implementations:**

| Backend | Behavior |
|:--------|:---------|
| `RangerAuthorizationBackend` | Delegates to existing Ranger plugin via `IAccessAuthorizer.checkAccess()` |
| `NativeACLAuthorizationBackend` | Evaluates Ozone native ACLs from OM metadata (read-only access) |

When the backend denies access, no token is issued. The client receives an authorization error and OM never sees the request.

---

### 2.5 Token Signing and Verification

#### Signing algorithm: HMAC-SHA256

| Aspect | Choice | Rationale |
|:-------|:-------|:----------|
| Algorithm | HMAC-SHA256 | ~100x faster than RSA verify; OM verifies every request |
| Key distribution | SCM `SecretKeyClient` / `ManagedSecretKey` | Already exists; distributes symmetric keys to services |
| Key rotation | SCM-managed, grace period overlap | Both old and new keys accepted during rotation window |

#### Signing input computation

The signing input is the protobuf serialization of all fields **except** `signature`, `signatureAlgorithm`, and `signingKeyId`:

```java
public byte[] computeSigningInput(OzoneAuthTokenProto.Builder token) {
    OzoneAuthTokenProto forSigning = token.clone()
        .clearSignature()
        .clearSigningKeyId()
        .clearSignatureAlgorithm()
        .build();
    return forSigning.toByteArray();
}
```

#### Verification at OM (TokenVerifier)

```java
/**
 * Verifies OzoneAuthTokens locally without network calls.
 * Thread-safe. Runs in the Ratis state machine path.
 * MUST NOT make any network calls.
 */
public class TokenVerifier {
    private final SecretKeyClient secretKeyClient;
    private final String expectedAudience;
    private final Clock clock;

    public VerifiedIdentity verify(OzoneAuthTokenProto token, OMRequest request)
            throws AuthTokenException {
        verifySignature(token);           // Step 1: cryptographic check
        verifyExpiry(token);              // Step 2: not expired
        verifyAudience(token);            // Step 3: correct OM cluster
        verifyCmdTypeBinding(token, request);   // Step 4: operation match
        verifyResourceBinding(token, request);  // Step 5: resource match
        verifyCanonicalHash(token, request);    // Step 6: optional param hash
        return new VerifiedIdentity(token.getSubject(),
            token.getGroupsList(), token.getAuthProvider());
    }
}
```

**Critical:** Signature comparison MUST use `MessageDigest.isEqual()` (constant-time) to prevent timing attacks. Never use `Arrays.equals()`.

---

### 2.6 OM Integration

#### Request handling flow change

Current identity resolution order in `OMClientRequest.getUserInfo()`:
1. `S3Authentication.accessId` present → resolve S3 identity
2. Kerberos UGI from RPC context → use Kerberos principal
3. gRPC context → use IP/hostname

New identity resolution order (insert at priority 0):
1. **`OMRequest.authToken` present → verify token, use token subject**
2. `S3Authentication.accessId` present → resolve S3 identity (legacy)
3. Kerberos UGI from RPC context → use Kerberos principal (legacy)
4. gRPC context → use IP/hostname

#### ACL skip for token-authenticated requests

When a valid `OzoneAuthToken` is present, the Auth Service has already performed authorization. OM skips `checkAcls()` to avoid double-evaluation:

```java
public class AuthTokenContext {
    private static final ThreadLocal<VerifiedIdentity> CURRENT = new ThreadLocal<>();

    public static void set(VerifiedIdentity identity) { CURRENT.set(identity); }
    public static boolean isPresent() { return CURRENT.get() != null; }
    public static void clear() { CURRENT.remove(); }
}
```

In `OzoneManager.checkAcls()`:
```java
if (AuthTokenContext.isPresent()) {
    return; // Auth Service already authorized this request
}
// Existing Ranger/ACL evaluation continues for non-token requests
```

The thread-local is always cleared in a `finally` block after request processing.

#### gRPC ServerInterceptor

For typed gRPC services (`KeyService`, `BucketService`), the token is carried in gRPC call metadata rather than inside `OMRequest`:

```java
public class OmGrpcAuthInterceptor implements ServerInterceptor {
    private static final Metadata.Key<byte[]> AUTH_TOKEN_KEY =
        Metadata.Key.of("ozone-auth-token-bin", Metadata.BINARY_BYTE_MARSHALLER);

    public static final Context.Key<VerifiedIdentity> CALLER_IDENTITY =
        Context.key("caller-identity");

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(
            ServerCall<T, R> call, Metadata headers, ServerCallHandler<T, R> next) {
        byte[] tokenBytes = headers.get(AUTH_TOKEN_KEY);
        if (tokenBytes == null) {
            call.close(Status.UNAUTHENTICATED, new Metadata());
            return new ServerCall.Listener<>() {};
        }
        OzoneAuthTokenProto token = OzoneAuthTokenProto.parseFrom(tokenBytes);
        VerifiedIdentity identity = tokenVerifier.verify(token);
        Context ctx = Context.current().withValue(CALLER_IDENTITY, identity);
        return Contexts.interceptCall(ctx, call, headers, next);
    }
}
```

The handler reads identity from context: `OmGrpcAuthInterceptor.CALLER_IDENTITY.get()`.

#### New OMException result code

```java
// In OMException.ResultCodes:
INVALID_AUTH_TOKEN
```

Returned when token verification fails for any reason (expired, invalid signature, binding mismatch, unknown signing key, replay detected).

---

### 2.7 RPC Protocol Support

The Auth Service supports both RPC protocols available in Ozone:

**Primary protocol — Hadoop RPC (default):**
```
Client → [Kerberos SASL] → OM Hadoop RPC → OMRequest with authToken field
```

Hadoop RPC remains the default protocol for all JVM clients. Kerberos SASL authenticates the transport connection. When the Auth Service is enabled, the client optionally obtains an `OzoneAuthToken` and attaches it in the `OMRequest.authToken` field. OM accepts both Kerberos-only and token-based authentication on this path.

**Secondary protocol — gRPC (typed services):**
```
Client → Auth Service (get token) → OM gRPC port → KeyService.RenameKey() + token in metadata
```

gRPC is available as a secondary protocol for typed services and lightweight non-JVM clients. Since gRPC does not support Kerberos/SASL natively, clients on this path authenticate through the Auth Service and carry the `OzoneAuthToken` in gRPC call metadata. The `OmGrpcAuthInterceptor` validates the token before the handler runs.

**Key design points:**
- The `OzoneAuthToken` format is shared across both protocols — `OMRequest.authToken` for Hadoop RPC, gRPC metadata for the gRPC path
- Typed gRPC services validate the token once per call; the handler receives only the verified identity
- Server-side streaming (`ListKeys`) works with per-session tokens since the token covers the operation type and resource prefix

---

### 2.8 S3 Gateway Migration

**Current flow:**
```
S3 Client → S3 Gateway (parse SigV4) → OMRequest{S3Authentication} → OM (re-verify sig)
```

**New flow with Auth Service:**
```
S3 Client → S3 Gateway (parse SigV4) → Auth Service (verify sig, issue token)
          → OMRequest{authToken} → OM (verify token only)
```

S3 Gateway calls `AuthService.Authenticate` with the S3 credential envelope (accessId, signature, stringToSign, signed headers). The Auth Service retrieves the S3 secret from OM metadata (or a local cache), verifies the signature, and returns an `OzoneAuthToken`. S3 Gateway attaches the token to `OMRequest` instead of `S3Authentication`.

**Fallback:** If the Auth Service is unreachable, S3 Gateway falls back to the legacy path (populating `S3Authentication` for OM to verify directly). This is configured via `ozone.s3g.auth.service.fallback.enabled` (default true).

**S3 secret storage:** S3 access key → secret mappings remain in OM metadata. The Auth Service reads them via a gRPC metadata lookup or a synced cache. `S3GetSecretRequest` / `S3RevokeSecretRequest` continue to manage secret lifecycle via OM.

---

### 2.9 REST/gRPC Gateway for Lightweight Clients

The Auth Service provides an optional gateway mode for non-JVM clients that cannot use Hadoop RPC or manage token lifecycle independently.

```protobuf
service OzoneAuthGateway {
  rpc ExecuteOperation(OperationRequest) returns (OperationResponse);
  rpc StreamOperation(OperationRequest) returns (stream OperationChunk);
}

message OperationRequest {
  optional CredentialProto credential = 1;
  optional string operationType = 2;   // e.g., "CreateKey", "ListKeys"
  optional string parameters = 3;      // JSON-encoded operation parameters
  optional string clientIp = 4;
  optional string sourceService = 5;
  optional string requestId = 6;
}

message OperationResponse {
  optional bool success = 1;
  optional string result = 2;          // JSON-encoded result
  optional string errorCode = 3;
  optional string errorMessage = 4;
  optional string requestId = 5;
  optional uint64 latencyMs = 6;
}
```

A REST endpoint wraps the gRPC service:
- `POST /v1/operations/{type}` — execute an operation with `Authorization` header
- `GET /v1/health` — health check
- `GET /v1/operations` — list available operations (discovery)

The `Authorization` header supports: `Bearer <jwt>` for OIDC, `AWS4-HMAC-SHA256 ...` for S3, `Negotiate <token>` for Kerberos.

---

### 2.10 Edge Cases and Security Considerations

#### Token expiry during Ratis replication

A token verified by the OM leader might expire before the operation is replicated to followers. Since token verification at OM is purely local (no cross-node coordination), each OM node verifies independently. To handle clock skew:
- OM applies a configurable clock skew tolerance (`ozone.auth.token.clock.skew.ms`, default 30000)
- The token TTL should be longer than the maximum expected Ratis replication latency

#### Key rotation overlap

During signing key rotation, both old and new keys must be accepted:
- SCM distributes the new key; old key enters a grace period
- Auth Service signs new tokens with the new key immediately
- OM accepts tokens signed with either key during the grace window
- After grace period, old key is removed; tokens signed with it are rejected

```
Time: ─────────────────────────────────────────────────────▶
Key A: ═══════════════╗
                      ║ grace period
Key B:          ╔═════╩═══════════════════════════════════
                ↑ rotation point
```

#### Replay protection sizing

The `ReplayProtector` maintains a bounded set of recently seen `tokenId` values:
- Only write operations are tracked (reads are idempotent)
- Set size is bounded by: `maxTokenTTL / averageWriteInterval`
- For 30s TTL with 1000 writes/sec = 30,000 entries (~1MB memory)
- Entries auto-expire after token TTL; periodic cleanup evicts stale entries

#### S3 Gateway fallback

When Auth Service is unreachable:
1. S3 Gateway detects connection failure or timeout
2. Falls back to legacy `S3Authentication` field in `OMRequest`
3. OM verifies the S3 signature directly (existing code path)
4. Logs a warning for operator visibility
5. Circuit breaker prevents repeated failed calls to Auth Service

#### Constant-time signature comparison

Token signature verification MUST use `MessageDigest.isEqual()` which performs byte-by-byte comparison in constant time regardless of where the first difference occurs. Standard `Arrays.equals()` short-circuits on the first mismatch, leaking information about the expected signature via timing side-channels.

#### Token not logged

The full token (especially signature bytes) must never appear in log output. Only `tokenId` and `subject` are safe to log for debugging and audit purposes.

#### Batch operations

For batch operations (e.g., `DeleteKeys` with multiple keys), the token's `resourceScope` covers the common parent at the appropriate level:
- `DeleteKeys` on `/vol1/bucket1/key1`, `/vol1/bucket1/key2` → scope is `/vol1/bucket1/*`
- If keys span multiple buckets, the scope widens to `/vol1/*`

---

## 3. Upgrades

### Phased migration plan

| Phase | Change | Risk | Backward Compatibility |
|:------|:-------|:-----|:-----------------------|
| Phase 0 | Deploy Auth Service alongside OM. No clients use it. | Zero — idle process | All existing paths unchanged |
| Phase 1 | S3 Gateway delegates S3 signature verification to Auth Service | Low | Fallback to legacy path available |
| Phase 2 | New gRPC clients authenticate via Auth Service | Zero | New capability, not migration |
| Phase 3 | OzoneFS client optionally exchanges Kerberos ticket for token | Low | Kerberos still works as fallback |
| Phase 4 | Legacy auth paths deprecated | Medium | Requires all clients migrated |

### Backward compatibility

- **Auth Service not deployed:** All existing auth paths work unchanged. `OMRequest.authToken` field is never set. Zero code changes required in clients or OM.
- **Auth Service deployed, legacy clients:** OM accepts both `OzoneAuthToken` (new) and legacy `S3Authentication`/Kerberos UGI (existing). Clients opt in via configuration (`ozone.client.auth.service.enabled`).
- **Mixed cluster during rolling upgrade:** OM nodes that have not yet been upgraded ignore the unknown field 120 in `OMRequest` (protobuf unknown field handling). Upgraded OM nodes check for the field and use it if present.

### Configuration gating

```java
// New OMLayoutFeature:
AUTH_SERVICE_TOKEN_SUPPORT(N, "Accept OzoneAuthToken in OMRequest for authentication");
```

Write-path behavior:
- Before finalization: OM rejects requests with `authToken` set (`NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION`)
- After finalization: OM accepts and verifies `authToken` when `ozone.auth.service.enabled=true`

---

## 4. Industry Patterns: Request-Bound Tokens in Distributed Systems

The Auth Service pattern draws from established systems that separate authentication from service execution:

### 4.1 Kubernetes TokenReview

Kubernetes API server validates bearer tokens by calling a TokenReview webhook. The webhook returns the authenticated user info (username, groups, UID). The API server then evaluates RBAC policies locally.

**Analogy to Ozone Auth Service:**
- TokenReview webhook ≈ Auth Service `Authenticate` RPC
- RBAC evaluation ≈ Ranger/ACL authorization backend
- Returned user info ≈ `VerifiedIdentity`
- API server local authz ≈ OM token binding verification

**Key difference:** Kubernetes tokens are session-scoped (valid for any API call). Ozone's `OzoneAuthToken` is operation-bound (valid for exactly one operation type on one resource). This prevents lateral movement — a stolen token for `ReadKey /vol1/b1/key1` cannot be reused for `DeleteVolume /vol1`.

### 4.2 Envoy ext_authz

Envoy proxy supports an external authorization filter (`ext_authz`) that calls a gRPC or HTTP service before forwarding a request upstream. The external service receives the full request context and returns allow/deny.

**Analogy:** The Auth Service is conceptually an `ext_authz` service for Ozone. The difference is that instead of sitting inline as a proxy (which adds latency to every request), the Auth Service issues a pre-authorized token that the client carries directly to OM.

### 4.3 Apache Knox

Apache Knox is a REST API gateway for Hadoop services. It authenticates users (Kerberos, LDAP, SAML, OAuth), applies topology-level authorization rules, and proxies requests to backend services using the service's Kerberos identity.

**Analogy:** Knox's topology = Auth Service provider configuration. Knox's service definitions = Auth Service gateway mode operation mapper.

**Key difference:** Knox proxies every request through itself (gateway pattern). The Auth Service primarily issues tokens (broker pattern) — clients carry tokens directly to OM. This avoids making the Auth Service a throughput bottleneck.

### 4.4 AWS STS (Security Token Service)

AWS STS issues temporary security credentials (access key + secret key + session token) with optional session policies. The credentials are valid for a configurable duration and can be scoped below the caller's permissions.

**Analogy:** The Auth Service STS/AssumeRole provider mirrors this pattern exactly:
- `AssumeRole` → scoped `OzoneAuthToken` with `sessionPolicy`
- Session policy intersection with principal permissions → same semantics
- Configurable duration → `expiryMs` in token

**Key difference:** AWS STS credentials are session-scoped (multiple API calls). Ozone tokens are per-operation. For use cases needing session semantics (e.g., a Spark job running thousands of reads), the client can cache and reuse tokens whose `resourceScope` covers a prefix.

---

## 5. Future Work

### Inter-service authentication via mTLS

The Auth Service addresses client-facing authentication. Inter-service authentication (SCM ↔ OM ↔ Datanode) is a separate concern. The recommended future approach is mutual TLS (mTLS) using SCM's existing internal Certificate Authority. SCM already issues X.509 certificates to all services via `CertificateClient`, and `InterSCMGrpcProtocolService` demonstrates the full mTLS pattern with `ClientAuth.REQUIRE`. Extending this to all service-to-service gRPC connections requires no new CA infrastructure.

### Custom enterprise providers via ServiceLoader

Beyond the built-in providers (Kerberos, S3, OIDC, STS), operators should be able to drop a JAR on the classpath that implements `AuthenticationProvider` and have it discovered automatically. The provider framework will support Java `ServiceLoader` for zero-configuration plugin loading.

### Token caching for read-heavy workloads

For workloads that perform many reads on the same resource prefix (e.g., Spark reading a partition), requesting a new token per operation adds unnecessary latency. A client-side token cache keyed by `(cmdType, resourceScope, allowedAction)` can reuse tokens until expiry. The cache must respect token TTL and invalidate on `policyEpoch` changes.

### Asymmetric signing for cross-domain verification

The current HMAC-SHA256 approach requires OM and Auth Service to share a symmetric key. For future scenarios where external systems need to verify Ozone tokens without possessing the signing key (e.g., cross-cluster federation), an asymmetric algorithm (RSA-PSS or Ed25519) could be added as an alternative `signatureAlgorithm`. The Auth Service signs with its private key; verifiers use the public key.

### Auth Service as Ranger policy decision point

Currently, Ranger evaluation happens inside OM for non-token requests. In the future, the Auth Service could become the sole Ranger policy decision point — OM would no longer need direct Ranger connectivity. This simplifies the OM deployment (no Ranger plugin JARs needed in OM classpath) and centralizes policy caching in the Auth Service.

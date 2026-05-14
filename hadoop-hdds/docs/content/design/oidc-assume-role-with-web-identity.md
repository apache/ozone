---
title: OIDC AssumeRoleWithWebIdentity for Ozone STS
summary: Web identity support for Ozone STS using OIDC and Ranger authorization
date: 2026-05-13
status: proposed
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

# OIDC AssumeRoleWithWebIdentity for Ozone STS

## Status

Proposed staged implementation.

This document narrows the previous broad OIDC direction to an upstream-friendly
MVP: extend the Ozone STS temporary S3 credential model with an AWS-compatible
`AssumeRoleWithWebIdentity` action.

## Problem

Secure Ozone S3 deployments currently depend on Kerberos-backed identities for
S3 credential issuance. Kubernetes workloads commonly already have OIDC tokens
from Keycloak or another IdP, but do not have an easy Kerberos bootstrap path.

The target is not a Kerberos-free Ozone cluster. The target is a narrow STS
exchange:

1. Keycloak authenticates the caller and issues a signed OIDC JWT.
2. Ozone STS validates the JWT locally using JWKS.
3. Ranger or the configured Ozone authorizer decides whether the identity may
   assume the requested role.
4. Ozone STS issues temporary S3 credentials.
5. S3 Gateway and OM validate those temporary credentials and authorize object
   operations using the assumed identity and session context.

Ranger remains the authorization source of truth. Keycloak roles and groups are
identity attributes only.

## Current STS And S3 Security Path

This design is based on the `origin/HDDS-13323-sts` branch at commit
`37a224b217`, which contains the STS runtime that was missing from earlier
base branches.

The current STS runtime contains:

- `/sts` HTTP endpoint in `org.apache.hadoop.ozone.s3sts.S3STSEndpoint`;
- endpoint authentication setup in `S3STSEndpointBase`;
- AWS STS `AssumeRole` XML response model in `S3AssumeRoleResponseXml`;
- S3G to OM client path through `ObjectStore`, `ClientProtocol`, `RpcClient`,
  `OzoneManagerProtocol`, and
  `OzoneManagerProtocolClientSideTranslatorPB.assumeRole()`;
- OM request handling in
  `org.apache.hadoop.ozone.om.request.s3.security.S3AssumeRoleRequest`;
- OM response handling in `S3AssumeRoleResponse`;
- session token identifier and secret manager in `STSTokenIdentifier`,
  `STSTokenSecretManager`, and `STSSecurityUtil`;
- revoked STS token metadata and cleanup through `S3RevokeSTSTokenRequest`,
  `S3DeleteRevokedSTSTokensRequest`, and
  `RevokedSTSTokenCleanupService`;
- authorization extension points in `AssumeRoleRequest`,
  `IAccessAuthorizer.generateAssumeRoleSessionPolicy()`, and
  `RequestContext.sessionPolicy`.

The current S3 request authentication path is:

- S3G parses AWS SigV4 in `AuthorizationFilter`,
  `SignatureProcessor`, `AuthorizationV4HeaderParser`,
  `AuthorizationV4QueryParser`, and `StringToSignProducer`.
- `EndpointBase` creates `S3Auth` from the parsed access key, signature, and
  string-to-sign, then stores it in the `ClientProtocol` thread-local.
- `OzoneManagerProtocolClientSideTranslatorPB` copies `S3Auth` into
  `OMRequest.s3Authentication`.
- `AWSSignatureProcessor` extracts `x-amz-security-token` for temporary
  credentials into `SignatureInfo.sessionToken`.
- `EndpointBase` and `S3STSEndpointBase` propagate the session token into
  `S3Auth`.
- `OzoneManagerProtocolClientSideTranslatorPB` copies `S3Auth` into
  `OMRequest.s3Authentication`.
- `S3SecurityUtil.validateS3Credential()` validates either permanent S3
  credentials or STS temporary credentials.
- For STS credentials, `STSSecurityUtil` decodes, validates, and decrypts the
  session token, then OM validates the SigV4 signature with the temporary
  secret access key.
- `OmMetadataReader` attaches STS session policy from the OM thread-local
  `STSTokenIdentifier` to `RequestContext` for subsequent authorization.

The current permanent S3 credential storage path is:

- `S3SecretManager` and `S3SecretManagerImpl`.
- `S3SecretValue`.
- OM metadata S3 secret table via `OmMetadataManagerImpl`.
- `ozone s3 getsecret`, `setsecret`, and `revokesecret` client paths.

## Dependency On Existing Ozone STS Runtime

`AssumeRoleWithWebIdentity` must be an incremental extension of the existing
`AssumeRole` runtime in `origin/HDDS-13323-sts`. It must not introduce a second
STS endpoint, a separate S3 authentication system, or local S3G-only temporary
credential state.

Existing runtime:

```text
AssumeRole
  -> S3 SigV4-authenticated /sts request
  -> OM S3AssumeRoleRequest
  -> temporary access key / secret / session token
  -> STSSecurityUtil validation on later S3 requests
  -> RequestContext.sessionPolicy
  -> Ranger or configured authorizer
```

New runtime:

```text
AssumeRoleWithWebIdentity
  -> unauthenticated /sts bootstrap request only for this action
  -> OM validates Keycloak/OIDC JWT
  -> OM authorizes role assumption through Ranger or configured authorizer
  -> existing temporary credential issuer / session token path
  -> existing STSSecurityUtil validation on later S3 requests
  -> RequestContext.sessionPolicy and assumed identity
  -> Ranger or configured authorizer
```

The OM runtime slice adds the WebIdentity request/protobuf path while preserving
the existing `AssumeRole` flow. S3G parses and routes
`Action=AssumeRoleWithWebIdentity`, but OM remains the authoritative validator
and issuer.

The raw `WebIdentityToken` is accepted only in the external OM RPC request. The
OM leader validates it in `S3AssumeRoleWithWebIdentityRequest.preExecute()`,
maps claims into a sanitized identity/session request, authorizes role
assumption, generates temporary credential material using the existing STS
helpers, and returns an `UpdateAssumeRoleWithWebIdentityRequest` for Ratis
replication. The replicated request must not contain the raw JWT.

`validateAndUpdateCache()` consumes only the sanitized update request. It must
not call Keycloak, refresh JWKS, revalidate JWTs, or otherwise depend on current
external IdP state during Ratis apply or replay. Credential expiration is
computed by the leader before replication and stored as
`credentialExpirationEpochSeconds` so replay does not depend on the apply-time
clock.

Temporary credentials must not be stored only in S3G memory. S3G can have
multiple replicas and can restart. The issuing and validation authority must be
OM-backed, persisted in Ozone metadata, or based on self-contained signed tokens
whose signing keys are rotation-safe and available to all validating components.

## Endpoint Placement

The existing STS runtime places `/sts` on the S3 Gateway HTTP/HTTPS port.
WebIdentity follows that placement: S3G exposes the AWS-compatible STS API
surface, while OM remains authoritative for JWT validation, identity mapping,
role-assumption authorization, credential issuance, revocation, and later
temporary credential validation.

Because existing `/sts` `AssumeRole` is protected by the normal S3 SigV4
`AuthorizationFilter`, `AssumeRoleWithWebIdentity` needs a narrow bootstrap
exception:

- only for the STS application path;
- only for `Action=AssumeRoleWithWebIdentity`;
- only when `ozone.sts.web.identity.enabled=true`;
- never for normal S3 object APIs;
- never for existing `AssumeRole` or other STS actions.

This exception must not make S3G a JWT source of truth. S3G may parse and route
the request, but it must forward the web identity token and request context to
OM. OM validates the JWT itself and issues the credentials.

## RoleArn Semantics

The current `AssumeRoleRequest` model contains `targetRoleName`, not a full AWS
IAM role database. No role metadata store or IAM-like role lifecycle was found
in this tree. `RoleArn` should therefore be treated as the authorization
resource and request context for Ranger or the configured Ozone authorizer in
the MVP.

The Web Identity patch must not invent a new IAM role database. If the STS
runtime already defines role ARN parsing or role-name normalization, Web
Identity should reuse it. Otherwise, `RoleArn` remains an opaque policy resource
for the authorizer and for audit/session context.

## New Flow

`AssumeRoleWithWebIdentity` is handled by the Ozone STS endpoint.

Request parameters:

- `Action=AssumeRoleWithWebIdentity`
- `RoleArn=<role>`
- `RoleSessionName=<session>`
- `WebIdentityToken=<OIDC JWT>`
- `DurationSeconds=<optional>`
- `Policy=<optional, only if the existing STS AssumeRole session policy path is
  implemented cleanly>`
- `ProviderId=<optional compatibility field>`

Flow:

1. The client or workload obtains an OIDC access token from Keycloak.
2. The client calls Ozone STS with `AssumeRoleWithWebIdentity`.
3. Ozone STS rejects the request unless `ozone.sts.web.identity.enabled=true`.
4. S3G validates only the STS request shape that is safe to validate at the
   edge: action, version, role ARN syntax, role session name, duration bounds,
   and presence of `WebIdentityToken`.
5. S3G forwards `RoleArn`, `RoleSessionName`, `WebIdentityToken`,
   `DurationSeconds`, `ProviderId`, and request context to OM in the external
   RPC request only.
6. OM validates the JWT:
   - token is a signed JWT;
   - `alg=none` is rejected;
   - signature validates against the configured JWKS;
   - `iss` equals `ozone.sts.web.identity.issuer.uri`;
   - configured audience is present;
   - `exp`, `nbf`, and `iat` are validated with configured clock skew;
   - configured username and subject claims are present.
7. OM maps claims into an Ozone identity:
   - username;
   - subject;
   - issuer;
   - groups;
   - roles;
   - token expiration.
8. OM builds an assume-role authorization request and calls Ranger or the
   configured Ozone authorizer before issuing any credential.
9. OM strips the raw JWT before Ratis replication and submits only sanitized
   identity/session fields:
   - role ARN and role session name;
   - provider id;
   - effective user;
   - subject, issuer, audience;
   - groups and roles;
   - web identity token expiration;
   - token fingerprint;
   - requested/effective duration;
   - credential expiration;
   - derived session policy.
10. If authorized, OM issues temporary S3 credentials:
   - `Credentials.AccessKeyId`;
   - `Credentials.SecretAccessKey`;
   - `Credentials.SessionToken`;
   - `Credentials.Expiration`;
   - `SubjectFromWebIdentityToken`;
   - `AssumedRoleUser`;
   - `Audience`;
   - `Provider`.
11. The client uses those credentials with ordinary AWS SigV4 against S3G.
12. S3G and OM validate the temporary credential, recover the assumed identity
   and session policy, and pass them to the authorizer for every S3 operation.

## Configuration

The MVP uses STS-focused configuration keys:

```properties
ozone.sts.web.identity.enabled=false
ozone.sts.web.identity.issuer.uri=
ozone.sts.web.identity.jwks.uri=
ozone.sts.web.identity.audience=
ozone.sts.web.identity.username.claim=preferred_username
ozone.sts.web.identity.subject.claim=sub
ozone.sts.web.identity.groups.claim=groups
ozone.sts.web.identity.roles.claim=realm_access.roles
ozone.sts.web.identity.clock.skew=60s
ozone.sts.web.identity.jwks.refresh.interval=10m
ozone.sts.web.identity.jwks.connect.timeout=5s
ozone.sts.web.identity.jwks.read.timeout=5s
ozone.sts.web.identity.jwks.size.limit=1MB
ozone.sts.web.identity.require.https=true
ozone.sts.web.identity.allow.insecure.http.for.tests=false
```

The feature is opt-in. When disabled, Kerberos, existing S3 SigV4 handling,
existing S3 secret handling, and non-secure mode behavior are unchanged.

## OIDC Validation

The reusable validation module is intentionally small:

- `AuthCredentials` wraps bearer token material and redacts it in `toString()`.
- `OzoneIdentity` carries normalized username, subject, issuer, groups, roles,
  auth method, authentication time, expiration time, and raw claims.
- `OidcJwtIdentityProvider` validates signed JWTs and maps claims.
- `JwksProvider`, `JwksFetcher`, `UrlJwksFetcher`, and
  `CachingJwksProvider` load and cache JWKS with refresh-on-unknown-kid
  behavior.
- `OidcAuthenticationException` fails closed without embedding the raw token in
  exception messages.

The module does not call Keycloak for every S3 request. JWKS validation is local,
with refresh on cache expiry and unknown key id. The default JWKS refresh
interval is 10 minutes. Unknown key ids may trigger an earlier refresh, but
repeated unknown kids are debounced to avoid refresh storms from attacker
supplied token headers. JWKS fetches use bounded connect/read timeouts and a
bounded response size.

## Ranger Authorization Points

The first authorization point is credential issuance. Before generating
temporary credentials, Ozone must call the configured authorizer with:

- user: mapped OIDC username;
- groups: mapped OIDC groups;
- action: `AssumeRoleWithWebIdentity`;
- resource: `RoleArn` or normalized Ozone role resource;
- context: issuer, subject, audience, role session name, provider id,
  requested duration, and client IP/host if available.

Deny is the default. If Ranger denies or the authorizer cannot decide, Ozone
returns `AccessDenied` and does not issue credentials.

The common request-shape extension point is
`AssumeRoleWithWebIdentityRequest`, with
`IAccessAuthorizer.generateAssumeRoleWithWebIdentitySessionPolicy()` as the
default authorizer hook. Existing authorizers are not forced to implement this
immediately because the new method has a fail-closed default implementation.
For production Ranger deployments, the external Ranger Ozone plugin must add a
companion override for this method. The `RangerOzoneAuthorizer` class is
provided by Apache Ranger, not this Ozone repository. Without a WebIdentity
capable Ranger/Ozone authorizer, the default hook returns
`NOT_SUPPORTED_OPERATION`, Ozone fails closed, and no WebIdentity temporary
credentials are issued.

The second authorization point is every S3 operation made with the temporary
credentials. OM must recover the assumed identity and session policy from the
session token and build a `RequestContext` carrying:

- `clientUgi` for the assumed OIDC username and groups;
- `sessionPolicy` returned by the assume-role authorization step;
- `s3Action` mapped from the S3 endpoint method;
- bucket/key resource information.

Ranger evaluates normal resource/action policies using that context.

## Temporary Credential Lifecycle

Temporary credentials must:

- expire no later than the requested `DurationSeconds` and Ozone's configured
  STS maximum;
- require `x-amz-security-token` or the equivalent SigV4 query parameter;
- fail closed if the session token is unknown, expired, revoked, malformed, or
  fails signature/MAC verification;
- never log the secret access key, session token, WebIdentityToken, refresh
  token, or client secret;
- map back to the assumed OIDC identity and role session context;
- preserve the authorizer session policy used for subsequent S3 authorization.

The existing STS runtime uses self-contained session tokens containing the
encrypted secret access key, original identity, role ARN, session policy,
expiration, signing key id, and MAC. `AssumeRoleWithWebIdentity` should reuse
that issuer and validator instead of creating a parallel token format.
The sanitized replicated OM request still carries temporary credential material
through Ratis in the same way as the existing `AssumeRole` implementation.
Operators must protect OM metadata, Ratis logs, snapshots, and backups as
sensitive security material. The raw WebIdentity JWT must not be replicated or
stored in OM metadata.

In `origin/HDDS-13323-sts`, `STSTokenIdentifier` stores the
`originalAccessKeyId` because `AssumeRole` starts from an existing S3 access
key. `AssumeRoleWithWebIdentity` has no permanent S3 access key. The token
model must therefore be extended backward-compatibly, for example with an
`authType` field plus optional WebIdentity fields:

- `authType=ASSUME_ROLE` for existing tokens, with `originalAccessKeyId`
  preserved;
- `authType=WEB_IDENTITY` for new tokens, with effective user, groups, issuer,
  subject, audience, role ARN, role session name, provider id, and session
  policy;
- old `AssumeRole` tokens must continue to deserialize and validate exactly as
  before;
- `STSSecurityUtil.ensureEssentialFieldsArePresentInToken()` must not require
  `originalAccessKeyId` for `WEB_IDENTITY` tokens, but must still require all
  fields needed to validate signatures and authorize later S3 operations.

Revocation should follow the STS design: store revoked session token identifiers
in OM metadata and fail closed if revocation status cannot be checked.

## AWS-Compatible Response

The XML response should follow the AWS STS shape where practical:

```xml
<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <SubjectFromWebIdentityToken>...</SubjectFromWebIdentityToken>
    <Audience>ozone</Audience>
    <Provider>https://keycloak.example.com/realms/ozone</Provider>
    <AssumedRoleUser>
      <Arn>...</Arn>
      <AssumedRoleId>...</AssumedRoleId>
    </AssumedRoleUser>
    <Credentials>
      <AccessKeyId>...</AccessKeyId>
      <SecretAccessKey>...</SecretAccessKey>
      <SessionToken>...</SessionToken>
      <Expiration>...</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>
```

Errors should use STS/S3-compatible codes where possible:

- invalid or expired JWT: `InvalidIdentityToken`;
- disabled feature: `AccessDenied` or `InvalidAction`;
- unauthorized role assumption: `AccessDenied`;
- unsupported optional parameter: `InvalidParameterValue`;
- internal validation or revocation failures: fail closed.

## Security Model

This feature does not replace Kerberos for daemon authentication or the broader
Ozone secure cluster model.

Security boundaries:

- Keycloak authenticates the caller by signing JWTs.
- Ozone STS validates the token and issues short-lived S3 credentials.
- Ranger or the configured Ozone authorizer decides whether the caller can
  assume a role and what object-store actions are allowed.
- Ozone S3G/OM enforce the temporary credential and Ranger decisions.

Required protections:

- TLS is required for production STS and Keycloak endpoints.
- Unsigned JWTs and `alg=none` are rejected.
- Incorrect issuer, audience, signature, time claims, or required claims are
  rejected.
- JWKS rotation is handled by cache refresh and refresh-on-unknown-kid.
- Web identity token and temporary credentials are never logged.
- Direct OM/SCM/DN access is still governed by existing Ozone security. This
  MVP only adds an STS path for temporary S3 credentials.

## Non-Goals

The MVP explicitly does not include:

- full OIDC-only secure Ozone cluster;
- replacing Kerberos daemon login;
- OFS OIDC login;
- `ozone auth login --oidc`;
- device-code flow;
- Keycloak Authorization Services as the object-store PDP;
- replacing Ranger with Keycloak roles;
- daemon-to-daemon OIDC authentication.

## Test Strategy

Unit tests:

- valid JWT from a test RSA key validates successfully;
- expired JWT fails;
- wrong issuer fails;
- wrong audience fails;
- wrong signature fails;
- `alg=none` fails;
- manipulated groups claim fails because the signature no longer matches;
- unknown `kid` triggers JWKS refresh or fails safely;
- username, subject, groups, and roles claim mapping works;
- token material is not present in exceptions.

STS authorization tests:

- fake authorizer sees user, groups, action `AssumeRoleWithWebIdentity`, role
  resource, issuer, subject, audience, and session name;
- allowed identity receives temporary credentials;
- denied identity receives `AccessDenied`;
- no credential is generated before authorization succeeds.

Temporary credential tests:

- credentials require a session token;
- expired credentials fail;
- tampered session token fails;
- unknown/revoked session token fails;
- allowed bucket operation succeeds with allowed role/session policy;
- denied bucket operation fails.

Integration tests:

- Keycloak Testcontainers or docker-compose realm `ozone-test`;
- client `ozone-sts`;
- users `tomato-user` and `denied-user`;
- group `ozone-tomato`;
- `tomato-user` token includes `preferred_username`, `sub`, `groups`,
  `realm_access.roles`, and `aud=ozone`;
- real Keycloak JWT validates with Ozone provider;
- fake/Ranger authorizer allows `tomato-user` to assume a test role and denies
  `denied-user`.

Full Ranger container testing is optional for the MVP. Unit and mock-layer tests
must prove request shape and fail-closed behavior.

## Migration And Future Work

Migration path:

1. Existing Kerberos and S3 secret behavior remains unchanged.
2. Operators enable `ozone.sts.web.identity.enabled=true` for STS only.
3. Workloads exchange Keycloak JWTs for temporary S3 credentials.
4. Ranger policies grant role assumption and object access.

Future work:

- reuse the OIDC validation module for OIDC-to-Ozone delegation tokens for
  OFS/CLI;
- add daemon authentication without Kerberos via mTLS, SPIFFE, Kubernetes
  ServiceAccount JWTs, or Keycloak client credentials;
- add hybrid Kerberos plus OIDC migration mode if broader Ozone authentication
  is pursued;
- improve AWS STS API compatibility;
- add an optional real Ranger integration test profile.

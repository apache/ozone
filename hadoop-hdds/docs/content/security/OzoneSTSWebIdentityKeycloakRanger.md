---
title: "Using Ozone STS AssumeRoleWithWebIdentity with Keycloak and Ranger"
date: "2026-05-14"
summary: Exchange Keycloak/OIDC web identity tokens for temporary Ozone S3 credentials through Ozone STS.
weight: 6
menu:
   main:
      parent: Security
icon: key
---
<!---
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

Ozone STS can exchange an OIDC web identity token for short-lived S3
credentials using an AWS-compatible `AssumeRoleWithWebIdentity` request. This
is intended for deployments where workloads already authenticate to Keycloak or
another OIDC provider and need temporary credentials for Ozone S3.

This feature is disabled by default.

## Architecture

The Web Identity flow has three separate responsibilities:

1. Keycloak authenticates the caller and issues a signed OIDC JWT.
2. Ozone STS validates the JWT and issues temporary S3 credentials.
3. Ranger or the configured Ozone authorizer authorizes role assumption and
   subsequent S3 access.

Keycloak groups and roles are identity attributes only. They are not the final
bucket or object policy engine. Ranger policies or the configured Ozone
authorizer remain the policy decision point (PDP) and source of authorization
decisions.

The request path is:

```text
Client or workload
  -> Keycloak access token
  -> Ozone STS AssumeRoleWithWebIdentity
  -> temporary AccessKeyId, SecretAccessKey, SessionToken
  -> normal S3 SigV4 request with x-amz-security-token
  -> OM STS token validation
  -> Ranger or Ozone authorizer
```

## What This Adds

This feature adds `AssumeRoleWithWebIdentity` to the existing Ozone STS
temporary credential model. Ozone STS validates the configured issuer,
audience, JWT signature, expiry, not-before time, issued-at time, username
claim, subject claim, and configured group and role claims.

The returned credentials use the existing STS session-token validation path for
later S3 operations. S3 clients must sign normal S3 requests with AWS Signature
Version 4 and include the returned session token as `x-amz-security-token`.

## What This Does Not Add

This feature does not replace Kerberos daemon authentication, does not add OFS
OIDC login, does not add CLI device-code login, does not make non-secure Ozone
fully secure, and does not use Keycloak Authorization Services as the Ozone
bucket or object policy decision point.

## Ozone Configuration

Enable Web Identity support only on clusters where STS and the S3 Gateway are
configured and the authorization provider can authorize STS role assumption and
subsequent S3 access.

```xml
<property>
  <name>ozone.sts.web.identity.enabled</name>
  <value>true</value>
</property>
<property>
  <name>ozone.sts.web.identity.issuer.uri</name>
  <value>https://keycloak.example.com/realms/ozone</value>
</property>
<property>
  <name>ozone.sts.web.identity.jwks.uri</name>
  <value>https://keycloak.example.com/realms/ozone/protocol/openid-connect/certs</value>
</property>
<property>
  <name>ozone.sts.web.identity.audience</name>
  <value>ozone</value>
</property>
<property>
  <name>ozone.sts.web.identity.username.claim</name>
  <value>preferred_username</value>
</property>
<property>
  <name>ozone.sts.web.identity.subject.claim</name>
  <value>sub</value>
</property>
<property>
  <name>ozone.sts.web.identity.groups.claim</name>
  <value>groups</value>
</property>
<property>
  <name>ozone.sts.web.identity.roles.claim</name>
  <value>realm_access.roles</value>
</property>
<property>
  <name>ozone.sts.web.identity.require.https</name>
  <value>true</value>
</property>
<property>
  <name>ozone.sts.web.identity.jwks.refresh.interval</name>
  <value>10m</value>
</property>
<property>
  <name>ozone.sts.web.identity.jwks.connect.timeout</name>
  <value>5s</value>
</property>
<property>
  <name>ozone.sts.web.identity.jwks.read.timeout</name>
  <value>5s</value>
</property>
<property>
  <name>ozone.sts.web.identity.jwks.size.limit</name>
  <value>1MB</value>
</property>
```

For local tests only, HTTP issuer and JWKS URLs can be enabled explicitly by
turning off the HTTPS requirement:

```xml
<property>
  <name>ozone.sts.web.identity.require.https</name>
  <value>false</value>
</property>
```

Production deployments should use HTTPS for both Keycloak and Ozone endpoints.

JWKS keys are cached. The default refresh interval is 10 minutes. A token with
an unknown `kid` can trigger an earlier JWKS refresh, but repeated unknown
`kid` values are debounced to avoid refresh storms. During Keycloak signing key
rotation, new tokens may fail until Ozone refreshes JWKS or sees the first
unknown `kid` after the debounce window. Operators should publish old and new
keys concurrently for at least the maximum token lifetime plus the JWKS refresh
interval.

## Keycloak Setup

A minimal Keycloak setup contains:

- Realm: `ozone`
- Client: `ozone-sts`
- Audience mapper: include `ozone` in access tokens
- Group membership mapper: include groups in the `groups` claim
- Users and groups used by Ranger policies, for example user `tomato-user` in
  group `ozone-tomato`

The token presented to Ozone STS must contain claims compatible with the Ozone
configuration:

```json
{
  "iss": "https://keycloak.example.com/realms/ozone",
  "aud": "ozone",
  "sub": "ce3f0b9b-...",
  "preferred_username": "tomato-user",
  "groups": ["ozone-tomato"],
  "realm_access": {
    "roles": ["offline_access"]
  }
}
```

## Ranger Policy Model

The authorizer must allow the mapped OIDC identity to perform
`AssumeRoleWithWebIdentity` on the requested role ARN before Ozone issues any
temporary credential. The role ARN is treated as an authorization resource for
this MVP. This patch does not add an IAM role database.

Production Ranger deployments need a WebIdentity-capable Ozone authorizer. In
this source tree, `IAccessAuthorizer` provides the
`generateAssumeRoleWithWebIdentitySessionPolicy(...)` extension point and its
default implementation fails closed with `NOT_SUPPORTED_OPERATION`. The
`org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer`
implementation is supplied by the external Ranger Ozone plugin, not by this
repository. Until that plugin overrides the WebIdentity method, deployments
using it will fail closed and will not issue temporary credentials for
`AssumeRoleWithWebIdentity`.

The authorizer must also allow the later S3 operations. In a Ranger deployment,
the recommended shape is:

- STS assumption policy: allow group `ozone-tomato` to assume the configured
  role ARN.
- S3 resource policy: allow group `ozone-tomato` to access the intended
  volume, bucket, key, or prefix.
- Deny by default for identities and buckets not covered by policy.

## STS Request

The client posts a form-encoded request to the Ozone STS endpoint:

```bash
curl -sS -X POST "https://s3g.example.com:9881/sts" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "Action=AssumeRoleWithWebIdentity" \
  --data-urlencode "RoleArn=arn:aws:iam::123456789012:role/tomato-role" \
  --data-urlencode "RoleSessionName=tomato-session" \
  --data-urlencode "WebIdentityToken=${KEYCLOAK_ACCESS_TOKEN}" \
  --data-urlencode "DurationSeconds=3600"
```

The bootstrap request does not use S3 SigV4. The unauthenticated bypass is
limited to `/sts`, only for `Action=AssumeRoleWithWebIdentity`, and only when
`ozone.sts.web.identity.enabled=true`. OM still validates the JWT itself before
issuing credentials.

## STS Response

The response follows the AWS STS shape where practical:

```xml
<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>...</AccessKeyId>
      <SecretAccessKey>...</SecretAccessKey>
      <SessionToken>...</SessionToken>
      <Expiration>2026-05-14T12:00:00Z</Expiration>
    </Credentials>
    <SubjectFromWebIdentityToken>...</SubjectFromWebIdentityToken>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/tomato-role/tomato-session</Arn>
      <AssumedRoleId>...</AssumedRoleId>
    </AssumedRoleUser>
    <Audience>ozone</Audience>
    <Provider>https://keycloak.example.com/realms/ozone</Provider>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>
```

## Using Temporary Credentials

AWS-compatible clients must use all three returned credential fields:

```bash
export AWS_ACCESS_KEY_ID="returned-access-key"
export AWS_SECRET_ACCESS_KEY="returned-secret-key"
export AWS_SESSION_TOKEN="returned-session-token"
export AWS_DEFAULT_REGION="us-east-1"

aws --endpoint-url https://s3g.example.com:9878 s3 cp ./file.txt s3://tomato-files/file.txt
aws --endpoint-url https://s3g.example.com:9878 s3 ls s3://tomato-files/
aws --endpoint-url https://s3g.example.com:9878 s3 cp s3://tomato-files/file.txt ./file.txt
```

If `AWS_SESSION_TOKEN` is missing, wrong, expired, or encoded in a
non-canonical form, STS temporary credential validation fails closed.

## Security Notes

The raw web identity JWT is validated by OM before Ratis replication and is
stripped before the replicated request is written. The replicated request and
STS token identifier contain sanitized identity and session fields, not the raw
JWT.

The temporary `SecretAccessKey` and returned session token are protected by the
existing STS token path. The sanitized replicated OM request still carries
temporary credential material through Ratis similarly to the existing
`AssumeRole` implementation, because the credentials are generated before the
state-machine apply path. Operators must protect OM metadata directories, Ratis
logs, snapshots, and backups as sensitive security material.

Do not log bearer tokens, session tokens, temporary secrets, client secrets, or
Authorization headers. Ozone errors should not include token material.

Temporary credentials expire. The effective expiration is constrained by the
STS duration limits and the web identity token lifetime.

Protect Ozone endpoints according to the deployment model. This feature does
not make direct unauthenticated OM, SCM, DataNode, OFS, or internal RPC access
safe. It only adds an OIDC-to-temporary-S3-credentials exchange path for Ozone
STS.

---
title: AWS STS Design for Ozone S3
summary: STS Support in Ozone
date: 2025-10-30
jira: HDDS-13323
status: implementing
author: Madhan Neethiraj, Ren Koike, Fabian Morgan, Stephen O'Donnell, Istvan Fajth, Uma Maheswara Rao Gangumalla
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

# AWS STS Design for Ozone S3

# 1. Introduction

S3 credentials used to communicate with Ozone S3 APIs are based on a Kerberos identity.

Historically, the Ozone community has had interest in a REST API capable of programmatically generating 
temporary S3 credentials.

Amazon AWS has the [Security Token Service (STS)](https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html) which 
provides the ability to generate short-lived access to resources.

The primary scope of this document is to detail the initial implementation of STS within the Ozone ecosystem.

# 2. Why Use STS Tokens?

Providing short-lived access to various resources in Ozone is useful in scenarios such as Data Lake
solutions that want to aggregate data across multiple cloud providers.

# 3. How Ozone STS Works

The initial implementation of Ozone STS supports only the [AssumeRole](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)
API from the AWS specification.  A new STS endpoint `/sts` on port `9880` (port `9881` for https) will be created to service STS requests in the S3 Gateway.
We use a separate port for STS to align with AWS so we don't have conflicts at a later time.  This means we have:
- Admin port for Ozone specific S3 admin operations
- STS port for STS APIs, analogous to AWS' separate STS endpoint
- Existing dedicated port/endpoint for S3 object APIs.

Furthermore, the initial implementation of Ozone STS focuses only on Apache Ranger for authorization in the first phase, 
as it aligns more with IAM policies.  Support for the Ozone Native Authorizer may be provided in a future phase.

## 3.1 Capabilities

The Ozone STS implementation has the following capabilities:

- Create temporary credentials that last from a minimum of 15 minutes to a maximum of 12 hours. The
return value of the AssumeRole call will be temporary credentials consisting of 3 components: 
  - accessKeyId - a generated String identifier (cryptographically strong using SecureRandom) beginning with the sequence "ASIA"
  - secretAccessKey - a generated String password (cryptographically strong using SecureRandom)
  - sessionToken - a Base64-encoded opaque String identifier
- The temporary credentials will have the permissions associated with a role. Furthermore, an 
[AWS IAM Session Policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session) can 
**optionally** be sent in the AssumeRole API call to limit the scope of the permissions further.  If 
an IAM policy is specified, the temporary credential will have the permissions comprising the intersection of the role permissions
and the IAM policy permissions. **Note:** If the IAM policy is specified and does not grant any permissions, then
the generated temporary credentials won't have any permissions and will essentially be useless.

## 3.2 Limitations in AssumeRole API Support

The AWS AssumeRole API has various required and optional fields.  We will support the two required fields, i.e. `RoleArn` 
and `RoleSessionName`.  Additionally, we will support the following optional fields (all others will be rejected):
- `DurationSeconds`
- `Policy`

## 3.3 Limitations in IAM Session Policy Support

The AWS IAM policy specification is vast and wide-ranging.  The initial Ozone STS implementation supports a limited
subset of its capabilities.  The restrictions are outlined below:

- The only supported prefix in ResourceArn is `arn:aws:s3:::` - all others will be rejected.  **Note**: a ResourceArn 
of `*` is supported as well.
- The only supported Condition operator is `StringEquals` - all others will be rejected.
- The only supported Condition key is `s3:prefix` - all others will be rejected.
- Only one Condition operator per Statement is supported - a Statement with more than one Condition will be rejected.
- The only supported Effect is `Allow` - all others will be rejected.
- If a (currently) unsupported S3 action is requested, such as `s3:GetAccelerateConfiguration`, it will be silently ignored.
Similarly, an invalid S3 action will be silently ignored.
- Supported wildcard expansions in Actions are: `s3:*`, `s3:Get*`, `s3:Put*`, `s3:List*`,
`s3:Create*`, and `s3:Delete*`.

A sample IAM policy that allows read access to all objects in the `example-bucket` bucket is shown below:
```JSON
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::example-bucket/*"
        }
    ]
}

```

### 3.3.1 Additional Context on Design Behavior

As mentioned above, some limitations in IAM Session Policy support result in API calls being rejected, while others are
silently ignored.  This design behavior came after (much) discussion with external teams. One external team will send 
S3 actions that Ozone doesn't support, and they don't have flexibility to change what they are sending, so that's one 
reason for the silent ignore. This external team also mentioned that some research indicates AWS also does not fail 
the AssumeRole request just because the inline session policy references unknown or unsupported actions, but rather 
it will fail when the temporary credentials are used, so this design is accordance with that finding. Another external 
team agreed that behavior is fine for actions, but does not work for Conditions, because one can have a Condition to 
restrict calls by sourceIp, and if we silently ignore this, the client may incorrectly think the temporary credentials 
are restricted for use by that IP address, so the consensus was to reject the request for that scenario.

## 3.4 SessionToken Format

As mentioned above, one of the return values from the AssumeRole call will be the sessionToken. To support not
storing temporary credentials server-side in Ozone, the sessionToken will comprise various components needed to validate
subsequent S3 calls that use the token.  The sessionToken will have the following information encoded:

- originalAccessKeyId - this is the Kerberos identity of the user that created the sessionToken via the AssumeRole call.
When the temporary credentials are used to make S3 API calls, this Kerberos identity (in conjunction with the role permissions and
optional session policy) will be used to authorize the call.  This identity is included in the sessionToken because 
S3 API calls (such as PutObject) require a Kerberos identity, but the temporary credentials don't have a
Kerberos identity associated to them, therefore the Kerberos identity of the user that created the token will be used in
these cases.
- roleArn - the role used in the original AssumeRole call
- encrypted secretAccessKey - this will be used to validate the AWS signature when the temporary credentials are used 
to make S3 API calls
- sessionPolicy - when using the RangerOzoneAuthorizer, if Ranger successfully authorizes the AssumeRole call,
it will return a String representing the role the token was authorized for.  Furthermore, if an AWS IAM Session Policy 
was included with the AssumeRole request, the String return value will also include resources (i.e. buckets, keys, etc.) 
and permissions (i.e. ACLType) corresponding to the AWS IAM Session Policy.  These resources and permissions, if present, 
would further limit the scope of the permissions and resources granted by the role in Ranger, such that the temporary 
credential will have the permissions comprising the intersection of the role permissions and the sessionPolicy permissions.
- HMAC-SHA256 signature - used to ensure the sessionToken was created by Ozone and was not altered since it was created.
- expiration time of the token (via `ShortLivedTokenIdentifier#getExpiry()`)
- UUID of the OzoneManager secret key used to sign the sessionToken and encrypt the secretAccessKey (via `ShortLivedTokenIdentifier#getSecretKeyId()`)

## 3.5 STS Token Revocation

In the rare event temporary credentials need to be revoked (ex. for security reasons), a table in the OzoneManager RocksDB will be created
to store revoked tokens, and a command-line utility will be created to add tokens to the table.  A background cleaner service
will be created to run every 3 hours to delete revoked tokens that have been in the table for more than 12 hours.  The
input parameter for the command-line utility will be the sessionToken - this value is returned in plain text as a result 
of the AssumeRole call (mentioned above).  In this way, specific STS tokens can be revoked as opposed to all tokens.  Furthermore, 
AWS doesn't have a standard API to revoke tokens therefore we are creating our own system.
Note: STS token revocation checks are strictly enforced and will fail-closed if there are internal errors such as not
being able to communicate with the revocation database table, etc.
Note: The creator of the STS token or an S3/tenant admin are the only ones allowed to revoke a token.

## 3.6 Prerequisites

A user must be configured with a Kerberos identity in Ozone and the S3 `getSecret` command
must be called to issue permanent S3 credentials.  With these credentials, the AssumeRole API call can be made, but additional 
steps below are needed for the call to be successfully authorized.  
When using RangerOzoneAuthorizer, a role must be configured in Ranger UI for each role the AssumeRole API
can be used with.  Further, Apache Ranger policies should be in place to grant the user permission to assume the role.

### 3.6.1 Additions to RangerOzoneAuthorizer

The `IAccessAuthorizer` interface that both the RangerOzoneAuthorizer and OzoneNativeAuthorizer implement, will have a
new method: 

```java
default String generateAssumeRoleSessionPolicy(AssumeRoleRequest assumeRoleRequest) throws OMException {
    throw new OMException("The generateAssumeRoleSessionPolicy call is not supported", NOT_SUPPORTED_OPERATION);
}
```

When using RangerOzoneAuthorizer, the AssumeRole API call must invoke this method to ensure the caller is authorized to create
temporary credentials, given the criteria in the AssumeRoleRequest.  The AssumeRoleRequest input parameter will have the
components:
- `String` host - hostname of caller
- `InetAddress` ip - IP address of caller
- `UserGroupInformation` ugi - the user making the call
- `String` targetRoleName - what role is being assumed
- `Set<AssumeRoleRequest.OzoneGrant>` grants - further limiting the scope of the role according to the grants

The grants parameter is optional, and would only be present if the AssumeRole API call had an IAM session policy JSON 
parameter supplied.  A conversion utility, `IamSessionPolicyResolver` will process the IAM policy and convert it to a 
`Set<AssumeRoleRequest.OzoneGrant>`, in effect translating from S3 nomenclature for resources and actions to Ozone nomenclature of 
`IOzoneObj` and `ACLType`.  Ranger would use all of this information to determine if the AssumeRole call should be 
successfully authorized, and if so, it will return a String representation of the granted permissions and paths.  

The format of this String is entirely up to the Ranger team.  What is required from the Ozone side is to supply this String to Ranger when any
subsequent S3 API calls are made that use STS tokens.  In order to achieve this, the sessionPolicy String from Ranger will 
be included in the sessionToken response to the AssumeRole API call (as mentioned above), and Ozone will supply this String
to Ranger whenever STS tokens are used on S3 API calls via a new `RequestContext.sessionPolicy` field in the 
`IAccessAuthorizer#checkAccess(IOzoneObj, RequestContext)` call.

## 3.7 Overall Flow

The following section outlines the overall flow when using STS in Ozone:

- An authorized user for AssumeRole API calls must be configured in Ozone, and if using RangerOzoneAuthorizer, the role
created in Ranger as per the Prerequisites above.
- This authorized user (having permanent S3 credentials) makes the AssumeRole STS call to Ozone.
- If successful, Ozone responds with the temporary credentials.
- A client makes S3 API calls with the temporary credentials for up to as long as the credentials last.  
- When Ozone receives an S3 api call using temporary credentials, it will use the Kerberos identity associated with the 
originalAccessKeyId in the session token and perform the following checks:
  - Ensure that if the accessKeyId starts with "ASIA", that a sessionToken was included in the `x-amz-security-token` header
  - Ensure the sessionToken is not expired
  - Ensure the sessionToken is not revoked via a `keyMayExist` check in OzoneManager RocksDB
  - Validate the HMAC-SHA256 signature in the sessionToken
  - Decrypt the secretAccessKey from the sessionToken and validate the AWS signature
  - Authorize the call with either RangerOzoneAuthorizer or OzoneNativeAuthorizer

Assuming all these checks pass, the S3 API call will be invoked.

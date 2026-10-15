---
title: S3 Object Lock
summary: Design to support S3 object lock.
date: 2026-08-18
jira: HDDS-15945
status: draft
author: Chung En Lee
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

# S3 Object Lock Design Doc

## Summary

This design document aims to plan and implement the Object Lock mechanism for OBS buckets, with authorization through Ozone native ACLs or Apache Ranger.
The primary objective is to provide data immutability and tamper-proof protection through the object locking feature.

## Problem statement

With growing demands for data security and compliance, ensuring that critical data stored in Ozone is
protected from accidental or malicious deletion and overwriting has become an essential system protection requirement.
To establish a more rigorous data protection mechanism, we plan to introduce the Object Lock feature.

Considering the current system architecture and access control strategies,
this design supports both Ozone native ACLs and Apache Ranger to manage Object Lock permissions on OBS buckets.
Native ACLs are used by deployments without Ranger and allow the core logic to be implemented and tested independently of the Ranger integration.
Meanwhile, to accelerate core feature delivery, we have decided to exclude complex multi-version locking (Versioning Lock),
Legacy buckets, and FSO buckets from this initial release. Narrowing the scope allows us to focus on the core functionality and ensure a rapid,
stable rollout of baseline tamper-proof protection.

**Goal:**

* Implement the Object Lock feature on standard OBS buckets, with permission and access control through native ACLs or Ranger.
* Support single-version objects only.

## Non-Goal

* Versioning Lock: Support for locking across multiple object versions is deferred (multi-version core features are currently under development).
* Legacy and FSO Buckets: Object Lock support for legacy and FSO buckets is excluded. The following considerations are deferred to a future phase:
  * Lock Granularity: FSO separates files and directories across FileTable and DirectoryTable, requiring explicit policy decisions on whether lock semantics apply at the file level, directory level, or both. 
  * Rename & Move Semantics: Renaming or moving paths alters namespace hierarchy without mutating object content; rules must be defined to either block operations on locked paths or preserve lock metadata across the same objectID
* Value-Level Ranger Authorization: a payload-aware Ranger condition evaluator (for example, allowing a legal hold to be set `ON` but not `OFF`) is deferred to a future phase.

## Differences from AWS

* Single-Version Model: Locks apply to the current key rather than individual object versions. 
* Direct Overwrite Blocking: Unlike AWS S3 (which writes a new version), Ozone strictly rejects Put, Copy, and MPU requests on locked keys with 403 Access Denied (WORMProtectionException). 
* Authorization: Ozone has no root account.
  * With native ACLs, changing Object Lock settings requires `WRITE_ACL` and bypassing Governance retention is limited to Ozone administrators (see [Native ACL Mapping](#1-native-acl-mapping)).
  * With Ranger, AWS-style action-level authorization for S3 requests requires both `ozone.s3g.sts.http.enabled` and `enableActionMatcherInPoliciesCondition` to be enabled; both are `false` by default (see [Prerequisite for S3 Fine-Grained Authorization](#3-prerequisite-for-s3-fine-grained-authorization)).
  * Otherwise, and for native clients, Ranger falls back to `READ_ACL` / `WRITE_ACL`, and bypassing Governance retention requires `WRITE_ACL`, `WRITE` and `DELETE` (see [Fallback: Base Permission Mode](#4-fallback-base-permission-mode)).
  * A Compliance retention cannot be removed through any API; it can only be changed offline with `ozone repair` (see [Offline Compliance Retention Repair](#offline-compliance-retention-repair)).

## Technical Description

### Terminology

**Legal Hold**

* **Definition**: Applies an indefinite lock status to an object. The object remains protected until a principal allowed to change the legal hold (`WRITE_ACL` on the key, plus the `PutObjectLegalHold` action in Ranger Fine-Grained Mode) explicitly removes the lock (Remove Legal Hold).
* **Restricted Operations**:
  * Put Object 
  * Delete Object 
  * Multipart Initial / Complete
* **Allowed Operations**:
  * Get Object / Head Object
  * Get / Put Object Tagging
  * Get Legal Hold
  * Put Legal Hold (Depends on permission)
* **Usage**:
  * Used to preserve data for litigation, investigations or audits, when it is not known in advance how long the data must be kept.
  * A legal hold has no mode and no expiration date. It is independent of retention: a key can have a legal hold, a retention period, or both,
    and it stays locked until the legal hold is removed and the retention period has expired.
  * A legal hold can be placed on a key in an Object Lock enabled bucket even if the key has no retention.

**Retention**

* Definition: Configures a retention policy for an object to prevent deletion or modification. Retention can be duration-based (configured in days or years, establishing a fixed `RetainUntilDate`) or event-driven (**Event Hold / Event-based Retention**, where an object remains protected indefinitely until an external business or legal event triggers the final retention countdown).
* Retention Modes:
  * Compliance Mode: The strictest protection tier. Once applied, no user (including root/admin) can remove the lock, shorten the duration, or overwrite the object before the retention period expires.
  * Governance Mode: A flexible protection tier. Standard users are restricted by locking rules, but users with `BypassGovernanceRetention` permissions can bypass restrictions to modify or delete the object.
* Event Hold / Event-based Retention:
  * Used for records management where the retention lifecycle depends on external events (e.g., contract termination, employee departure, loan closure).
  * Keeps the object immutable while awaiting event notification; once the event occurs, the definitive expiration date (`RetainUntilDate`) is calculated and applied.
  * While the event has not occurred, `eventHold.enabled = true` keeps the key locked indefinitely. `eventHold.rule` is copied from the bucket default when the key is created, so later changes to the bucket default do not affect existing keys.
  * The bucket default can have both `rule` and `eventHold`. In that case a new key gets `retentionDate` = creation time + `rule` and an active event hold;
    the key stays locked until the event hold is released and `retentionDate` has passed. With only `eventHold`, `retentionDate` is unset until the hold is released.
  * While an event hold is active, a `GOVERNANCE` key can be deleted or overwritten with `bypassGovernanceRetention`; a `COMPLIANCE` key cannot.
* Restricted Operations:
  * Put Object / Copy Object (Overwrites)
  * Delete Object
  * Multipart Upload (Initial / Complete)
  * Shorten Retention Period (in Compliance Mode)
* Allowed Operations:
  * Get Object / Head Object
  * Get / Put Object Tagging
  * Extend Retention Period

#### Event Hold Trigger Workflow
For enterprise governance workflows, the retention lifecycle often depends on specific business triggers rather than a static configuration at creation. The Event Hold mechanism handles this dynamic lifecycle through the following state transitions:

```text
object created
      |
event hold applied (indefinite WORM protection)
      |
business event occurs
      |
hold released
      |
retention timer starts (RetainUntilDate is calculated)
      |
WORM until timer expires
```

The "business event occurs" step is triggered with `PutObjectRetention`, so no new API or Ranger action is needed
and S3 clients can release the hold through S3 Gateway (native clients use `OzoneBucket#setRetention`).
When OM handles `PutObjectRetention` on a key with an active event hold, it clears `eventHold` under the key write lock
and sets `retentionDate` to the latest of the existing `retentionDate`, the requested `RetainUntilDate`, and the leader-stamped request time plus the event hold `rule`.
The retention mode cannot be changed by this request, so the usual rules for shortening retention still apply.

> _**Note**:
> * Background & Root Cause: A prerequisite for enabling WORM (Write Once, Read Many) in AWS S3 is that Object Versioning must be enabled. Under S3 architecture, executing a Put on a locked object generates a new version without affecting the protected prior version; thus, S3 Object Lock primarily restricts Delete Object. 
> * Ozone Implementation Status: Because Ozone's versioning feature is still under development, to guarantee absolute immutability during the lock period, Ozone will directly block and reject all overwrite operations (such as any form of Put or overwrite) on locked objects.

### Table Changes

**Bucket Table**

Two new fields: objectLockEnabled & defaultRetention.

```protobuf
  message BucketInfo {
    // ... existing fields
    optional bool objectLockEnabled = 24 [default = false];
    optional BucketRetention defaultRetention = 25;
  }

  message BucketRetention {
    required RetentionMode retentionMode = 1;
    optional Rule rule = 2;
    optional EventHold eventHold = 3;
  }
  
  message EventHold {
    required bool enabled = 1;
    required Rule rule = 2;
  }
  
  message Rule {
    required TimeUnit timeUnit = 1;
    required uint64 duration = 2;
  }
  
  enum TimeUnit {
    DAYS = 1;
    YEARS = 2;
  }
  
  enum RetentionMode {
    GOVERNANCE = 1;
    COMPLIANCE = 2;
  }
```



**Key Table**

Two new fields: retention & legalHold.

```protobuf
message KeyInfo {
  // ... existing fields
  optional KeyRetention retention = 23;
  optional bool legalHold = 24 [default = false];
}

message KeyRetention {
  required RetentionMode retentionMode = 1;
  optional uint64 retentionDate = 2; // epoch milliseconds
  optional EventHold eventHold = 3;
}
```



### Access Control

Ozone Object Lock enforces a dual-gate protection mechanism combining **authorization (native ACLs or Ranger)** and **underlying OM WORM state validation**:

* **Standard Data Operations (Put / Delete)**: Even if a principal has valid `CREATE`, `WRITE` or `DELETE` access, any attempt to mutate, overwrite, or delete an object under an active Legal Hold or an unexpired Retention period is immediately rejected with `403 Access Denied` (`WORMProtectionException`).
* **Base Permissions**: Object Lock operations are checked against the existing `READ_ACL` / `WRITE_ACL` types for both native ACLs and Ranger, so no new ACL type is needed (see below).
* **Fine-Grained S3 Action Matching (Ranger)**: For requests through S3 Gateway, Object Lock management integrates with the action-matching authorization framework introduced via STS. S3 Object Lock actions map 1:1 to AWS S3 action names. See [AWS STS Design for Ozone S3](ozone-sts.md) for details.

---

#### 1. Native ACL Mapping

Changing Object Lock state is treated as a permission change, so it requires `WRITE_ACL` rather than `WRITE`.
A user who can only write or delete data cannot remove or shorten a lock.

| Operation                              | Resource Level | Required Native ACL                |
|:---------------------------------------|:---------------|:-----------------------------------|
| `GetBucketObjectLockConfiguration`     | Bucket         | `READ_ACL`                         |
| `PutBucketObjectLockConfiguration`     | Bucket         | `WRITE_ACL`                        |
| `GetObjectRetention`                   | Key            | `READ_ACL`                         |
| `PutObjectRetention`                   | Key            | `WRITE_ACL`                        |
| `GetObjectLegalHold`                   | Key            | `READ_ACL`                         |
| `PutObjectLegalHold`                   | Key            | `WRITE_ACL`                        |
| Bypass Governance retention            | Key            | Ozone administrator only           |

**Ozone administrator** means a user listed in `ozone.administrators` or a member of a group listed in `ozone.administrators.groups`
(if neither is set, only the user that starts the OM is an administrator). This is the same check used for other admin-only OM operations.
* The native authorizer already grants every ACL to Ozone administrators, so they also pass the `READ_ACL` / `WRITE_ACL` checks above.
* Bypass is limited to administrators because the native ACL model has no permission dedicated to it: any ACL type would also be grantable by bucket or key owners,
  who could then remove the Governance protection from their own data.
* Read-only administrators (`ozone.readonly.administrators`) and S3 administrators (`ozone.s3.administrators`) cannot bypass Governance retention.
* The administrator role is cluster wide, so it cannot be scoped to a single volume or bucket. Deployments that need per-bucket bypass entitlement should use Ranger.
* The bypass does not apply to Compliance retention or a legal hold; a legal hold must be removed explicitly with `PutObjectLegalHold`, which an administrator can do because it only requires `WRITE_ACL`.

---

#### 2. Ranger Authorization Modes

With Ranger, every Object Lock request is authorized in one of two modes.
S3 actions are an S3 protocol concept, so only S3 Gateway sets them; OM does not add S3 actions to requests itself.
In both modes OM always checks the base access type (`READ_ACL` / `WRITE_ACL`), and the OM WORM state validation is the same.

| Request source                  | `ozone.s3g.sts.http.enabled` | `enableActionMatcherInPoliciesCondition` | Mode                                                                    |
|:--------------------------------|:-----------------------------|:-----------------------------------------|:------------------------------------------------------------------------|
| S3 Gateway                      | `true`                       | `true`                                   | [Fine-Grained Mode](#3-prerequisite-for-s3-fine-grained-authorization)  |
| S3 Gateway                      | `false`                      | `true`                                   | [Base Permission Mode](#4-fallback-base-permission-mode)                |
| S3 Gateway                      | `false`                      | `false`                                  | [Base Permission Mode](#4-fallback-base-permission-mode)                |
| S3 Gateway                      | `true`                       | `false`                                  | [Base Permission Mode](#4-fallback-base-permission-mode) (not recommended, see below) |
| Native Java API / CLI           | any                          | any                                      | [Base Permission Mode](#4-fallback-base-permission-mode)                |

* **Fine-Grained Mode**: Ranger checks the S3 action (for example `PutObjectLegalHold`) and its base access type in the [mapping below](#6-action-to-ranger-access-type-mapping).
* **Base Permission Mode**: Ranger checks only the base access type.

---

#### 3. Prerequisite for S3 Fine-Grained Authorization

> _**Important for S3 users**_: AWS-style action-level authorization for Object Lock (separate permissions for `PutObjectRetention`,
> `PutObjectLegalHold`, `PutBucketObjectLockConfiguration` and `BypassGovernanceRetention`) is only available when **both** flags below are enabled.
> Both are `false` by default, so a cluster with default settings authorizes S3 Object Lock requests in [Base Permission Mode](#4-fallback-base-permission-mode).

```properties
# S3 Gateway and OM: extract the S3 action and forward it to the authorizer
ozone.s3g.sts.http.enabled=true
# Ranger Ozone service definition: evaluate the S3 action in policy conditions
ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition=true
```

**Why both flags are required**:
* `ozone.s3g.sts.http.enabled` decides whether the S3 action reaches Ranger at all. Today S3 Gateway sets `S3Authentication.s3Action` and OM copies it into the `RequestContext` only when this flag is `true`
  (`EndpointBase.applyS3Action`, `OmMetadataReader.maybeAddToContextFromThreadLocal`). The action was originally only needed for STS, which is why it is tied to this flag.
* `enableActionMatcherInPoliciesCondition` decides whether Ranger uses the action. When it is `false`, Ranger's `RangerActionMatcher` ignores the action and only checks the access type.
* If only the Ranger flag is enabled, S3 Gateway sends no action, so every request falls back to Base Permission Mode.
* If only the STS flag is enabled, Ranger ignores the action, so requests also fall back to Base Permission Mode.
  This combination is not recommended: STS session policies restrict temporary credentials by S3 action (see [AWS STS Design for Ozone S3](ozone-sts.md)),
  so without the Ranger flag those restrictions would not be enforced. This is also why the STS flag is not enabled by default.

**What S3 users get in Fine-Grained Mode**:
* Object Lock permissions can be granted separately, as in AWS IAM. For example, a legal team can be granted `PutObjectLegalHold` without `PutObjectRetention`,
  and `BypassGovernanceRetention` can be granted to a smaller group than the one that sets retention.
* STS session policies (AssumeRole) can include or exclude Object Lock actions, and `IamSessionPolicyResolver` restricts the temporary credential accordingly.
* Each action still requires its base access type, so a wrong action mapping in S3 Gateway cannot reduce the check below `READ_ACL` / `WRITE_ACL`.

**Operator guidance**:
* Clusters that expose Object Lock to S3 users with different roles (for example, data writers, compliance officers and administrators) should enable both flags before enabling Object Lock on any bucket.
* The flags only affect S3 Gateway requests. Native clients are always authorized in Base Permission Mode, even when both flags are enabled, and are not limited by action conditions (see [below](#4-fallback-base-permission-mode)).
* The user documentation of this feature will describe both flags and the difference between the two modes.

---

#### 4. Fallback: Base Permission Mode

Base Permission Mode is used for every request without an S3 action:
requests from the native Java API or CLI, and S3 Gateway requests when either flag in the [previous section](#3-prerequisite-for-s3-fine-grained-authorization) is disabled.
OM does not set an S3 action for these requests, so that the S3 action mapping stays in `S3GActionIamMapper` only.

**Checks**:

| Operation                              | Resource Level | Required Ranger Access Type |
|:---------------------------------------|:---------------|:----------------------------|
| `GetBucketObjectLockConfiguration`     | Bucket         | `READ_ACL`                  |
| `PutBucketObjectLockConfiguration`     | Bucket         | `WRITE_ACL`                 |
| `GetObjectRetention`                   | Key            | `READ_ACL`                  |
| `PutObjectRetention`                   | Key            | `WRITE_ACL`                 |
| `GetObjectLegalHold`                   | Key            | `READ_ACL`                  |
| `PutObjectLegalHold`                   | Key            | `WRITE_ACL`                 |
| Bypass Governance retention            | Key            | `WRITE_ACL`, `WRITE` and `DELETE` |

**Why `READ_ACL` / `WRITE_ACL` instead of `READ` / `WRITE`**:
* Without an S3 action, a `WRITE` check could not tell `PutObject` from `PutObjectLegalHold`, so any user who can write a key could also remove its legal hold or shorten its retention.
* Changing a lock is treated as a permission change, so it uses the same access type as changing an ACL. Users with only `READ`, `WRITE`, `CREATE` or `DELETE` on a key cannot read or change its Object Lock settings.
* This matches the [Native ACL Mapping](#1-native-acl-mapping), so the same operation needs the same access type with native ACLs and with Ranger.

**Limitations compared with Fine-Grained Mode**:
* All Object Lock changes on a resource need the same `WRITE_ACL`, so they cannot be granted separately. A principal that can set a legal hold can also set retention on the same key, and can bypass Governance retention if it also has `WRITE` and `DELETE`.
* Governance retention therefore only protects keys from principals without `WRITE_ACL`, `WRITE` and `DELETE`. Deployments that need a separate bypass entitlement should use Fine-Grained Mode.
* `WRITE_ACL` is also the access type for changing ACLs, so granting Object Lock management also grants that.
* Unlike native ACLs, where bypass is limited to Ozone administrators, Ranger has no Ozone administrator role, so bypass is granted through a policy with `WRITE_ACL`, `WRITE` and `DELETE`.
* Compliance retention is not affected: it cannot be removed in either mode.

**Native clients when both flags are enabled**:
* Native clients carry no S3 action, so they are checked only against the base access type, even on a cluster that uses Fine-Grained Mode for S3.
* When a request carries no action, Ranger skips the action condition of a policy item and only checks the access type.
  A principal whose `WRITE_ACL` is limited to `PutObjectLegalHold` through S3 Gateway can therefore also set retention on the same key through a native client, and bypass Governance retention if it also has `WRITE` and `DELETE`.
* Action-level separation is thus only enforced for S3 Gateway requests. Deployments that rely on it should not give those principals native client access to the locked buckets.

---

#### 5. Ranger & S3 Gateway Component Updates

1. **UI & Metadata Registration**:
  * Update `security-admin/src/main/webapp/react-webapp/src/utils/actionRequirements/ozone.json` in Apache Ranger:
    Register the 7 new Object Lock actions along with their prerequisite primitive permissions (`READ_ACL`, `WRITE_ACL`) mapped by resource level (Bucket vs. Key). This enforces design-time validation in the Ranger React UI, preventing administrators from creating invalid policy items.

2. **S3 Gateway & STS Mapping**:
  * Update **`S3GActionIamMapper`**: Map incoming HTTP request context and sub-resources (e.g., `?legal-hold`, `?retention`, `?object-lock`) to the corresponding action string.
  * Update **`IamSessionPolicyResolver`**: Ensure session-scoped policies (STS AssumeRole / federation tokens) recognize and evaluate the new Object Lock action strings, mapped to the same `READ_ACL` / `WRITE_ACL` base access types.

---

#### 6. Action to Ranger Access Type Mapping

In Fine-Grained Mode, following the declarative contract in `ozone.json`, each S3 action requires a baseline primitive Ranger permission at the designated resource hierarchy:

| S3 Action                              | Resource Level | Required Ranger Access Type | Description                                                         |
|:---------------------------------------|:---------------|:----------------------------|:--------------------------------------------------------------------|
| **`GetBucketObjectLockConfiguration`** | Bucket         | `READ_ACL`                  | Retrieve default Object Lock settings on a bucket.                  |
| **`PutBucketObjectLockConfiguration`** | Bucket         | `WRITE_ACL`                 | Configure default retention mode and period on a bucket.            |
| **`GetObjectRetention`**               | Key            | `READ_ACL`                  | Read the retention mode and Retain-Until date of an object version. |
| **`PutObjectRetention`**               | Key            | `WRITE_ACL`                 | Set or extend object retention mode and duration.                   |
| **`GetObjectLegalHold`**               | Key            | `READ_ACL`                  | Query the current Legal Hold status (`ON` or `OFF`).                |
| **`PutObjectLegalHold`**               | Key            | `WRITE_ACL`                 | Toggle the Legal Hold state (`ON` or `OFF`).                        |
| **`BypassGovernanceRetention`**        | Key            | `WRITE_ACL`, `WRITE` and `DELETE` | Privileged entitlement to bypass retention in Governance Mode. |

---

#### 7. Retention Modes & Governance Bypass Enforcement

* **Compliance Mode**:
  * The strictest WORM protection tier.
  * Once applied, the lock is immutable through the Ozone API. **No role, administrator, native ACL or Ranger permission** (including `BypassGovernanceRetention`) can overwrite, shorten, or delete the object version until the retention period expires. The only way to change it early is the offline `ozone repair` command described below.
* **Governance Mode & `BypassGovernanceRetention`**:
  * Protects objects from accidental deletion by general users while retaining operational flexibility for authorized personnel.
  * **Dual-Check Enforcement**:
    1. **Client Declaration**: The client must explicitly set the `bypassGovernanceRetention` flag on the request. S3 Gateway sets it from the `x-amz-bypass-governance-retention: true` header; native Ozone clients pass it as a boolean argument.
       It is carried in `KeyArgs` and `DeleteKeyArgs`, so it is available to every operation that checks Object Lock (delete, batch delete, overwrite, multipart upload, `PutObjectRetention`).
       Because Ozone does not support versioning yet, this flag is an Ozone-specific design: when Object Lock is enabled without versioning, a Governance-locked key can only be deleted or overwritten with the flag.
    2. **Bypass Entitlement**: With native ACLs, the principal must be an Ozone administrator (`ozone.administrators`).
       With Ranger, the principal needs `WRITE_ACL`, `WRITE` and `DELETE` on the target key, and in Fine-Grained Mode also the **`BypassGovernanceRetention`** action.
    3. If either check fails, the request is aborted with `403 Access Denied`.


> _**Warning**: Permanent Data Immutability in Compliance Mode_
>
> Unlike AWS S3, where objects in Compliance mode cannot be deleted by any user (including root) and will only be destroyed if the entire AWS account itself is removed, Apache Ozone has no concept of a root or tenant account.
> Once applied, a Compliance lock cannot be overridden or deleted through the Ozone API by any admin, native ACL or Ranger policy until the retention period expires.
> It can only be changed offline with `ozone repair` (see below).

### New Ozone APIs

Surfacing objectLockEnabled, default retention settings, and active WORM protection via ozone shell will be addressed in a follow-up phase.

**OzoneBucket**

```java
public void setRetentionConfig(RetentionArgs retentionArgs);
public void setRetention(String keyName, ObjectRetention retention, boolean bypassGovernanceRetention);
public void setLegalHold(String keyName, boolean hold);
```

### Supported S3 APIs

To ensure seamless integration with existing S3 clients (e.g., AWS CLI, Boto3) and applications, the Ozone S3 Gateway (S3G) will translate and support standard AWS S3 APIs.
In alignment with AWS S3, HeadObject and GetObject responses will be enriched with lock-related headers (`x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`, and `x-amz-object-lock-legal-hold`) when the caller possesses the required GetObjectRetention or GetObjectLegalHold permissions. Without the permission the headers are omitted and the request still succeeds, so the extra authorization never fails a plain read.

**Bucket-Level APIs:**
* `CreateBucket`: Supports enabling Object Lock at creation time via the x-amz-bucket-object-lock-enabled: true header.
* `GetBucketObjectLockConfiguration`: Retrieves the Object Lock status and the default retention configuration (if any) for a specified OBS bucket.
* `PutBucketObjectLockConfiguration`: Enables Object Lock and default retention on an existing bucket. In alignment with AWS S3, passing the `x-amz-bucket-object-lock-token` header is supported when enabling lock on existing buckets.
* Bucket links: S3 buckets are often links (for example `/s3v/bucket1` → `/vol1/bucket1`). Unlike `OMBucketSetPropertyRequest`, which rejects property changes on links,
  `PutBucketObjectLockConfiguration` follows the link and stores the configuration on the source bucket, and `GetBucketObjectLockConfiguration` reads it from there.
  Authorization is checked on both the link and the source bucket, like other requests that resolve links. Key operations already resolve links, so WORM checks always use the source bucket's configuration.

**Object-Level APIs:**
* `PutObject / CreateMultipartUpload`: Supports setting retention and legal hold directly during object creation via `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`, and `x-amz-object-lock-legal-hold` request headers.
* `PutObjectRetention`: Places a retention configuration on an object, specifying the retention mode (COMPLIANCE or GOVERNANCE) and the retention duration (in days or years).
* `GetObjectRetention`: Retrieves the current retention configuration applied to an object.
* `PutObjectLegalHold`: Applies or removes a Legal Hold configuration to the specified object.
* `GetObjectLegalHold`: Retrieves the current Legal Hold status of an object.

**Impacted Standard APIs:**
Standard data mutating APIs will intercept the Object Lock state and return the `403 Access Denied` (or specifically `WORMProtectionException`) if a modification is attempted on a locked object:
* `PutObject` / `CopyObject` (Overwrites will be rejected)
* `DeleteObject` / `DeleteObjects` (Deletions will be rejected)
* `CreateMultipartUpload` / `CompleteMultipartUpload` / `UploadPart` (Uploads will be rejected if the object is locked)
* Rename (`RenameKey` / `RenameKeys`): S3 Gateway returns `NotImplemented`, but OBS buckets support rename through native clients (for example `ozone sh key rename`), and the lifecycle service uses rename when moving keys to trash.
  Renaming a key removes it from its original name, so it is treated as a delete of the source key: rejected under a legal hold or Compliance retention,
  and allowed under Governance retention only with `bypassGovernanceRetention` and the bypass entitlement.
* `CopyObject` does not copy the Object Lock configuration of the source object (same as AWS S3 by default); the destination gets the bucket default retention or the `x-amz-object-lock-*` headers of the request.
* The rejection uses a new OM result code for `WORMProtectionException`, which S3 Gateway maps to `403 AccessDenied`; `DeleteObjects` reports it as a per-key `AccessDenied` error.
* Lifecycle expiration: when the lifecycle service selects expired keys, it skips keys that are locked. It already reads the `OmKeyInfo` of each key during the scan, so no extra lookup is needed,
  and locked keys are not sent again on every run. A skipped key is evaluated again in later scans and expires normally once it is unlocked.
  The WORM check in `DeleteKeys` / `RenameKey` remains the authoritative guard in case a key is locked after it was selected.

> _**Note**:
> * **Object Lock Immutability & Future Versioning Binding**:
>   - **Current State (No Versioning)**: Once Object Lock is enabled on a bucket (`objectLockEnabled = true`), it cannot be disabled.
>   - **AWS S3 Alignment & Future Evolution**: In standard AWS S3, Object Lock inherently requires and automatically enables Versioning upon bucket creation. While Ozone temporarily decouples them because Ozone does not support Versioning yet, future implementations will align with S3 semantics: enabling Object Lock will automatically enforce Versioning, and Versioning can never be suspended or disabled thereafter.

### Impact on Write and Delete Flows

**Impact on Put Object Flow**

![object-lock-put-object.png](object-lock-put-object.png)

During the Create Key phase, the system executes a preliminary WORM check in `preExecute` to enforce a fail-fast mechanism:

1. Linearizability does not need to be guaranteed at this stage. 
2. It avoids Raft consensus overhead, conserving system resources.

During the Commit Key phase, the system performs final WORM validation in `validateAndUpdateCache`:

1. It guarantees linearizability, ensuring only one write request can succeed simultaneously on a WORM-protected object. 
2. Keys that fail to commit remain in an Open Key state, and their associated metadata and content will be reclaimed periodically by system background cleanup. 

**Impact on Multipart Upload Flow**

Multipart Upload adopts the same design logic as Put Object to ensure operational consistency:
1. Similar to the creation key flow, WORM checks during the Initiate Multipart Upload and Put Part phases are executed in `preExecute` for fail-fast behavior (linearizability is not yet required, minimizing consensus overhead).
2. Similar to the commit key flow, WORM check during the Complete Multipart Upload phase is executed in `validateAndUpdateCache` to guarantee linearizability for writes under locked states.

**Impact on Delete Object Flow**

All delete operations must perform WORM validation during the `validateAndUpdateCache` phase to preserve linearizability and prevent accidental deletion of protected data.

### Offline Compliance Retention Repair

Whether and how Ozone supports an AWS-like account closure is out of the scope of this feature.
The problem this feature must answer is whether anyone can remove a Compliance retention once it is set.
Following the existing `ozone repair` convention for offline fixes, an offline command is provided to update the retention of keys, one key or a batch at a time:

```text
ozone repair om object-lock --bucket /<volume>/<bucket> [--key <key> | --prefix <prefix>] [--dry-run]
```

* **Offline**: it runs against the OM DB while OM is stopped, like other `ozone repair om` tools, so it is not reachable through S3 Gateway or OM RPC.
* **Who can run it**: Ozone administrators with access to the OM host and DB. This is already outside the API-level trusted boundary (see [Trusted Boundary](#trusted-boundary-and-compliance-scope)).
* **What it does**: removes the retention of the selected keys or changes it from `COMPLIANCE` to `GOVERNANCE`, after which the normal Governance rules apply. `--dry-run` lists the affected keys without changing them.
* **Audit**: every changed key is logged so the change can be reviewed later.

## Performance

Introducing WORM validation into critical execution paths (such as Create Key and Commit Key) incurs minor overhead.
To minimize impact, this design employs a two-phase validation strategy: preliminary filtering in `preExecute` provides fail-fast pruning to prevent invalid requests from reaching the consensus layer, followed by atomic validation in `validateAndUpdateCache` for linearizability.
The additional metadata lookups are expected to have a small impact on throughput and latency; this will be verified with benchmarks and metrics during implementation.

## Security

Access control via native ACLs or Ranger ensures all Object Lock operations (such as Retention Policy configuration and Legal Hold management) are enforced under strict access permissions. With native ACLs, changing a lock requires `WRITE_ACL` and Governance bypass is limited to Ozone administrators.

With Ranger, changing a lock requires `WRITE_ACL`, and bypassing Governance retention requires `WRITE_ACL`, `WRITE` and `DELETE`. For S3 Gateway requests, by leveraging the **STS action-matching framework**, Ozone maps standard S3 API actions (e.g., `BypassGovernanceRetention`, `PutObjectLegalHold`) directly to standard Ranger primitive access types (`READ_ACL`, `WRITE_ACL`, plus `WRITE` and `DELETE` for `BypassGovernanceRetention`). This fine-grained, action-level authorization enforces the Principle of Least Privilege **without requiring custom dedicated access types**. It effectively prevents unauthorized tampering or removal of locks, ensuring robust compliance and data immutability.

### Trusted Boundary and Compliance Scope

While AWS S3 can guarantee absolute immutability across all levels because its infrastructure is entirely managed and abstracted from users, Apache Ozone is a self-managed, software-defined storage system. It is important to define the trusted boundary to understand the scope of data protection and compliance.

**Trusted Boundary:**
The trusted boundary for this Object Lock design is defined at the **Application API Level**.
* All data and metadata operations routed through the Ozone Java API, S3 Gateway, RPC interfaces, and standard Ozone clients are strictly governed by the Object Lock validation (`validateAndUpdateCache`) and native ACLs or Ranger access policies.
* Within this boundary, immutability is strictly enforced. Unless explicitly authorized with specific privileged permissions (e.g., s3:BypassGovernanceRetention), even cluster administrators using standard client tools cannot overwrite or delete locked objects until the retention period expires or the legal hold is removed.

**Out of Scope (Infrastructure Level bypass):**
Since Ozone runs on customer-managed infrastructure, operations executed with OS-level `root` privileges or physical access to the server nodes bypass the application-level API boundary. The following scenarios are not protected by Ozone Object Lock:
* **Direct Database Tampering:** Modifying or deleting key metadata directly from the underlying RocksDB instances while the node is offline.
* **Low-Level DR Tools:** Using `ozone repair` tools offline, including the Object Lock repair command above, or using them to explicitly restore the OM's DB directory from an older Ratis snapshot (taken before the lock was applied) or running `skip-ratis-transaction` to skip retention state entries.
* **Storage Level Deletion:** Directly deleting block files from the DataNode filesystems.
* **System Clock Tampering**: Artificially manipulating or rolling back the system clock (NTP/OS time) on OM or DataNode hosts to prematurely expire object retention periods or evade time-based WORM validation.
* **Ranger Super Admin Privilege Abuse**: Modifying, disabling, or overriding Ozone authorization policies via Apache Ranger admin credentials (or direct access to Ranger’s backend database), allowing privileged operators to alter bucket WORM flags or grant unrestricted deletion permissions outside normal enforcement workflows.

**Compliance Recommendation:**
To achieve strict regulatory compliance (e.g., SEC 17a-4 or FINRA WORM requirements) in an on-premise deployment, Ozone's Object Lock must be complemented with strict infrastructure security measures. This includes enforcing rigid OS-level RBAC/IAM, restricting SSH/root access to OM/DN nodes, and continuously forwarding Ozone audit logs to a tamper-evident, external SIEM system to detect manual offline interventions.

## Compatibility

* **Table Schema Changes**: Since all newly introduced fields in the Bucket Table (`objectLockEnabled`, `defaultRetention`) and Key Table (`retention`, `legalHold`) are defined as `optional` in Protobuf, they are wire-compatible.
  To avoid RocksDB serialization problems if the cluster is rolled back after new fields were written, a new OM layout feature gates Object Lock: until the upgrade is finalized, OM rejects every Object Lock request (bucket configuration, retention, legal hold, and `x-amz-object-lock-*` headers on Put / Multipart Upload), so no Object Lock metadata is persisted before finalization.
* **OM Version Leveling**: OM version leveling is introduced primarily because this feature adds new OM APIs (such as `setRetentionConfig` and `setLegalHold`). The new OM version allows clients to determine whether the target OM cluster supports executing these Object Lock APIs during rolling upgrades.

## Alternatives

### 1. Why Not Provide a Volume Force Delete Command?

* **Proposal**:
  Provide an admin command to force delete a volume that contains locked keys, as the closest analogue to closing an AWS account (which destroys all data, including Compliance-locked objects).

* **Why Rejected**:
  1. Whether and how Ozone supports an account closure is a separate problem and out of the scope of this feature.
  2. The actual question is whether a Compliance retention can be removed at all. An offline `ozone repair` command per key or batch answers it with a smaller scope and fits the existing repair tool convention.

## Plan

The implementation is divided into the phases below. Only the highlights are listed here; implementation details are left to the individual Jiras.

### Phase 1: Metadata & Upgrade

* Add the `BucketInfo` and `KeyInfo` fields, the `BucketRetention` / `KeyRetention` messages, and the `bypassGovernanceRetention` flag.
* Add the OM layout feature that rejects Object Lock requests until finalization.
* Add a helper that decides whether a key is locked from `legalHold`, `eventHold` and `retentionDate`, using the leader-stamped request time.

### Phase 2: Authorization

* Native ACLs: `READ_ACL` / `WRITE_ACL` checks for the Object Lock operations, and Ozone administrator check for Governance bypass.
* Ranger: `READ_ACL` / `WRITE_ACL` checks for the Object Lock operations, and `WRITE_ACL`, `WRITE` and `DELETE` for Governance bypass (Base Permission Mode).
* Fine-Grained Mode: register the 7 Object Lock actions in S3 Gateway and Ranger.
* Document both flags and the two modes in the user documentation.

### Phase 3: Object Lock Management APIs

* Bucket: `CreateBucket` with Object Lock, `PutBucketObjectLockConfiguration`, `GetBucketObjectLockConfiguration` (following bucket links).
* Key: `PutObjectRetention` / `GetObjectRetention` (including releasing an event hold), `PutObjectLegalHold` / `GetObjectLegalHold`.
* `OzoneBucket` client APIs.

### Phase 4: WORM Enforcement

* Apply the bucket default retention or `x-amz-object-lock-*` headers when a key is created.
* Reject overwrite (key commit, multipart upload complete), delete (single and batch) and rename of locked keys in `validateAndUpdateCache`, with fail-fast checks in `preExecute`.
* Lifecycle service skips locked keys when selecting expired keys.

### Phase 5: Offline Compliance Retention Repair

* Add `ozone repair om object-lock` to remove or downgrade Compliance retention per key or prefix while OM is offline, with `--dry-run` and logging of every change.

### Phase 6: Testing

* Unit tests for the lock check, each OM request and the repair command.
* Integration tests for the S3 Gateway to OM flow, native ACL authorization, bucket links, lifecycle and layout feature gating.
* Ranger acceptance tests in the `ozonesecure-ha` environment, including with STS disabled.

### Future Work

* **S3 Versioning**: when versioning is enabled, existing keys become the latest version and keep their lock state; overwrites create new versions instead of being rejected, and locks apply per version.
* **FSO Buckets**: define lock granularity for files and directories and the rename / move semantics.
* **Value-Level Ranger Authorization**: a payload-aware Ranger condition evaluator on `s3:object-lock-legal-hold` and `s3:object-lock-mode`, so policies can allow, for example, setting a legal hold `ON` but not `OFF`, or only `GOVERNANCE` mode.

## References

- [AWS S3 Object Lock User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [AWS S3 PutBucketObjectLockConfiguration API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketObjectLockConfiguration.html)
- [AWS S3 PutObjectRetention API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html)
- [AWS S3 PutObjectLegalHold API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html)
- [HDDS-15945: S3 Object Lock Support in Apache Ozone](https://issues.apache.org/jira/browse/HDDS-15945)
- [Ozone STS Design](ozone-sts.md)

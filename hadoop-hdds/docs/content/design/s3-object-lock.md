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

This design adds S3 Object Lock to OBS buckets, so keys can be protected from deletion and overwrite.
Authorization uses Ozone native ACLs or Apache Ranger.

## Problem statement

Data stored for security or compliance must be protected from accidental or malicious deletion and overwrite. Object Lock provides this protection.

The design supports both native ACLs and Ranger. Native ACLs serve deployments without Ranger, and let the core logic be built and tested without the Ranger integration.
To ship the core feature sooner, the first release excludes versioning, Legacy buckets and FSO buckets.

**Goal:**

* Object Lock on OBS buckets, with authorization through native ACLs or Ranger.
* Single-version keys only.

## Non-Goal

* Versioning: locks across multiple object versions (versioning is still under development).
* Legacy and FSO buckets. FSO needs its own decisions on:
  * Lock granularity: files and directories are in different tables (FileTable / DirectoryTable), so locks could apply to files, directories or both.
  * Rename and move: they change the namespace without changing the data, so they must either be blocked on locked paths or keep the lock with the same objectID.
* Value-level Ranger authorization, for example allowing a legal hold to be set `ON` but not `OFF`.

## Differences from AWS

* Single version: locks apply to the current key, not to object versions.
* Overwrites are rejected: in AWS S3, Put, Copy or MPU on a locked object writes a new version. In Ozone it would replace the locked key,
  so Ozone rejects Put, Copy, MPU and rename (native clients only) on locked keys with `403 Access Denied` (`WORMProtectionException`).
* Governance bypass on overwrite and rename: a Governance-locked key can only be overwritten or renamed with the `bypassGovernanceRetention` flag and the bypass permission.
  AWS S3 does not need this because it writes a new version (see [Retention Modes and Governance Bypass](#3-retention-modes-and-governance-bypass)).
* Authorization: Ozone has no root account.
  * Native ACLs: changing locks and bypassing Governance need the volume or bucket owner; setting Compliance needs an Ozone administrator (see [Native ACLs](#1-native-acls)).
  * Ranger: action-level permissions need `enableActionMatcherInPoliciesCondition`, which is `false` by default (see [Ranger](#2-ranger)).
    Object Lock uses the same access types as data (`READ` / `WRITE`), so without `action-matches`, anyone who can write a key can also change its locks.
  * No API can remove a Compliance retention. With Ranger, only the offline `ozone repair` command can (see [Offline Compliance Retention Repair](#offline-compliance-retention-repair)).

## Technical Description

### Terminology

**Legal Hold**

* Locks a key with no end date, until someone allowed to change the legal hold removes it
  (the owner with native ACLs; `WRITE` with Ranger, limited to `PutObjectLegalHold` when `action-matches` is used).
* Used for litigation, investigations or audits, when it is not known how long the data must be kept.
* Has no mode and no date, and is independent of retention: a key can have a legal hold, a retention, or both, and stays locked until both are gone.
  A legal hold can be set on a key without a retention.
* Blocks: Put / Copy (overwrite), Delete, Multipart Upload (initiate / complete), Rename (native clients).
* Allows: Get / Head, Get / Put Object Tagging, Get Legal Hold, Put Legal Hold (with permission).

**Retention**

* Protects a key until a `RetainUntilDate`. The date is either fixed (set directly, or from days / years) or comes from an event hold.
* Modes:
  * Compliance: no one, including administrators, can remove it, shorten it or overwrite the key through the Ozone API before it expires. With Ranger, only the offline `ozone repair` command can change it.
  * Governance: blocks normal users, but users with the bypass permission can bypass it.
* Event hold (AWS S3 *variable retention*):
  * Used when the retention depends on an external event, such as a contract ending, an employee leaving or a loan closing.
  * While the event has not happened (`EventHold=ON`), the key is locked with no end date. When it happens, the hold is released (`EventHold=OFF`)
    and the duration (`EventHoldDuration`, in days or years) becomes a fixed `RetainUntilDate`.
  * See [Event Hold Trigger Workflow](#event-hold-trigger-workflow) for the rules.
* Blocks (Governance allows them with `bypassGovernanceRetention`): Put / Copy (overwrite), Delete, Multipart Upload (initiate / complete), Rename (native clients),
  shortening or removing the retention (never allowed for Compliance).
* Allows: Get / Head, Get / Put Object Tagging, extending the retention.

#### Event Hold Trigger Workflow

```text
object created
      |
event hold applied (locked with no end date)
      |
business event occurs
      |
hold released
      |
RetainUntilDate is calculated
      |
locked until RetainUntilDate
```

Event holds use `PutObjectRetention`, as in AWS S3, so no new API or Ranger action is needed (native clients use `OzoneBucket#setRetention`).
OM handles the request under the key write lock and uses the leader-stamped request time as now.

The key stores `retentionDate` and the event hold (status and duration):
* The duration comes from the request, or from the bucket default when the key is created. Later changes to the bucket default do not affect existing keys.
* If the bucket default has both `rule` and `eventHold`, a new key gets `retentionDate` = creation time + `rule` and an active event hold.
  With only `eventHold`, `retentionDate` is unset until the hold is released.
* While the hold is `ON`, the effective `RetainUntilDate` is the later of `retentionDate` and now + duration, so it moves forward over time.
  `GetObjectRetention`, `HeadObject` and `GetObject` return this date.

Below, "earlier" and "later" compare the requested `RetainUntilDate` with the effective date. Only `GOVERNANCE` with the bypass can make it earlier; `COMPLIANCE` never can.

1. **Request format**: checked before the lock and the bypass; a request that breaks a rule gets `400 Bad Request`.
   * The request has a mode, and `RetainUntilDate`, `EventHold` or both.
     The only exception is an empty retention, which removes the retention: it needs the bypass for `GOVERNANCE` (`403` without it) and is never allowed for `COMPLIANCE`.
   * `RetainUntilDate` is in the future.
   * `EventHold=ON` has exactly one `EventHoldDuration` (positive, at most 36,500 days or 100 years). A `RetainUntilDate` in the same request is at least now + duration.
   * `EventHold=OFF` has no `EventHoldDuration`, and `EventHoldDuration` needs `EventHold=ON`.
   * The same applies to the `x-amz-object-lock-*` headers at creation: the mode header needs the retain-until-date header or the event hold headers, and they need the mode header.
2. **`EventHold=ON`** sets or updates the hold.
   * Without the bypass, `RetainUntilDate` can only be later (`403` otherwise). The duration can change;
     when it gets shorter, OM first saves the current effective date in `retentionDate`, so the date never moves earlier, even with the bypass.
   * With the bypass (`GOVERNANCE` only), `RetainUntilDate` can be earlier, but still at least now + the new duration (rule 1).
3. **`EventHold=OFF`** releases the hold. It is the only way to release it.
   * Without `RetainUntilDate`: `retentionDate` = the later of `retentionDate` and now + duration.
   * With `RetainUntilDate`: that date is used. An earlier date needs the bypass (`GOVERNANCE` only), otherwise `403`.
   * On a key without an active hold: no change if the key has a future `retentionDate`; `400` if neither the key nor the request has a future date.
4. **No `EventHold`** keeps the hold as it is and only changes `RetainUntilDate`.
   * A later date is accepted, also while the hold is `ON`.
   * An earlier date needs the bypass (`GOVERNANCE` only), otherwise `403`. While the hold is `ON`, it gets `400` even with the bypass: the request must use rule 2 or 3.

Releasing the hold keeps the mode. A different mode in the request is a mode change (see [Retention Updates](#retention-updates)).

> _**Note**_: AWS S3 requires versioning for Object Lock, so a Put on a locked object only adds a new version and Object Lock mainly blocks deletes.
> Ozone has no versioning yet, so it rejects every overwrite of a locked key.

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
  optional uint64 retentionDate = 2; // epoch milliseconds; the minimum date while the event hold is ON
  optional EventHold eventHold = 3;
}
```



### Access Control

A request on a locked key must pass two checks: authorization (native ACLs or Ranger) and the WORM check in OM.
Even with `CREATE`, `WRITE` or `DELETE`, overwriting or deleting a locked key is rejected with `403 Access Denied` (`WORMProtectionException`),
unless the Governance bypass applies (see [Retention Modes and Governance Bypass](#3-retention-modes-and-governance-bypass)).

The cluster uses one of two authorization models:
* [Native ACLs](#1-native-acls) have no per-action permissions, so changing locks needs the owner, and setting Compliance needs an Ozone administrator.
* [Ranger](#2-ranger) checks access types, and `action-matches` can limit a policy item to specific S3 actions. Use Ranger when each Object Lock operation needs its own permission.

In both models, keys that only get the bucket default retention need no extra check, because the default was authorized when it was set.
Native clients pass the same settings and the `bypassGovernanceRetention` flag as request arguments, and the same rules apply.

---

#### 1. Native ACLs

| Operation                                                                         | Resource     | Required            |
|:----------------------------------------------------------------------------------|:-------------|:--------------------|
| `GetBucketObjectLockConfiguration`                                                | Bucket       | `READ`              |
| `GetObjectRetention`, `GetObjectLegalHold`                                        | Key          | `READ`              |
| `PutBucketObjectLockConfiguration` (no default, or `GOVERNANCE`)                  | Bucket       | Owner               |
| `PutObjectRetention` (`GOVERNANCE`, extending, or releasing an event hold), `PutObjectLegalHold` | Key | Owner         |
| Governance bypass (including shortening or removing a `GOVERNANCE` retention)     | Key          | Owner               |
| Set `COMPLIANCE`: on a key, `GOVERNANCE` → `COMPLIANCE`, or as the bucket default (also changing a `COMPLIANCE` default) | Bucket / Key | Ozone administrator |

* **Owner**: the owner of the volume or bucket, the same owner that `OzoneNativeAuthorizer` already lets skip ACL checks. Ozone administrators can do everything an owner can.
  Locks use the owner instead of an ACL type like `WRITE_ACL`, because any ACL type can be granted to other users.
* **Ozone administrator**: a user in `ozone.administrators` or a group in `ozone.administrators.groups` (if neither is set, the user that starts OM).
  Read-only and S3 administrators are not included. The role is cluster wide; use Ranger for finer control.
* Reading lock information only needs `READ`, because `HeadObject` / `GetObject` already return it in the `x-amz-object-lock-*` headers.
* Setting Compliance needs an administrator because it cannot be undone through any API. Extending a Compliance retention or releasing its event hold only needs the owner, as in AWS S3.
  Keys that get a Compliance bucket default need no administrator check.

##### Extra Checks for Object Lock Headers

Requests that carry Object Lock headers also need the owner or an administrator, checked in the same request after the normal ACL check:

| Request                                                             | Header or condition                                                                                    | Extra check                                              |
|:--------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------|:---------------------------------------------------------|
| `CreateBucket`                                                      | `x-amz-bucket-object-lock-enabled: true`                                                               | Owner                                                    |
| `PutObject`, `CopyObject` (destination), `CreateMultipartUpload`    | `x-amz-object-lock-mode`, `-retain-until-date`, `-event-hold`, `-event-hold-duration-days` / `-years` | Owner (`GOVERNANCE`), Ozone administrator (`COMPLIANCE`) |
| `PutObject`, `CopyObject` (destination), `CreateMultipartUpload`    | `x-amz-object-lock-legal-hold`                                                                         | Owner                                                    |
| `DeleteObject`, `DeleteObjects`, overwrite, rename (native clients) | `bypassGovernanceRetention` on a key under Governance retention                                        | Owner                                                    |

If the check fails, the request fails (`DeleteObjects` reports a per-key `AccessDenied`). `GetObject` / `HeadObject` need no extra check and always return the lock headers.
So a user who can only write data cannot set a lock by adding a header.

---

#### 2. Ranger

> _**Important**_: action-level permissions need the Ranger action matcher, which is `false` by default.
> Without it, every principal with `WRITE` on a key can also change its locks, including setting a Compliance retention.
> ```properties
> ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition=true
> ```

* Object Lock uses the same access types as data: `READ` to read lock information, `WRITE` to change it, and `WRITE` and `DELETE` for the Governance bypass.
  They apply to every client: S3 Gateway, ofs / o3fs and the Ozone CLI.
* Ranger has no administrator role for Object Lock. `action-matches` is what separates lock management from data access, including setting Compliance.
  No Ranger permission can remove a Compliance retention; only the offline `ozone repair` command can (see [Offline Compliance Retention Repair](#offline-compliance-retention-repair)).

##### Access Type Mapping

Policy items without `action-matches` are checked by access type only.

| S3 Action                          | Resource | Access type          | Description                                                                 |
|:-----------------------------------|:---------|:---------------------|:----------------------------------------------------------------------------|
| `GetBucketObjectLockConfiguration` | Bucket   | `READ`               | Read the bucket's Object Lock settings.                                     |
| `PutBucketObjectLockConfiguration` | Bucket   | `WRITE`              | Set the bucket's default retention (mode, period, event hold).              |
| `GetObjectRetention`               | Key      | `READ`               | Read the retention mode, date and event hold.                               |
| `PutObjectRetention`               | Key      | `WRITE`              | Set, extend, shorten or remove a retention; set or release an event hold.   |
| `GetObjectLegalHold`               | Key      | `READ`               | Read the legal hold.                                                        |
| `PutObjectLegalHold`               | Key      | `WRITE`              | Set the legal hold `ON` or `OFF`.                                           |
| `BypassGovernanceRetention`        | Key      | `WRITE` and `DELETE` | Bypass Governance retention.                                                |

##### `action-matches`

* With `enableActionMatcherInPoliciesCondition`, a policy item can have an `action-matches` condition: a list of S3 actions (`*` and `Prefix*` allowed)
  that `RangerActionMatcher` compares with the action of the request.
* It only narrows an item: the item applies when both the access type and the action match. Items without it behave as before.
* Example: grant a legal team `WRITE` with `action-matches: PutObjectLegalHold`, so it can change legal holds but not retention.
* S3 Gateway sets the action in `S3GActionIamMapper`, and OM passes it to Ranger in the `RequestContext`. OM only adds the dependent actions below.
* Today S3 Gateway only sends the action when `ozone.s3g.sts.http.enabled=true`. This design depends on [HDDS-17054](https://issues.apache.org/jira/browse/HDDS-17054), which sends it for every S3 request.
* For requests without an S3 action (ofs, o3fs, Ozone CLI, Java API), see [Trusted Boundary](#trusted-boundary-and-compliance-scope).

##### Dependent Permissions

As in AWS S3 ([required permissions for S3 API operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html)),
some requests also need a second action, depending on their headers or on the key:

| S3 API                                                    | Main S3 action       | Header or condition                                                                                    | Dependent S3 action                        | Access type             | If denied                                   |
|:----------------------------------------------------------|:---------------------|:-------------------------------------------------------------------------------------------------------|:-------------------------------------------|:------------------------|:--------------------------------------------|
| `CreateBucket`                                            | `CreateBucket`       | `x-amz-bucket-object-lock-enabled: true`                                                               | `PutBucketObjectLockConfiguration`         | `WRITE` (bucket)        | Request fails                               |
| `PutObject`, `CopyObject` (destination), `CreateMultipartUpload` | `PutObject`   | `x-amz-object-lock-mode`, `-retain-until-date`, `-event-hold`, `-event-hold-duration-days` / `-years` | `PutObjectRetention`                       | `WRITE` (key)           | Request fails                               |
| `PutObject`, `CopyObject` (destination), `CreateMultipartUpload` | `PutObject`   | `x-amz-object-lock-legal-hold`                                                                         | `PutObjectLegalHold`                       | `WRITE` (key)           | Request fails                               |
| `GetObject`, `HeadObject`                                 | `GetObject`          | The key has a retention or a legal hold                                                                | `GetObjectRetention`, `GetObjectLegalHold` | `READ` (key)            | OM drops the lock information, so S3 Gateway omits the `x-amz-object-lock-*` headers; the read succeeds |
| `DeleteObject`, `DeleteObjects`                           | `DeleteObject`       | `x-amz-bypass-governance-retention: true` on a key under Governance retention                          | `BypassGovernanceRetention`                | `WRITE`, `DELETE` (key) | Request fails (`DeleteObjects`: per-key `AccessDenied`) |
| `PutObjectRetention`                                      | `PutObjectRetention` | Shortens or removes a Governance retention                                                             | `BypassGovernanceRetention`                | `WRITE`, `DELETE` (key) | Request fails                               |
| `PutObject`, `CopyObject`, `CompleteMultipartUpload`, rename (native clients) | `PutObject` | Overwrites or renames a key under Governance retention with `bypassGovernanceRetention`    | `BypassGovernanceRetention`                | `WRITE`, `DELETE` (key) | Request fails                               |

A dependent action uses the same access type as the main action, so it only matters for policy items with `action-matches`.
With such items, a data writer cannot set its own retention with a header, and a retention manager cannot upload data.

**How it works**: S3 Gateway sends one RPC with the main action. OM calls `checkAccess` once per action, each with one access type and one action, and every call must pass.
For example, `PutObject` with retention headers is checked as (`CREATE` / `WRITE`, `PutObject`) and then (`WRITE`, `PutObjectRetention`). Each call is its own Ranger audit record.

This extends the existing `DEPENDENT_S3_ACTIONS` check in `OmMetadataReader` (today `GetObjectAttributes` → `GetObject`; write requests reach it through `OMClientRequest#checkAcls`).
Instead of a fixed map, the OM request passes the (access type, action) pairs to check:
1. Only when the condition holds, for example `PutObjectRetention` only when the request has retention headers.
2. Each with its own access type and resource, for example `WRITE` on the key for `PutObjectRetention`, while `PutObject` also checks `CREATE` on the bucket.
3. For `GetObject` / `HeadObject`, only when the key has lock information, and a denial does not fail the read: OM drops the lock information.
   This needs no extra RPC. Listings do not return lock information, so they need no check per key.

These checks also run for requests without an S3 action, by access type only.

##### Component Updates

* Ranger `security-admin/src/main/webapp/react-webapp/src/utils/actionRequirements/ozone.json`: register the 7 actions with their access types and resource levels, so the Ranger UI rejects invalid policy items.
* `S3GActionIamMapper`: map `?legal-hold`, `?retention` and `?object-lock` requests to the actions.
* `IamSessionPolicyResolver`: recognize the new actions in STS session policies, with the same access types.

---

#### 3. Retention Modes and Governance Bypass

* **Compliance**: no one can overwrite, shorten or delete the key through the Ozone API until the retention expires, including administrators and `BypassGovernanceRetention`.
  With Ranger, only the offline `ozone repair` command can change it.
* **Governance**: protects keys from accidental deletion, and authorized users can bypass it. A bypass needs both:
  1. The `bypassGovernanceRetention` flag: S3 Gateway sets it from `x-amz-bypass-governance-retention: true`, and native clients pass it as an argument.
     It is carried in `KeyArgs` and `DeleteKeyArgs` for delete, batch delete, overwrite, multipart upload, rename and `PutObjectRetention`.
     Because Ozone has no versioning yet, this flag is the only way to delete, overwrite or rename a Governance-locked key.
  2. The bypass permission: the owner or an Ozone administrator with native ACLs; with Ranger, `WRITE` and `DELETE` on the key, plus `BypassGovernanceRetention` if the item has `action-matches`.

  Otherwise the request fails with `403 Access Denied`.

##### Retention Updates

`PutObjectRetention` on a key that already has a retention follows AWS S3 (see [Event Hold Trigger Workflow](#event-hold-trigger-workflow)).
"Longer" and "shorter" compare the requested `RetainUntilDate` with the current effective date:

| Change                                                                              | `GOVERNANCE`                                     | `COMPLIANCE`                         |
|:------------------------------------------------------------------------------------|:-------------------------------------------------|:-------------------------------------|
| Longer retention (later `RetainUntilDate`, longer event hold duration, `EventHold=ON`) | Allowed                                        | Allowed                              |
| Shorter event hold duration while the hold is `ON`                                  | Allowed; the effective date is kept              | Allowed; the effective date is kept  |
| Shorter retention (earlier `RetainUntilDate`, also on release) or removal (an empty retention) | Needs the bypass; while the hold is `ON`, an earlier `RetainUntilDate` must come with `EventHold` | Rejected with `403 Access Denied` |
| Release the event hold (`EventHold=OFF` without `RetainUntilDate`)                  | Allowed                                          | Allowed                              |
| Change the mode                                                                     | `GOVERNANCE` → `COMPLIANCE`: allowed             | `COMPLIANCE` → `GOVERNANCE`: rejected |

Who may send `PutObjectRetention` depends on the model: see [Native ACLs](#1-native-acls) and [Ranger](#2-ranger).

> _**Warning**: Compliance mode is permanent_
>
> In AWS S3, Compliance objects are only destroyed when the whole AWS account is closed. Ozone has no root or tenant account,
> so no administrator, native ACL or Ranger policy can remove a Compliance lock through the Ozone API before it expires.
> With Ranger, it can only be changed offline with `ozone repair` (see below).

### New Ozone APIs

Showing Object Lock settings in the ozone shell is left to a follow-up (see [Future Work](#future-work)).

**OzoneBucket**

```java
public void setRetentionConfig(RetentionArgs retentionArgs);
public RetentionArgs getRetentionConfig();
public void setRetention(String keyName, ObjectRetention retention, boolean bypassGovernanceRetention);
public ObjectRetention getRetention(String keyName);
public void setLegalHold(String keyName, boolean hold);
public boolean getLegalHold(String keyName);
```

* `setRetention` with an empty retention removes it (see [Retention Updates](#retention-updates)).
* `createKey`, `createStreamKey` and `initiateMultipartUpload` get overloads that take the retention and legal hold of the new key.
* `createKey`, `completeMultipartUpload`, `deleteKey`, `deleteKeys` and `renameKey` get overloads that take `bypassGovernanceRetention`.

### Supported S3 APIs

S3 Gateway supports the standard AWS S3 APIs, so existing clients such as the AWS CLI and Boto3 work unchanged.
As in AWS S3, `HeadObject` and `GetObject` return the lock headers (`x-amz-object-lock-mode`, `-retain-until-date`, `-event-hold`, `-event-hold-duration-days` / `-years`, and `-legal-hold`).
With Ranger, they need `GetObjectRetention` / `GetObjectLegalHold`; without them the headers are omitted and the read still succeeds (see [Dependent Permissions](#dependent-permissions)).

**Bucket APIs:**
* `CreateBucket`: enables Object Lock with the `x-amz-bucket-object-lock-enabled: true` header.
* `GetBucketObjectLockConfiguration`: returns whether Object Lock is enabled and the default retention.
* `PutBucketObjectLockConfiguration`: enables Object Lock and sets the default retention on an existing bucket. The `x-amz-bucket-object-lock-token` header is supported, as in AWS S3.
  The default retention can include `DefaultEventHold`, so new keys start with an active event hold.
* Bucket links (for example `/s3v/bucket1` → `/vol1/bucket1`): `OMBucketSetPropertyRequest` rejects changes on links, but `PutBucketObjectLockConfiguration` follows the link
  and stores the configuration on the source bucket, and `GetBucketObjectLockConfiguration` reads it from there. Authorization is checked on both, like other requests that resolve links.
  Key operations already resolve links, so WORM checks always use the source bucket.

**Key APIs:**
* `PutObject` / `CreateMultipartUpload`: set the retention and legal hold at creation with the `x-amz-object-lock-mode`, `-retain-until-date`, `-event-hold`,
  `-event-hold-duration-days` / `-years` and `-legal-hold` headers. These headers need extra permissions (see [native ACLs](#extra-checks-for-object-lock-headers) and [Ranger](#dependent-permissions)).
* `PutObjectRetention`: sets the mode, `RetainUntilDate`, `EventHold` and `EventHoldDuration`.
  See [Event Hold Trigger Workflow](#event-hold-trigger-workflow) and [Retention Updates](#retention-updates) for the rules, including removal and the `x-amz-bypass-governance-retention` header.
* `GetObjectRetention`: returns the mode, `RetainUntilDate` and event hold status. While the hold is `ON`, it also returns the duration, and the date is the computed one.
  After release, the duration is not returned. Without a retention, it returns `NoSuchObjectLockConfiguration`.
* `PutObjectLegalHold` / `GetObjectLegalHold`: set or read the legal hold.

**Affected APIs:**
On a locked key, these requests are rejected with `403 Access Denied` (`WORMProtectionException`).
Under Governance retention without a legal hold, they are allowed with `bypassGovernanceRetention` and the bypass permission (see [Retention Modes and Governance Bypass](#3-retention-modes-and-governance-bypass)).
* `PutObject` / `CopyObject` (overwrite).
* `DeleteObject` / `DeleteObjects`.
* `CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload`.
* Rename (`RenameKey` / `RenameKeys`): S3 Gateway returns `NotImplemented`, but native clients (for example `ozone sh key rename`) and the lifecycle service (moving keys to trash) can rename OBS keys.
  A rename removes the source key, so it is checked like a delete.

Other changes:
* `CopyObject` does not copy the source's Object Lock settings, as in AWS S3. The destination gets the bucket default or the request's `x-amz-object-lock-*` headers.
* A new OM result code for `WORMProtectionException`, which S3 Gateway maps to `403 AccessDenied`. `DeleteObjects` reports it per key.
* Lifecycle expiration skips locked keys. It already reads each key's `OmKeyInfo` during the scan, so this needs no extra lookup, and locked keys are not resent on every run.
  A skipped key is checked again in later scans and expires once unlocked. The WORM check in `DeleteKeys` / `RenameKey` still guards keys that get locked after selection.

> _**Note**_: Object Lock cannot be disabled on a bucket once enabled (`objectLockEnabled = true`).
> AWS S3 enables versioning together with Object Lock. Ozone keeps them separate for now because it has no versioning;
> once versioning exists, enabling Object Lock will enable versioning, and versioning cannot be suspended afterwards.

### Impact on Write and Delete Flows

**Put Object**

![object-lock-put-object.png](object-lock-put-object.png)

* Create Key: a WORM check in `preExecute` fails fast. It does not need to be linearizable, and it avoids sending doomed requests through Ratis.
* Commit Key: the final WORM check in `validateAndUpdateCache` is linearizable, so only one write can succeed on a locked key.
  Keys that fail to commit stay as open keys and are cleaned up by the existing background service.

**Multipart Upload**

* Initiate and Upload Part: fail-fast WORM check in `preExecute`, as for Create Key.
* Complete: final WORM check in `validateAndUpdateCache`, as for Commit Key.
* The `x-amz-object-lock-*` headers of Create Multipart Upload are checked and stored with the upload, and applied to the key on complete.

**Delete and Rename**

The WORM check runs in `validateAndUpdateCache`, so it is linearizable.

### Offline Compliance Retention Repair

Account closure, as in AWS, is out of scope. The question here is whether a Compliance retention can be removed at all.
Following the `ozone repair` convention, an offline command updates the retention of one key or a batch of keys:

```text
ozone repair om object-lock --bucket /<volume>/<bucket> [--key <key> | --prefix <prefix>] [--dry-run]
```

* **Only for Ranger**: Ranger has no administrator role for Object Lock, so anyone with `PutObjectRetention` (`WRITE`) can set Compliance, and no Ranger permission can remove it.
  A Compliance retention set by mistake can only be removed with this command. With native ACLs only Ozone administrators can set Compliance (see [Native ACLs](#1-native-acls)), so the command is not needed.
* **Offline**: runs against the OM DB while OM is stopped, like other `ozone repair om` tools, so it cannot be reached through S3 Gateway or OM RPC.
* **Who**: Ozone administrators with access to the OM host and DB, which is already outside the API trusted boundary (see [Trusted Boundary](#trusted-boundary-and-compliance-scope)).
* **What**: removes the retention of the selected keys or changes it from `COMPLIANCE` to `GOVERNANCE`. `--dry-run` lists the keys without changing them.
* **Audit**: every changed key is logged.

## Performance

The WORM check adds a little work to Create Key and Commit Key. The fail-fast check in `preExecute` keeps invalid requests out of Ratis,
and the final check in `validateAndUpdateCache` keeps it linearizable. The impact will be measured with benchmarks during implementation.

## Security

With native ACLs, changing locks and bypassing Governance need the volume or bucket owner, and setting Compliance needs an Ozone administrator.

With Ranger, the S3 Object Lock actions map to the existing access types (`READ`, `WRITE`, and `DELETE` for the bypass), and `action-matches` limits policy items to specific actions.
This gives least privilege without new access types.

### Trusted Boundary and Compliance Scope

AWS S3 runs on infrastructure that users cannot reach. Ozone is self-managed, so its protection is limited to a trusted boundary.

**Trusted Boundary**: the API level. Every request through the Ozone Java API, S3 Gateway, RPC and Ozone clients goes through the WORM check (`validateAndUpdateCache`) and authorization.
* A key under Governance retention can only be overwritten or deleted with the bypass flag and the bypass permission:
  * Native ACLs: the volume or bucket owner, or an Ozone administrator.
  * Ranger: `WRITE` and `DELETE` on the key, and `BypassGovernanceRetention` when the policy item has `action-matches`.
* A key under Compliance retention or a legal hold cannot be overwritten or deleted by anyone, including Ozone administrators, until the retention expires or the hold is removed.
* **With Ranger, only `action-matches` separates lock permissions.** `PutObjectRetention`, `PutObjectLegalHold` and `PutBucketObjectLockConfiguration` all map to `WRITE`, the same as writing data.
  * An item that grants `WRITE` without `action-matches` (or with `*`) allows every lock change, from any client, including setting Compliance, which no Ranger permission can undo.
  * To let a principal write data without changing locks, or only change some locks (for example only `PutObjectLegalHold`), use `action-matches` on its items,
    and make sure no other item grants it `WRITE` on the same resources without the condition. Ranger allows a request if any item matches.
* **Requests without an S3 action** (ofs, o3fs, Ozone CLI, Hadoop and Java API) go through the same WORM check and access type checks.
  * Items with a specific `action-matches` value do not match them; only items without the condition, or with `*`, do. The same applies to inline (session) policies.
  * So switching to a non-S3 client cannot get around an `action-matches` restriction: the request is denied.
  * Deployments with both S3 and non-S3 clients need items without `action-matches` for the non-S3 users. Those items allow every lock change, so grant them carefully.

**Out of scope**: OS `root` access or physical access to the nodes bypasses the API boundary. Object Lock does not protect against:
* **Database tampering**: changing key metadata directly in RocksDB while the node is offline.
* **Offline tools**: `ozone repair` (including the repair command above), restoring the OM DB from an older Ratis snapshot, or `skip-ratis-transaction`.
* **Storage deletion**: deleting block files on datanodes.
* **Clock tampering**: moving the system clock on OM or datanode hosts to expire retention early.
* **Ranger admin abuse**: changing Ozone policies with Ranger admin credentials or the Ranger database.

**Compliance recommendation**: for regulations such as SEC 17a-4 or FINRA, combine Object Lock with infrastructure controls:
strict OS access control, limited SSH / root access to OM and datanode hosts, and forwarding Ozone audit logs to a tamper-evident external SIEM.

## Compatibility

* **Table schema**: the new `BucketInfo` and `KeyInfo` fields are `optional`, so they are wire-compatible.
  To avoid problems if the cluster is rolled back, a new OM layout feature gates Object Lock: until finalization, OM rejects every Object Lock request
  (bucket configuration, `CreateBucket` with Object Lock, retention, legal hold, and `x-amz-object-lock-*` headers), so no Object Lock metadata is written.
* **OM version**: a new OM version lets clients check whether the OM supports the new Object Lock APIs during rolling upgrades.

## Alternatives

### 1. Why Not Provide a Volume Force Delete Command?

* **Proposal**: an admin command that force-deletes a volume with locked keys, like closing an AWS account (which deletes all data, including Compliance objects).
* **Why rejected**:
  1. Account closure is a separate problem and out of scope.
  2. The real question is whether a Compliance retention can be removed. A per-key `ozone repair` command answers it with a smaller scope and fits the existing repair tools.

### 2. Why Not Check Dependent Actions in One `checkAccess` Call or in S3 Gateway?

* **Proposal**:
  1. Pass several actions to one `checkAccess` call and extend `action-matches` to match a list.
  2. Let S3 Gateway send an extra RPC per dependent action, as `CopyObject` does for its source and destination.
* **Why rejected**:
  1. One call has one access type, but the actions may need different ones (for example `CREATE` on the bucket for `PutObject`).
     Matching any action would let an item that only allows `PutObject` grant `PutObjectRetention`; matching all would force both actions into one item,
     although they usually belong to different roles. It also needs a Ranger plugin change.
  2. S3 Gateway does not know the key state (whether a key has a lock or is under Governance retention), native clients do not use S3 Gateway,
     and a separate check adds an RPC and a gap between the check and the write.

## Plan

The work is split into the phases below. Details are left to the individual Jiras.

### Phase 1: Metadata & Upgrade

* `BucketInfo` / `KeyInfo` fields, `BucketRetention` / `KeyRetention` messages, and the `bypassGovernanceRetention` flag.
* OM layout feature that rejects Object Lock requests until finalization, and a new OM version.
* A helper that decides whether a key is locked from `legalHold`, `eventHold` and `retentionDate`, using the leader-stamped request time.

### Phase 2: Authorization

* Native ACLs: `READ` to read locks, the owner to change locks and bypass Governance, an Ozone administrator for Compliance,
  and the [extra checks for Object Lock headers](#extra-checks-for-object-lock-headers).
* Ranger: register the 7 actions in `S3GActionIamMapper`, `IamSessionPolicyResolver` and Ranger's `ozone.json` with the [access types](#access-type-mapping). Depends on HDDS-17054.
* Ranger [dependent permissions](#dependent-permissions): replace the fixed `DEPENDENT_S3_ACTIONS` map with (access type, action) pairs from the OM request, also checked for requests without an S3 action.
* User documentation: the native ACL owner model, the Ranger action matcher, and that without `action-matches` every principal with `WRITE` can change locks, including setting Compliance.

### Phase 3: Object Lock Management APIs

* Bucket: `CreateBucket` with Object Lock, `PutBucketObjectLockConfiguration`, `GetBucketObjectLockConfiguration` (following bucket links).
* Key: `PutObjectRetention` / `GetObjectRetention` (with event hold), `PutObjectLegalHold` / `GetObjectLegalHold`.
* The rules in [Event Hold Trigger Workflow](#event-hold-trigger-workflow) and [Retention Updates](#retention-updates).
* `x-amz-object-lock-*` headers on `HeadObject` / `GetObject`, dropped by OM when the caller lacks the permission.
* `OzoneBucket` APIs and overloads.

### Phase 4: WORM Enforcement

* Apply the bucket default or the `x-amz-object-lock-*` headers when a key is created, with the same format checks as `PutObjectRetention`.
  For multipart upload, store the headers at initiate and apply them on complete.
* Reject overwrite (commit, multipart complete), delete (single and batch) and rename of locked keys in `validateAndUpdateCache`, with fail-fast checks in `preExecute`.
* OM result code for `WORMProtectionException`, mapped to `403 AccessDenied` by S3 Gateway (per key for `DeleteObjects`).
* Lifecycle skips locked keys.

### Phase 5: Offline Compliance Retention Repair

* `ozone repair om object-lock` for Ranger deployments: remove or downgrade Compliance retention per key or prefix while OM is offline, with `--dry-run` and a log of every change.

### Phase 6: Testing

* Unit tests for the lock check, each OM request and the repair command.
* Integration tests for S3 Gateway to OM, native ACLs, bucket links, lifecycle and layout feature gating.
* S3 compatibility tests for the event hold and retention rules, including the `400` and `403` cases, matching the AWS S3 behavior verified for this design.
* Ranger acceptance tests in `ozonesecure-ha`, also with STS disabled: items with and without `action-matches`, a data writer that cannot change locks,
  the dependent permissions, and requests without an S3 action.

### Future Work

* **S3 versioning**: existing keys become the latest version and keep their locks; overwrites create new versions instead of being rejected, and locks apply per version.
* **FSO buckets**: lock granularity for files and directories, and rename / move rules.
* **Ozone shell**: show `objectLockEnabled`, the default retention, and each key's retention and legal hold in `ozone sh`.
* **Value-level Ranger authorization**: a condition evaluator on `s3:object-lock-legal-hold`, `s3:object-lock-mode`, `s3:object-lock-event-hold` and `s3:object-lock-event-hold-duration-days`,
  for example to allow setting a legal hold `ON` but not `OFF`, or only `GOVERNANCE` mode.

## References

- [AWS S3 Object Lock User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [AWS S3 PutBucketObjectLockConfiguration API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketObjectLockConfiguration.html)
- [AWS S3 PutObjectRetention API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html)
- [AWS S3 PutObjectLegalHold API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html)
- [HDDS-15945: S3 Object Lock Support in Apache Ozone](https://issues.apache.org/jira/browse/HDDS-15945)
- [Ozone STS Design](ozone-sts.md)

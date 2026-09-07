---
title: S3 Object Lock
summary: Design to support S3 object lock.
date: 2026-08-18
jira: HDDS-15945
status: accepted
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

This design document aims to plan and implement the Object Lock mechanism for OBS buckets integrated with Ranger.
The primary objective is to provide data immutability and tamper-proof protection through the object locking feature.

## Problem statement

With growing demands for data security and compliance, ensuring that critical data stored in OBS (Object Storage) is
protected from accidental or malicious deletion and overwriting has become an essential system protection requirement.
To establish a more rigorous data protection mechanism, we plan to introduce the Object Lock feature.

Considering the current system architecture and access control strategies,
this design integrates with existing Apache Ranger to manage Object Lock permissions on OBS buckets.
Meanwhile, to accelerate core feature delivery, we have decided to exclude complex multi-version locking (Versioning Lock) 
and legacy FSO buckets from this initial release. In addition, support for Native ACLs is excluded; Native ACLs typically grant permissions 
at the granular bucket or object level, whereas Object Lock permission management favors broad, role-based authorization,
creating a conflict in design philosophies. Narrowing the scope allows us to focus on the core functionality and ensure a rapid, 
stable rollout of baseline tamper-proof protection.

**Goal:**

* Implement the Object Lock feature on standard OBS buckets, fully integrated with Ranger for permission and access control. 
* Support single-version objects only.

## Non-Goal

* Versioning Lock: Support for locking across multiple object versions is deferred (multi-version core features are currently under development).
* FSO Legacy Buckets: Object Lock support for legacy FSO buckets is excluded.
* Native ACL Support: Native ACLs will not be used for access control or advanced configuration such as Retention Mode (Governance); access management is centralized exclusively via Ranger.

## 5. Technical Description

### Terminology

**Legal Hold**

* **Definition**: Applies an indefinite lock status to an object. The object remains protected until an administrator explicitly removes the lock (Remove Legal Hold). 
* **Restricted Operations**:
  * Put Object 
  * Delete Object 
  * Multipart Initial / Complete
* **Allowed Operations**:
  * Get Object
  * Get Legal Hold 
  * Put Legal Hold (Depends on permission)

**Retention**

* Definition: Configures a fixed retention duration (specified in days or years) for an object and applies a specific retention mode. 
* Retention Modes:
  * Compliance Mode: The strictest protection tier. Once applied, no user (including root/admin) can remove the lock, shorten the duration, or overwrite the object before the retention period expires. 
  * Governance Mode: A flexible protection tier. Standard users are restricted by locking rules, but users with bypassgovernance permissions can bypass restrictions to perform modifications or deletions.
* Restricted Operations:
  * Put Object 
  * Delete Object 
  * Multipart Initial/Complete 
  * Set Retention Period
* Allowed Operations:
  * Get Object

> _**Note**:
> * Background & Root Cause: A prerequisite for enabling WORM (Write Once, Read Many) in AWS S3 is that Object Versioning must be enabled. Under S3 architecture, executing a Put on a locked object generates a new version without affecting the protected prior version; thus, S3 Object Lock primarily restricts Delete Object. 
> * Ozone Implementation Status: Because Ozone's versioning feature is still under development, to guarantee absolute immutability during the lock period, Ozone will directly block and reject all overwrite operations (such as any form of Put or overwrite) on locked objects.

### Table Changes

**Bucket Table**

Two new fields: objectLockEnabled & defaultRetention.

```protobuf
  message BucketInfo {
  // ... existing fields
  required bool objectLockEnabled = 24 [default = false];
  optional RetentionConfig defaultRetention = 25;
  }
  
  message RetentionConfig {
  optional RetentionMode retentionMode = 23;
  optional uint64 retainUntilDate = 24;
  }
  
  enum RetentionMode {
    GOVERNANCE = 1;
    COMPLIANCE = 2;
  }
```



**Key Table**

Two new fields: retentionConfig & legalHold.

```protobuf
  message KeyInfo {
  // ... existing fields
  optional RetentionConfig retentionConfig = 23;
  optional bool legalHold = 24 [default = false];
  }
```



### Ranger Access Control

Ozone Object Lock enforces a dual-gate mechanism combining **Ranger authorization checks** and **underlying WORM state validation**:

* **Standard Data Operations (Put / Delete)**: Even if a user is granted Ranger `WRITE` or `DELETE` permissions, the operation is immediately denied with a `403 Access Denied` (`WORMProtectionException`) if the target object is actively locked by an unexpired retention period or an active Legal Hold.
* **Lock Management and Bypass Authorization**: To adhere to the Principle of The Least Privilege, dedicated Ranger Access Types are introduced to govern Legal Hold state transitions and Governance Mode bypass privileges:

#### 1. Legal Hold Access Control

Three dedicated Access Types are added to `accessTypeRestrictions` at the Key level to govern Legal Hold states:

* **`GET_LEGAL_HOLD`**: Allows querying the current Legal Hold status of an object (maps to the `GetObjectLegalHold` API).
* **`PUT_LEGAL_HOLD`**: Grants permission to apply a Legal Hold to an object (maps to `PutObjectLegalHold` with status ON). Once applied, the object is locked indefinitely, blocking all overwrite and delete operations.
* **`CLEAR_LEGAL_HOLD`**: Grants permission to remove a Legal Hold from an object (maps to `PutObjectLegalHold` with status OFF). Because this clears immutability protection, it is treated as a high-privilege action restricted only to authorized users.

#### 2. Retention Access Control and Governance Mode Bypass

For object retention, the interaction with Ranger policies depends on the configured Retention Mode:

* **Compliance Mode**: Serves as the strictest compliance tier. Until the retention period expires, **no role or Ranger permission can bypass or overwrite the lock**, including cluster administrators.
* **Governance Mode and `BYPASS_GOVERNANCE`**:
  * **Mechanism**: Governance Mode allows authorized users to overwrite, delete, or alter the retention duration of a locked object before its expiration date.
  * **Authorization Binding**: Introduces the **`BYPASS_GOVERNANCE`** Access Type, configured under `accessTypeRestrictions` at the **Volume level**.
  * **Enforcement Flow**: While regular users are strictly blocked from mutating locked objects in Governance Mode, any request attempting to overwrite or delete such objects requires explicit `BYPASS_GOVERNANCE` entitlement granted in the Ranger policy for that Volume. Without this explicit permission, even administrators using standard client tools cannot modify or remove the locked object.
### New Ozone APIs

**ObjectStore**

```java
   public void addRetentionConfig(OzoneObj obj, RetentionArgs retentionArgs);
   public void addLegalHold(OzoneObj obj, bool hold);
```

**OzoneBucket**

```java
public void setRetentionConfig(@Nullable RetentionArgs retentionArgs);
```

### Supported S3 APIs

To ensure seamless integration with existing S3 clients (e.g., AWS CLI, Boto3) and applications, the Ozone S3 Gateway (S3G) will translate and support the following standard AWS S3 Object Lock APIs:

**Bucket-Level APIs:**
* `GetBucketObjectLockConfiguration`: Retrieves the Object Lock status and the default retention configuration (if any) for a specified OBS bucket.
* `PutBucketObjectLockConfiguration`: Maps to the new Ozone API to enable Object Lock for an existing bucket. Configuring default retention rules (DefaultRetention) is optional.

**Object-Level APIs:**
* `PutObjectRetention`: Places a retention configuration on an object, specifying the retention mode (COMPLIANCE or GOVERNANCE) and the `RetainUntilDate`.
* `GetObjectRetention`: Retrieves the current retention configuration applied to an object.
* `PutObjectLegalHold`: Applies or removes a Legal Hold configuration to the specified object.
* `GetObjectLegalHold`: Retrieves the current Legal Hold status of an object.

**Impacted Standard APIs:**
Standard data mutating APIs will intercept the Object Lock state and return the `403 Access Denied` (or specifically `WORMProtectionException`) if a modification is attempted on a locked object:
* `PutObject` / `CopyObject` (Overwrites will be rejected)
* `DeleteObject` / `DeleteObjects` (Deletions will be rejected)
* `CreateMultipartUpload` / `CompleteMultipartUpload` / `UploadPart` (Uploads will be rejected if the object is locked)

> _**Note**:
> * **Object Lock Immutability & Future Versioning Binding**:
>   - **Current State (No Versioning)**: Once Object Lock is enabled on a bucket (`objectLockEnabled = true`), it cannot be disabled.
>   - **Future Evolution (With Versioning)**: When S3 Versioning is supported, Object Lock and Versioning will be tightly coupled following AWS S3 semantics: once Object Lock is enabled, Versioning cannot be suspended or disabled.
> * **Bucket Creation Semantics**:
>   AWS S3 supports enabling Object Lock and configuring RetentionConfig directly during bucket creation.
>   In Apache Ozone's current design, these settings must be configured via dedicated API calls after the bucket has been created.

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

## Performance

Introducing WORM validation into critical execution paths (such as Create Key and Commit Key) incurs minor overhead.
To minimize impact, this design employs a two-phase validation strategy: preliminary filtering in `preExecute` provides fail-fast pruning to prevent invalid requests from reaching the consensus layer, followed by atomic validation in `validateAndUpdateCache` for linearizability.
Preliminary assessments indicate that the additional metadata lookups impose an acceptable impact on overall system throughput and latency. Metrics will be continuously monitored and tuned as necessary.

## Security

Centralized access control via Ranger ensures all Object Lock operations (such as Retention Policy configuration and Legal Hold management) are enforced under strict access permissions. Dedicated Access Types (BYPASS_GOVERNANCE, PUT_LEGAL_HOLD, etc.) enforce the Principle of Least Privilege, preventing unauthorized tampering or removal of locks and enhancing data immutability.

### Trusted Boundary and Compliance Scope

While AWS S3 can guarantee absolute immutability across all levels because its infrastructure is entirely managed and abstracted from users, Apache Ozone is a self-managed, software-defined storage system. It is important to define the trusted boundary to understand the scope of data protection and compliance.

**Trusted Boundary:**
The trusted boundary for this Object Lock design is defined at the **Application API Level**.
* All data and metadata operations routed through the Ozone Java API, S3 Gateway, RPC interfaces, and standard Ozone clients are strictly governed by the Object Lock validation (`validateAndUpdateCache`) and Ranger access policies.
* Within this boundary, immutability is strictly enforced. Unless explicitly authorized with specific privileged permissions (e.g., s3:BypassGovernanceRetention), even cluster administrators using standard client tools cannot overwrite or delete locked objects until the retention period expires or the legal hold is removed.

**Out of Scope (Infrastructure Level bypass):**
Since Ozone runs on customer-managed infrastructure, operations executed with OS-level `root` privileges or physical access to the server nodes bypass the application-level API boundary. The following scenarios are not protected by Ozone Object Lock:
* **Direct Database Tampering:** Modifying or deleting key metadata directly from the underlying RocksDB instances while the node is offline.
* **Low-Level DR Tools:** Using disaster recovery tools like `ozone repair` to explicitly restore the OM's DB directory from an older Ratis snapshot (taken before the lock was applied) or running `skip-ratis-transaction` to skip retention state entries.
* **Storage Level Deletion:** Directly deleting block files from the DataNode filesystems.

**Compliance Recommendation:**
To achieve strict regulatory compliance (e.g., SEC 17a-4 or FINRA WORM requirements) in an on-premise deployment, Ozone's Object Lock must be complemented with strict infrastructure security measures. This includes enforcing rigid OS-level RBAC/IAM, restricting SSH/root access to OM/DN nodes, and continuously forwarding Ozone audit logs to a tamper-evident, external SIEM system to detect manual offline interventions.

## Compatibility

Because this design alters the schemas of Bucket Table and Key Table, OM (Ozone Manager) version leveling will be introduced to maintain forward and backward compatibility across rolling upgrades.

## Alternatives

### 1. Why Not Use Native ACLs for Object Lock Management?

* **Proposal**:
  Support Native ACLs alongside or instead of Ranger to configure and manage Object Lock states (e.g., using Ozone native ACLs or POSIX-like ACLs at the Volume/Bucket/Key level).

* **Why Rejected**:
  1. **Conflicting Security & Authorization Philosophies**:
     Native ACLs in Ozone are primarily designed for granular, resource-centric permissions (often granting permissions directly to specific objects/buckets or inherited via path hierarchies). In contrast, Object Lock is an enterprise compliance feature that requires strict, role-based access governance (RBAC/ABAC). Compliance controls like WORM and Legal Holds are inherently tied to compliance officers, security auditors, or legal teams—not individual data owners.
  2. **Risk of Decentralized & Inconsistent Governance**:
     Native ACLs allow object creators or bucket owners to adjust permissions. If lock controls were exposed through Native ACLs, standard bucket administrators or key owners could modify or override lock-related ACLs locally. Centralizing lock authorization exclusively in **Apache Ranger** ensures that compliance policies are managed from a unified pane of glass, preventing local policy drift or accidental misconfigurations.
  3. **Operational Complexity**:
     Supporting dual authorization models (Ranger + Native ACLs) for critical compliance features introduces unnecessary complexity, potential security loopholes, and ambiguous resolution hierarchies during audits. Excluding Native ACLs ensures clean boundary enforcement.

### 2. Why Introduce New Access Types in Ranger Instead of Reusing Existing Permissions (e.g., WRITE / DELETE)?

* **Proposal**:
  Reuse standard data mutating permissions—such as granting users with `WRITE` or `ALL` the right to put/clear Legal Holds and configure Retention, or allowing users with `DELETE` to bypass Governance mode.

* **Why Rejected**:
  1. **Violation of Separation of Duties (SoD)**:
     Object Lock is fundamentally an identity- and role-bound compliance mechanism, not merely a flag toggled on an object. Its primary objective is to protect critical data against accidental deletion, ransomware, or malicious overwrites—**specifically from actors who already possess standard `WRITE` or `DELETE` permissions** (e.g., operational scripts, ETL pipelines, compromised credentials, or everyday developers).
  2. **Renders Lock Protection Ineffective (Self-Unlock & Delete)**:
     If lock management is trivially bound to existing `WRITE` or `DELETE` permissions, any user authorized to modify the data could simply unlock the object (or clear the Legal Hold) and proceed to delete or overwrite it. In such a design, the lock mechanism loses its entire defensive value.
  3. **Enforcing Independent Privileged Roles**:
     To achieve true tamper-proofing, we must be able to block users who have `WRITE` permissions from modifying locked data, while reserving lock overrides exclusively for independent roles:
    - **Decoupled Lifecycle Actions**: Clearing a Legal Hold (`CLEAR_LEGAL_HOLD`) or bypassing retention (`BYPASS_GOVERNANCE`) is a high-privilege administrative action that must be granted separately from routine data operations.
    - **Dual-Gate WORM Enforcement**: Routine data writers operate with standard `WRITE`/`DELETE`, but once a lock is active, they are completely blocked. Only security or compliance personas holding the dedicated Ranger access types can intervene.


## Plan

All Ranger integration capabilities are currently verified via Smoke Tests. We plan to add dedicated test suites under the Smoke Test framework to validate S3 WORM functionality across diverse permission scenarios.

Once S3 Versioning support matures, future efforts will focus on ensuring compatibility with S3 multi-version object locking.

## References

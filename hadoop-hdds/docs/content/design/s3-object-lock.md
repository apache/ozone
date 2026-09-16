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
, Legacy buckets, and FSO buckets from this initial release. In addition, support for Native ACLs is excluded; Native ACLs typically grant permissions 
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

## Technical Description

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

* Definition: Configures a retention policy for an object to prevent deletion or modification. Retention can be duration-based (configured in days or years, establishing a fixed `RetainUntilDate`) or event-driven (**Event Hold / Event-based Retention**, where an object remains protected indefinitely until an external business or legal event triggers the final retention countdown).
* Retention Modes:
  * Compliance Mode: The strictest protection tier. Once applied, no user (including root/admin) can remove the lock, shorten the duration, or overwrite the object before the retention period expires.
  * Governance Mode: A flexible protection tier. Standard users are restricted by locking rules, but users with `BypassGovernanceRetention` permissions can bypass restrictions to modify or delete the object.
* Event Hold / Event-based Retention:
  * Used for records management where the retention lifecycle depends on external events (e.g., contract termination, employee departure, loan closure).
  * Keeps the object immutable while awaiting event notification; once the event occurs, the definitive expiration date (`RetainUntilDate`) is calculated and applied.
* Restricted Operations:
  * Put Object / Copy Object (Overwrites)
  * Delete Object
  * Multipart Upload (Initial / Complete)
  * Shorten Retention Period (in Compliance Mode)
* Allowed Operations:
  * Get Object
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

  message  RetentionConfig {
    optional Rule rule = 1;
    optional EventHold eventHold = 2;
  
  }
  
  message EventHold {
    required bool enabled = 1;
    required Rule rule = 2;
  }
  
  message Rule {
    required RetentionMode retentionMode = 1;
    required TimeUnit timeUnit = 2;
    required uint64 duration = 3;
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

* **retentionDate**: Represents the expiration timestamp of the retention lock. While clients or API calls specify retention duration in terms of days or years, Ozone calculates the definitive expiration date upon applying the rule and persists it as a timestamp into key table.
* **legalHold**: Indicates whether an explicit legal hold is active on the key.

Two new fields: retentionDate & legalHold.

```protobuf
  message KeyInfo {
  // ... existing fields
  optional string retentionDate = 23;
  optional bool legalHold = 24 [default = false];
  }
```



### Ranger Access Control

Ozone Object Lock enforces a dual-gate protection mechanism combining **Ranger policy-based authorization** and **underlying OM WORM state validation**:

* **Standard Data Operations (Put / Delete)**: Even if a principal has valid Ranger `WRITE` or `DELETE` access, any attempt to mutate, overwrite, or delete an object under an active Legal Hold or an unexpired Retention period is immediately rejected with `403 Access Denied` (`WORMProtectionException`).
* **Fine-Grained S3 Action Matching**: Object Lock management integrates with the action-matching authorization framework introduced via STS. S3 Object Lock actions map 1:1 to AWS S3 action names. See [AWS STS Design for Ozone S3](ozone-sts.md) for details.

---

#### 1. Configuration & Component Updates

To enable action-level authorization and onboard Object Lock actions into the Ozone & Ranger ecosystem, the following updates are required:

**Prerequisite: Feature Flag Activation**:
* Enable the Ranger action matcher condition in the Ozone configuration:
  ```properties
  ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition=true
  ```
* When enabled, Ozone S3 Gateway (S3G) intercepts the high-level S3 API operation and populates the S3 action name into the `RangerAccessRequest`, allowing Ranger's `RangerActionMatcher` to evaluate policy conditions against incoming requests.

1. **UI & Metadata Registration**:
  * Update `security-admin/src/main/webapp/react-webapp/src/utils/actionRequirements/ozone.json` in Apache Ranger:
    Register the 7 new Object Lock actions along with their prerequisite primitive permissions (e.g., `READ`, `WRITE`) mapped by resource level (Bucket vs. Key). This enforces design-time validation in the Ranger React UI, preventing administrators from creating invalid policy items.

2. **S3 Gateway & STS Mapping**:
  * Update **`S3GActionIamMapper`**: Map incoming HTTP request context and sub-resources (e.g., `?legal-hold`, `?retention`, `?object-lock`) to the corresponding action string.
  * Update **`IamSessionPolicyResolver`**: Ensure session-scoped policies (STS AssumeRole / federation tokens) recognize and evaluate the new Object Lock action strings.

---

#### 2. Action to Ranger Access Type Mapping

Following the declarative contract in `ozone.json`, each S3 action requires a baseline primitive Ranger permission at the designated resource hierarchy:

| S3 Action | Resource Level | Required Ranger Access Type | Description |
| :--- | :--- | :--- | :--- |
| **`GetBucketObjectLockConfiguration`** | Bucket | `READ` | Retrieve default Object Lock settings on a bucket. |
| **`PutBucketObjectLockConfiguration`** | Bucket | `WRITE` | Configure default retention mode and period on a bucket. |
| **`GetObjectRetention`** | Key | `READ` | Read the retention mode and Retain-Until date of an object version. |
| **`PutObjectRetention`** | Key | `WRITE` | Set or extend object retention mode and duration. |
| **`GetObjectLegalHold`** | Key | `READ` | Query the current Legal Hold status (`ON` or `OFF`). |
| **`PutObjectLegalHold`** | Key | `WRITE` | Toggle the Legal Hold state (`ON` or `OFF`). |
| **`BypassGovernanceRetention`** | Key | `WRITE` / `DELETE` | Privileged entitlement to bypass retention in Governance Mode. |

---

#### 3. Value-Level Authorization & Ranger Condition Evaluator

While API invocation is controlled by the S3 Action (e.g., granting `PutObjectLegalHold`), real-world governance often requires **role segregation based on request values** (e.g., Compliance Officers can toggle `ON`, but only external Auditors can toggle `OFF`; or preventing operators from setting `COMPLIANCE` mode).

To support this without fragmenting AWS-compatible Action definitions, Ozone and Ranger introduce a payload-aware **String Matcher Condition Evaluator**:

1. **Context Extraction in Ozone S3G**:
  * During request interception, S3G inspects the request body/headers and injects target attributes into the `RangerAccessRequest.getContext()`:
    * `s3:object-lock-legal-hold`: Extracted from the XML body (`ON` or `OFF`).
    * `s3:object-lock-mode`: Extracted from the XML body (`GOVERNANCE` or `COMPLIANCE`).

2. **Ranger Custom Condition Evaluator**:
  * Implement a dedicated condition evaluator extending `RangerAbstractConditionEvaluator` (registered in `ranger-servicedef-ozone.json`):
    * Evaluates `s3:object-lock-legal-hold` against configured policy criteria (`ON` vs. `OFF`).
    * Evaluates `s3:object-lock-mode` against configured policy criteria (`GOVERNANCE` vs. `COMPLIANCE`).
  * Enables fine-grained policies such as:
    * *Policy A (Evidence Preservation)*: Allow `PutObjectLegalHold` **ONLY IF** `s3:object-lock-legal-hold == "ON"`.
    * *Policy B (Safe Retention)*: Allow `PutObjectRetention` **ONLY IF** `s3:object-lock-mode == "GOVERNANCE"`.

---

#### 4. Retention Modes & Governance Bypass Enforcement

* **Compliance Mode**:
  * The strictest WORM protection tier.
  * Once applied, the lock is immutable. **No role, administrator, or Ranger permission** (including `BypassGovernanceRetention`) can overwrite, shorten, or delete the object version until the retention period expires.
* **Governance Mode & `BypassGovernanceRetention`**:
  * Protects objects from accidental deletion by general users while retaining operational flexibility for authorized personnel.
  * **Dual-Check Enforcement**:
    1. **Client Declaration**: The client must explicitly specify the `x-amz-bypass-governance-retention: true` header in the delete or overwrite request.
    2. **Ranger Action Entitlement**: Ozone checks whether the principal has the **`BypassGovernanceRetention`** action granted in Ranger for the target key resource.
    3. If either check fails, the request is aborted with `403 Access Denied`.


> _**Warning**:
> _Permanent Data Immutability in Compliance Mode_
> 
> Unlike AWS S3, where objects in Compliance mode cannot be deleted by any user (including root) and will only be destroyed if the entire AWS account itself is removed, Apache Ozone has no concept of a root or tenant account.
> Once applied, a Compliance lock cannot be overridden or deleted by any admin or Ranger policy until the retention period expires. 

### New Ozone APIs

**ObjectStore**

```java
   public void addRetentionConfig(OzoneObj obj, RetentionArgs retentionArgs);
   public void addLegalHold(OzoneObj obj, bool hold);
```

**OzoneBucket**

```java
public void setRetentionConfig(RetentionArgs retentionArgs);
```

### Supported S3 APIs

To ensure seamless integration with existing S3 clients (e.g., AWS CLI, Boto3) and applications, the Ozone S3 Gateway (S3G) will translate and support the following standard AWS S3 Object Lock APIs:

**Bucket-Level APIs:**
* `GetBucketObjectLockConfiguration`: Retrieves the Object Lock status and the default retention configuration (if any) for a specified OBS bucket.
* `PutBucketObjectLockConfiguration`: Maps to the new Ozone API to enable Object Lock for an existing bucket. Configuring default retention rules (DefaultRetention) is optional.

**Object-Level APIs:**
* `PutObjectRetention`: Places a retention configuration on an object, specifying the retention mode (COMPLIANCE or GOVERNANCE) and the retention duration (in days or years).
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
>   - **AWS S3 Alignment & Future Evolution**: In standard AWS S3, Object Lock inherently requires and automatically enables Versioning upon bucket creation. While Ozone temporarily decouples them due to unsupporting Versioning, future implementations will align with S3 semantics: enabling Object Lock will automatically enforce Versioning, and Versioning can never be suspended or disabled thereafter.

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
* **System Clock Tampering**: Artificially manipulating or rolling back the system clock (NTP/OS time) on OM or DataNode hosts to prematurely expire object retention periods or evade time-based WORM validation.
* **Ranger Super Admin Privilege Abuse**: Modifying, disabling, or overriding Ozone authorization policies via Apache Ranger admin credentials (or direct access to Ranger’s backend database), allowing privileged operators to alter bucket WORM flags or grant unrestricted deletion permissions outside normal enforcement workflows.

**Compliance Recommendation:**
To achieve strict regulatory compliance (e.g., SEC 17a-4 or FINRA WORM requirements) in an on-premise deployment, Ozone's Object Lock must be complemented with strict infrastructure security measures. This includes enforcing rigid OS-level RBAC/IAM, restricting SSH/root access to OM/DN nodes, and continuously forwarding Ozone audit logs to a tamper-evident, external SIEM system to detect manual offline interventions.

## Compatibility

* **Table Schema Changes**: Since all newly introduced fields in the Bucket Table (`objectLockEnabled`, `defaultRetention`) and Key Table (`retentionConfig`, `legalHold`) are defined as `optional` in Protobuf, they maintain natural forward and backward compatibility across rolling upgrades.
* **OM Version Leveling**: OM version leveling is introduced primarily because this feature adds new OM APIs (such as `setRetentionConfig` and `addLegalHold`). The new OM version allows clients to determine whether the target OM cluster supports executing these Object Lock APIs during rolling upgrades.

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

## Plan

The implementation of S3 Object Lock in Apache Ozone is divided into five structured phases covering the 11 core functional requirements. This ensures incremental delivery, clean service boundaries, and testability at each stage.

### Phase 1: Metadata Infrastructure & Schema Definitions (Item 1)

*Goal: Establish protobuf schemas, in-memory domain models, and RocksDB table serialization codecs.*

1. **Protobuf Definitions (`OmClientProtocol.proto`)**:
   - Introduce retention domain messages:
     - `RetentionMode` enum (`GOVERNANCE = 1`, `COMPLIANCE = 2`).
     - `Rule` message (`retentionMode`, `timeUnit`, `duration`).
     - `EventHold` message (`enabled`, `rule`).
     - `RetentionConfig` message (`rule`, `eventHold`).
   - Extend `BucketInfo` (OBS buckets only):
     - `optional bool objectLockEnabled = 24 [default = false];`
     - `optional RetentionConfig defaultRetention = 25;`
   - Extend `KeyInfo` & `KeyInfoProtoLight`:
     - `optional RetentionConfig retentionConfig = 23;`
     - `optional bool legalHold = 24 [default = false];`
   - Introduce new class `RetentionExpiration` to encapsulate rules of retention:
     - `fromProto` to convert from protobuf to java class.
     - `toProto` to convert from java class to protobuf.
     - `validate` to validate the retention configuration (e.g., duration > 0, valid time unit, valid retention mode).

2. **Domain Models & Helpers (`hadoop-ozone/common`)**:
   - Update `OmBucketInfo` / `OmBucketInfo.Builder` with getters, setters, and protobuf translation.
   - Update `OmKeyInfo` / `OmKeyInfo.Builder` with getters, setters, and protobuf translation.
   - Validate bucket layout: enforce that Object Lock can only be enabled on `OBJECT_STORE` (OBS) layout buckets; reject `FILE_SYSTEM_OPTIMIZED` (FSO) or `LEGACY` buckets.

---

### Phase 2: Ranger Integration & Action Mapping (Item 2)

*Goal: Enable fine-grained access control separating standard data mutating permissions from compliance actions.*

1. **Ranger Action Mapping & UI Validation (`ozone.json`)**:
  - Do **NOT** create new underlying Ranger access types. Retain primitive access types (`READ`, `WRITE`, `DELETE`) and leverage the STS action-matching framework (`action-matches`).
  - Register the 7 S3 Object Lock actions in `security-admin/src/main/webapp/react-webapp/src/utils/actionRequirements/ozone.json`:
    - `GetBucketObjectLockConfiguration` (Bucket level: requires `READ`)
    - `PutBucketObjectLockConfiguration` (Bucket level: requires `WRITE`)
    - `GetObjectRetention` (Key level: requires `READ`)
    - `PutObjectRetention` (Key level: requires `WRITE`)
    - `GetObjectLegalHold` (Key level: requires `READ`)
    - `PutObjectLegalHold` (Key level: requires `WRITE`)
    - `BypassGovernanceRetention` (Key level: requires `WRITE` / `DELETE`)
  - Enable design-time validation in Ranger React UI to ensure administrators cannot grant compliance actions without selecting their required primitive permissions.

2. **Payload-Aware String Matcher Condition Evaluator**:
  - Register a dedicated custom condition evaluator (e.g., `RangerOzoneValueMatcher`, extending `RangerAbstractConditionEvaluator`) in `ranger-servicedef-ozone.json` under `policyConditions`.
  - Update `RangerOzoneAuthorizer` and request context handling to extract sensitive payload attributes and inject them into `RangerAccessRequest.getContext()`:
    - `s3:object-lock-legal-hold`: Extracted from the request body (`ON` vs. `OFF`).
    - `s3:object-lock-mode`: Extracted from the request body (`GOVERNANCE` vs. `COMPLIANCE`).
    - `s3:x-amz-bypass-governance-retention`: Extracted from request headers (`true` vs. `false`).
  - Implement runtime string matching within the evaluator to enforce value-level role segregation:
    - Allow decoupling privileged operations (e.g., granting `PutObjectLegalHold` exclusively when `s3:object-lock-legal-hold == "ON"`, while restricting `OFF` to compliance officers).
    - Prevent accidental or unauthorized lock lock-in by enforcing `s3:object-lock-mode == "GOVERNANCE"` for standard operators, denying `COMPLIANCE` configuration.

3. **S3 Gateway & IAM Action Resolution**:
   - Update `S3GAction` in `hadoop-ozone/s3gateway`: add audit actions for all lock configuration operations.
   - Update `S3GActionIamMapper` in `hadoop-ozone/s3gateway`: map the new audit actions to standard IAM S3 actions (`GetBucketObjectLockConfiguration`, `PutBucketObjectLockConfiguration`, `GetObjectRetention`, `PutObjectRetention`, `GetObjectLegalHold`, `PutObjectLegalHold`, `BypassGovernanceRetention`).
   - Update `IamSessionPolicyResolver.S3Action` in `hadoop-ozone/common`: register the actions with their resource scopes (Bucket vs. Object) and base permissions (`READ`, `WRITE`).
   - Propagate `s3Action` via `S3Auth` through OM RPC to `RequestContext.s3Action` for evaluation by `RangerOzoneAuthorizer`.

---

### Phase 3: Object Lock Management APIs (Items 3 – 8)

*Goal: Implement S3 REST endpoints, OM client RPC protocols, and OM HA consensus handlers for configuring and querying locks.*

#### 1. Bucket Object Lock Configuration (Items 3 & 6)
- **`PutBucketObjectLockConfiguration` (Item 3)**:
  - *S3G*: Route `PUT /{bucket}?object-lock`. Parse `ObjectLockConfiguration` XML (`ObjectLockEnabled`, optional `DefaultRetention`).
  - *RPC*: Add `SetBucketObjectLockConfigRequest` / `Response` in `OmClientProtocol.proto`.
  - *OM Handler (`OMBucketSetObjectLockConfigRequest`)*:
    - `preExecute`: Validate Ranger action `PutBucketObjectLockConfiguration`. Fail-fast on invalid parameters.
    - `validateAndUpdateCache`: Under bucket write lock, check bucket exists and layout is OBS. Ensure immutability: if `objectLockEnabled` is already `true`, it cannot be toggled to `false`. Set `objectLockEnabled` and update `defaultRetention`. Commit to `BucketTable`.
- **`GetBucketObjectLockConfiguration` (Item 6)**:
  - *S3G*: Route `GET /{bucket}?object-lock`. Return `ObjectLockConfiguration` XML.
  - *OM / Metadata Reader*: Authorize `GetBucketObjectLockConfiguration`. Read from `BucketTable`. Return `404 ObjectLockConfigurationNotFoundError` if Object Lock is not enabled.

#### 2. Object Retention Management (Items 4 & 7)
- **`PutObjectRetention` (Item 4)**:
  - *S3G*: Route `PUT /{bucket}/{key}?retention`. Parse `Retention` XML (`Mode`, `RetainUntilDate`). Parse header `x-amz-bypass-governance-retention`.
  - *RPC*: Add `SetObjectRetentionRequest` / `Response` in `OmClientProtocol.proto`.
  - *OM Handler (`OMKeySetRetentionRequest`)*:
    - `preExecute`: Authorize `PutObjectRetention`. If bypass header is set, authorize `BypassGovernanceRetention`.
    - `validateAndUpdateCache`: Under key write lock, verify bucket has `objectLockEnabled == true`.
      - **Compliance Mode**: If existing key is in COMPLIANCE, the new `RetainUntilDate` MUST be >= existing `RetainUntilDate` (cannot shorten duration or switch mode).
      - **Governance Mode**: If shortening retention or removing, caller must have `BypassGovernanceRetention` permission and send the bypass flag.
      - Update `OmKeyInfo.retentionConfig` in `KeyTable`.
- **`GetObjectRetention` (Item 7)**:
  - *S3G*: Route `GET /{bucket}/{key}?retention`. Return `Retention` XML.
  - *OM / Metadata Reader*: Authorize `GetObjectRetention`. Read from `KeyTable`. Return `404 NoSuchObjectLockConfiguration` if no retention policy is applied.

#### 3. Object Legal Hold Management (Items 5 & 8)
- **`PutObjectLegalHold` (Item 5)**:
  - *S3G*: Route `PUT /{bucket}/{key}?legal-hold`. Parse `LegalHold` XML (`Status`: `ON` / `OFF`).
  - *RPC*: Add `SetObjectLegalHoldRequest` / `Response` in `OmClientProtocol.proto`.
  - *OM Handler (`OMKeySetLegalHoldRequest`)*:
    - `preExecute`: Authorize `PutObjectLegalHold`.
    - `validateAndUpdateCache`: Under key write lock, verify bucket has `objectLockEnabled == true`. Update `OmKeyInfo.legalHold` (`true` for `ON`, `false` for `OFF`) in `KeyTable`.
- **`GetObjectLegalHold` (Item 8)**:
  - *S3G*: Route `GET /{bucket}/{key}?legal-hold`. Return `LegalHold` XML (`Status`: `ON` / `OFF`).
  - *OM / Metadata Reader*: Authorize `GetObjectLegalHold`. Read from `KeyTable`. Return `404 NoSuchObjectLockConfiguration` if unconfigured.

---

### Phase 4: WORM Enforcement on Data Operations (Items 9 – 11)

*Goal: Enforce dual-gate WORM protection on all data-mutating operations to guarantee immutability on single-version OBS objects.*

#### 1. Check Lock on Put / Copy Operations (Item 9)
- **Bucket Default Retention Inheritance**:
  - When creating a *new* key in an Object-Lock-enabled bucket:
    - If bucket has `defaultRetention`, automatically attach `RetentionConfig` to the newly committed `OmKeyInfo`.
    - If request provides explicit retention/legal hold headers (`x-amz-object-lock-*`), validate permissions and apply them on creation.
- **Overwrite Protection (Two-Phase Validation)**:
  - **`OMKeyCreateRequest.preExecute`** (Fail-fast):
    - Check if key already exists in `KeyTable`.
    - If exists and locked (`legalHold == true` OR unexpired `retentionConfig` without valid Governance bypass): immediately fail with `WORMProtectionException` (maps to S3 `403 AccessDenied`). Avoids unnecessary Raft consensus.
  - **`OMKeyCommitRequest.validateAndUpdateCache`** (Linearizability):
    - Under bucket/key write lock, perform authoritative WORM check against committed `KeyTable`.
    - Reject commit if target is actively locked. Keys that fail to commit remain in `OpenKeyTable` and are reclaimed by background open-key cleanup.

#### 2. Check Lock on Delete Operations (Item 10)
- **Single Delete (`DeleteObject`)**:
  - In `OMKeyDeleteRequest.validateAndUpdateCache` under key write lock:
    - Lookup target key in `KeyTable`. If absent, return standard idempotent success (`204 No Content`).
    - If key exists:
      - Active Legal Hold (`legalHold == true`) -> Reject with `WORMProtectionException`.
      - Active Retention:
        - `COMPLIANCE` -> Reject unconditionally.
        - `GOVERNANCE` -> Allow only if caller has `BypassGovernanceRetention` permission AND header `x-amz-bypass-governance-retention: true` is present; otherwise reject.
    - If unlocked or bypass authorized, proceed with deletion and quota reclaim.
- **Batch Delete (`DeleteObjects`)**:
  - In `S3BatchDeleteRequest` / `MultiDeleteEndpoint`: evaluate WORM status per key.
  - Locked keys return `<Error><Code>AccessDenied</Code><Message>Access Denied: Object is WORM protected</Message></Error>` in the multi-delete XML payload while allowing unlocked keys in the batch to proceed.

#### 3. Check Lock on Multipart Upload Operations (Item 11)
- **`InitiateMultipartUpload` (`S3InitiateMultipartUploadRequest`)**:
  - `preExecute`: Perform fail-fast WORM check on destination key. If target exists and is locked, reject before creating MPU state in `multipartInfoTable`.
  - Carry forward default/specified retention parameters into MPU metadata.
- **`UploadPart` / `UploadPartCopy` (`S3MultipartUploadCommitPartRequest`)**:
  - `preExecute`: Fail-fast check on destination key to detect concurrent locks applied after initiation.
- **`CompleteMultipartUpload` (`S3MultipartUploadCompleteRequest`)**:
  - `validateAndUpdateCache`: Under bucket write lock, perform linearizable WORM check on destination key before committing the final assembled key.
  - Apply inherited or initiated retention/legal hold metadata to the completed `OmKeyInfo`.
- **`AbortMultipartUpload`**:
  - Only aborts open parts in `multipartInfoTable` and `openKeyTable`; does not affect existing locked committed objects.

---

### Phase 5: Verification, Testing & Tooling

*Goal: Ensure end-to-end test coverage across unit, integration, and security acceptance suites.*

**Acceptance & Ranger Smoke Tests (Docker Compose)**:
- Add automated test suites under `hadoop-ozone/dist/src/main/smoketest/security/s3-object-lock.robot`.
- Run in `ozonesecure-ha` environment (`test-ranger.sh`):
  - Test dual-gate access control: verify users with `WRITE`/`DELETE` permissions are blocked by active WORM status.
  - Test compliance officer role with `PutObjectLegalHold` and `BypassGovernanceRetention`.
  - Test compliance immutability against admin accounts.

Once S3 Versioning support matures, future efforts will focus on ensuring compatibility with S3 multi-version object locking.

### Phase 6: Future Considerations — S3 Versioning Support

*Goal: Seamlessly transition to version-level Object Lock once Ozone supports S3 Versioning.*

1. **Version State Transition & Lock Binding**:
  - When Versioning is enabled, existing objects will immediately be treated as the **latest version**.
  - Any pre-existing Retention and Legal Hold states on an object will bind directly to that specific version.

2. **Version-Level WORM Enforcement**:
  - **Version-Scoped Operations**: All operations will target a specific version; an action is blocked **only if that specific version is locked**.
  - **Overwrites as New Versions**: `PUT` or `MPU` operations will no longer overwrite existing data in place. Instead, they will create a new version (which can have its own lock configuration), while the previous version and its lock status remain intact.

## References

- [AWS S3 Object Lock User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [AWS S3 PutBucketObjectLockConfiguration API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketObjectLockConfiguration.html)
- [AWS S3 PutObjectRetention API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html)
- [AWS S3 PutObjectLegalHold API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html)
- [HDDS-15945: S3 Object Lock Support in Apache Ozone](https://issues.apache.org/jira/browse/HDDS-15945)
- [Ozone STS Design](ozone-sts.md)

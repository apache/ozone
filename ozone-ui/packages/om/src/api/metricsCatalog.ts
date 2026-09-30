/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Reference catalog for the `OMMetrics` JMX bean.
 *
 * The single source of truth mapping OM's exact JMX metric names to the operations
 * the Metrics page shows. Keeping it as data (rather than inferring identity from a
 * regex) means every row is cross-checked against the field declarations in
 * `OMMetrics.java`, and metrics that are counts/gauges rather than RPC requests
 * (e.g. `NumOpenKeysCleaned`, `NumSnapshotActive`, the aggregate `Num<Type>Ops`) are
 * simply absent here and fall through to the "Other" category instead of being
 * mis-reported as requests.
 */

/**
 * Ordered operation categories that seed the metric-type dropdown; a type with no
 * activity is disabled there. These are the `<Type>` first-words OM uses in its
 * `Num<Type><Op>` names. Any category discovered in the bean but missing here is
 * still surfaced (appended after these); {@link OTHER_TYPE} is deliberately excluded
 * so it sorts last.
 */
export const METRIC_TYPES = [
  'Get',
  'Abort',
  'Add',
  'Block',
  'Bucket',
  'Cancel',
  'Commit',
  'Complete',
  'Create',
  'Delete',
  'Expired',
  'Follower',
  'Initiate',
  'Key',
  'Leader',
  'Linearizable',
  'List',
  'Lookup',
  'Open',
  'Put',
  'Recover',
  'Remove',
  'Set',
  'Snapshot',
  'Tenant',
  'Trash',
  'Volume',
] as const;

export type MetricType = (typeof METRIC_TYPES)[number];

/** Catch-all category for numeric metrics not described by {@link OM_OPERATIONS}. */
export const OTHER_TYPE = 'Other';

/** Object-count / data-size metrics rendered as the summary cards (not operations). */
export const SUMMARY_KEYS = {
  volumes: 'NumVolumes',
  buckets: 'NumBuckets',
  keys: 'NumKeys',
  totalCommittedBytes: 'TotalDataCommitted',
} as const;

/**
 * One OM operation: its dropdown `type`, human-readable `name` (the "Operational
 * Action" column), and the exact JMX counter names for its request and (optional)
 * failure. `requestKey` is omitted for a failure-only aggregate (e.g. Trash).
 */
export interface OmOperationDef {
  type: string;
  name: string;
  requestKey?: string;
  failureKey?: string;
}

/**
 * Every RPC operation `OMMetrics` exposes, keyed by exact JMX name. `type` matches
 * OM's `Num<Type>...` first-word so the dropdown grouping is unchanged; `name` is a
 * readable label decoupled from the raw metric. Cross-validated against the field
 * declarations in `OMMetrics.java`.
 *
 * Notable exact-name cases: the `GetKeyInfo` failure field is misnamed in OM so it is
 * exposed as `GetNumGetKeyInfoFails` (no `Num` prefix); the CheckAccess request is the
 * `-es` plural `NumVolumeCheckAccesses`; `NumTrashFails` is OM's single aggregate
 * trash-processing failure counter with no request counterpart.
 */
export const OM_OPERATIONS: OmOperationDef[] = [
  // Volume
  {
    type: 'Volume',
    name: 'Create',
    requestKey: 'NumVolumeCreates',
    failureKey: 'NumVolumeCreateFails',
  },
  {
    type: 'Volume',
    name: 'Update',
    requestKey: 'NumVolumeUpdates',
    failureKey: 'NumVolumeUpdateFails',
  },
  { type: 'Volume', name: 'Info', requestKey: 'NumVolumeInfos', failureKey: 'NumVolumeInfoFails' },
  {
    type: 'Volume',
    name: 'Delete',
    requestKey: 'NumVolumeDeletes',
    failureKey: 'NumVolumeDeleteFails',
  },
  {
    type: 'Volume',
    name: 'Check Access',
    requestKey: 'NumVolumeCheckAccesses',
    failureKey: 'NumVolumeCheckAccessFails',
  },
  { type: 'Volume', name: 'List', requestKey: 'NumVolumeLists', failureKey: 'NumVolumeListFails' },

  // Bucket
  {
    type: 'Bucket',
    name: 'Create',
    requestKey: 'NumBucketCreates',
    failureKey: 'NumBucketCreateFails',
  },
  { type: 'Bucket', name: 'Info', requestKey: 'NumBucketInfos', failureKey: 'NumBucketInfoFails' },
  {
    type: 'Bucket',
    name: 'Update',
    requestKey: 'NumBucketUpdates',
    failureKey: 'NumBucketUpdateFails',
  },
  {
    type: 'Bucket',
    name: 'Delete',
    requestKey: 'NumBucketDeletes',
    failureKey: 'NumBucketDeleteFails',
  },
  { type: 'Bucket', name: 'List', requestKey: 'NumBucketLists', failureKey: 'NumBucketListFails' },
  {
    type: 'Bucket',
    name: 'S3 Create',
    requestKey: 'NumBucketS3Creates',
    failureKey: 'NumBucketS3CreateFails',
  },
  {
    type: 'Bucket',
    name: 'S3 Delete',
    requestKey: 'NumBucketS3Deletes',
    failureKey: 'NumBucketS3DeleteFails',
  },
  {
    type: 'Bucket',
    name: 'S3 List',
    requestKey: 'NumBucketS3Lists',
    failureKey: 'NumBucketS3ListFails',
  },

  // Key
  {
    type: 'Key',
    name: 'Allocate',
    requestKey: 'NumKeyAllocate',
    failureKey: 'NumKeyAllocateFails',
  },
  { type: 'Key', name: 'Lookup', requestKey: 'NumKeyLookup', failureKey: 'NumKeyLookupFails' },
  { type: 'Key', name: 'Rename', requestKey: 'NumKeyRenames', failureKey: 'NumKeyRenameFails' },
  { type: 'Key', name: 'Delete', requestKey: 'NumKeyDeletes', failureKey: 'NumKeyDeleteFails' },
  { type: 'Key', name: 'List', requestKey: 'NumKeyLists', failureKey: 'NumKeyListFails' },
  { type: 'Key', name: 'Commit', requestKey: 'NumKeyCommits', failureKey: 'NumKeyCommitFails' },
  { type: 'Key', name: 'HSync', requestKey: 'NumKeyHSyncs' },

  // Block
  {
    type: 'Block',
    name: 'Allocation',
    requestKey: 'NumBlockAllocations',
    failureKey: 'NumBlockAllocationFails',
  },

  // Get (grab-bag of read ops OM names Num*Get* / with Get first-word)
  {
    type: 'Get',
    name: 'Service List',
    requestKey: 'NumGetServiceLists',
    failureKey: 'NumGetServiceListFails',
  },
  {
    type: 'Get',
    name: 'Key Info',
    requestKey: 'NumGetKeyInfo',
    failureKey: 'GetNumGetKeyInfoFails',
  },
  { type: 'Get', name: 'ACL', requestKey: 'NumGetAcl' },
  {
    type: 'Get',
    name: 'File Status',
    requestKey: 'NumGetFileStatus',
    failureKey: 'NumGetFileStatusFails',
  },
  {
    type: 'Get',
    name: 'Object Tagging',
    requestKey: 'NumGetObjectTagging',
    failureKey: 'NumGetObjectTaggingFails',
  },

  // Set
  { type: 'Set', name: 'ACL', requestKey: 'NumSetAcl' },
  { type: 'Set', name: 'Time', requestKey: 'NumSetTime' },

  // Add / Remove ACL
  { type: 'Add', name: 'ACL', requestKey: 'NumAddAcl' },
  { type: 'Remove', name: 'ACL', requestKey: 'NumRemoveAcl' },

  // Put / Delete object tagging
  {
    type: 'Put',
    name: 'Object Tagging',
    requestKey: 'NumPutObjectTagging',
    failureKey: 'NumPutObjectTaggingFails',
  },
  {
    type: 'Delete',
    name: 'Object Tagging',
    requestKey: 'NumDeleteObjectTagging',
    failureKey: 'NumDeleteObjectTaggingFails',
  },

  // Multipart upload
  {
    type: 'Initiate',
    name: 'Multipart Upload',
    requestKey: 'NumInitiateMultipartUploads',
    failureKey: 'NumInitiateMultipartUploadFails',
  },
  {
    type: 'Complete',
    name: 'Multipart Upload',
    requestKey: 'NumCompleteMultipartUploads',
    failureKey: 'NumCompleteMultipartUploadFails',
  },
  {
    type: 'Commit',
    name: 'Multipart Upload Part',
    requestKey: 'NumCommitMultipartUploadParts',
    failureKey: 'NumCommitMultipartUploadPartFails',
  },
  {
    type: 'Abort',
    name: 'Multipart Upload',
    requestKey: 'NumAbortMultipartUploads',
    failureKey: 'NumAbortMultipartUploadFails',
  },
  {
    type: 'List',
    name: 'Multipart Uploads',
    requestKey: 'NumListMultipartUploads',
    failureKey: 'NumListMultipartUploadFails',
  },
  {
    type: 'List',
    name: 'Multipart Upload Parts',
    requestKey: 'NumListMultipartUploadParts',
    failureKey: 'NumListMultipartUploadPartFails',
  },

  // File-system object (FSO) ops
  {
    type: 'Create',
    name: 'Directory',
    requestKey: 'NumCreateDirectory',
    failureKey: 'NumCreateDirectoryFails',
  },
  { type: 'Create', name: 'File', requestKey: 'NumCreateFile', failureKey: 'NumCreateFileFails' },
  { type: 'Lookup', name: 'File', requestKey: 'NumLookupFile', failureKey: 'NumLookupFileFails' },
  { type: 'List', name: 'Status', requestKey: 'NumListStatus', failureKey: 'NumListStatusFails' },
  {
    type: 'List',
    name: 'Open Files',
    requestKey: 'NumListOpenFiles',
    failureKey: 'NumListOpenFilesFails',
  },

  // Snapshot request ops (state counts NumSnapshotActive/Deleted/CacheSize → Other)
  {
    type: 'Snapshot',
    name: 'Create',
    requestKey: 'NumSnapshotCreates',
    failureKey: 'NumSnapshotCreateFails',
  },
  {
    type: 'Snapshot',
    name: 'Delete',
    requestKey: 'NumSnapshotDeletes',
    failureKey: 'NumSnapshotDeleteFails',
  },
  {
    type: 'Snapshot',
    name: 'List',
    requestKey: 'NumSnapshotLists',
    failureKey: 'NumSnapshotListFails',
  },
  {
    type: 'Snapshot',
    name: 'Rename',
    requestKey: 'NumSnapshotRenames',
    failureKey: 'NumSnapshotRenameFails',
  },
  {
    type: 'Snapshot',
    name: 'Diff Job',
    requestKey: 'NumSnapshotDiffJobs',
    failureKey: 'NumSnapshotDiffJobFails',
  },
  {
    type: 'Snapshot',
    name: 'Info',
    requestKey: 'NumSnapshotInfos',
    failureKey: 'NumSnapshotInfoFails',
  },
  {
    type: 'Cancel',
    name: 'Snapshot Diff',
    requestKey: 'NumCancelSnapshotDiffs',
    failureKey: 'NumCancelSnapshotDiffFails',
  },
  {
    type: 'List',
    name: 'Snapshot Diff Jobs',
    requestKey: 'NumListSnapshotDiffJobs',
    failureKey: 'NumListSnapshotDiffJobFails',
  },

  // Tenant
  {
    type: 'Tenant',
    name: 'Create',
    requestKey: 'NumTenantCreates',
    failureKey: 'NumTenantCreateFails',
  },
  {
    type: 'Tenant',
    name: 'Delete',
    requestKey: 'NumTenantDeletes',
    failureKey: 'NumTenantDeleteFails',
  },
  {
    type: 'Tenant',
    name: 'Assign User',
    requestKey: 'NumTenantAssignUsers',
    failureKey: 'NumTenantAssignUserFails',
  },
  {
    type: 'Tenant',
    name: 'Revoke User',
    requestKey: 'NumTenantRevokeUsers',
    failureKey: 'NumTenantRevokeUserFails',
  },
  {
    type: 'Tenant',
    name: 'Assign Admin',
    requestKey: 'NumTenantAssignAdmins',
    failureKey: 'NumTenantAssignAdminFails',
  },
  {
    type: 'Tenant',
    name: 'Revoke Admin',
    requestKey: 'NumTenantRevokeAdmins',
    failureKey: 'NumTenantRevokeAdminFails',
  },
  { type: 'Tenant', name: 'List', requestKey: 'NumTenantLists' },
  { type: 'Tenant', name: 'Get User Info', requestKey: 'NumTenantGetUserInfos' },
  { type: 'Tenant', name: 'User List', requestKey: 'NumTenantTenantUserLists' },

  // Open-key / expired-MPU request ops (the *Cleaned/*Aborted counters → Other)
  {
    type: 'Open',
    name: 'Key Delete Request',
    requestKey: 'NumOpenKeyDeleteRequests',
    failureKey: 'NumOpenKeyDeleteRequestFails',
  },
  {
    type: 'Expired',
    name: 'MPU Abort Request',
    requestKey: 'NumExpiredMPUAbortRequests',
    failureKey: 'NumExpiredMPUAbortRequestFails',
  },

  // Recover lease
  {
    type: 'Recover',
    name: 'Lease',
    requestKey: 'NumRecoverLease',
    failureKey: 'NumRecoverLeaseFails',
  },

  // Ratis read path
  { type: 'Linearizable', name: 'Read', requestKey: 'NumLinearizableRead' },
  { type: 'Leader', name: 'Skip Linearizable Read', requestKey: 'NumLeaderSkipLinearizableRead' },
  {
    type: 'Follower',
    name: 'Local Lease Read (Success)',
    requestKey: 'NumFollowerReadLocalLeaseSuccess',
  },
  {
    type: 'Follower',
    name: 'Local Lease Read (Fail: Log)',
    requestKey: 'NumFollowerReadLocalLeaseFailLog',
  },
  {
    type: 'Follower',
    name: 'Local Lease Read (Fail: Time)',
    requestKey: 'NumFollowerReadLocalLeaseFailTime',
  },

  // Trash — request counters plus OM's single aggregate failure counter (which has
  // no request counterpart). Other NumTrash* counters remain background counts → Other.
  { type: 'Trash', name: 'Rename', requestKey: 'NumTrashRenames' },
  { type: 'Trash', name: 'Delete', requestKey: 'NumTrashDeletes' },
  { type: 'Trash', name: 'Processing', failureKey: 'NumTrashFails' },
];

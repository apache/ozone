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

/** JMX query for the OM metrics bean (RPC operation counters + object counts). */
export const OM_METRICS_QUERY = 'Hadoop:service=OzoneManager,name=OMMetrics';

/**
 * Operation categories exposed by `OMMetrics` as `Num<Type><Op>` counters. Order
 * seeds the metric-type dropdown; a type with no activity is disabled there. Any
 * category found in the bean but missing here is still surfaced (appended), so a
 * newly added OM metric type is never silently dropped.
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

/** Raw OM metrics JMX bean — dynamic `Num*`/count keys plus string tags. */
export type OMMetricsBean = Record<string, number | string>;

export type OperationStatus = 'Active' | 'Warning' | 'Inactive';

export interface MetricOperation {
  key: string;
  /** Canonical operation label (e.g. `Allocate`, `Commit`, `Delete`). */
  name: string;
  /** Successful request count for this operation. */
  requests: number;
  /** Failure count for this operation. */
  failures: number;
  status: OperationStatus;
}

export interface MetricTypeData {
  type: string;
  /** Total requests: the sum of the requests of the operations shown for this type. */
  totalRequests: number;
  /** Operations with any activity (requests or failures), busiest first. */
  operations: MetricOperation[];
  /** False when the type has no activity at all — its dropdown option is greyed out. */
  enabled: boolean;
}

export interface MetricsSummary {
  volumes: number;
  buckets: number;
  keys: number;
  totalCommittedBytes: number;
}

export interface ParsedOmMetrics {
  summary: MetricsSummary;
  byType: Record<string, MetricTypeData>;
}

/**
 * Request metric key: `Num<Type><Op>` — two CamelCase segments after `Num`, e.g.
 * `NumKeyCommits` → type `Key`, op `Commits`. Single-word counters such as
 * `NumKeys`/`NumVolumes` (no `<Op>` segment) intentionally do not match, so object
 * counts are never treated as operations.
 */
const REQUEST_KEY_RE = /^Num([A-Z][a-z]+)([A-Z].+)$/;

/**
 * Counters that are NOT per-operation request counts and must be excluded from the
 * operation rows (and therefore from the request total). These are the cross-category
 * aggregate `Num<Type>Ops` totals (e.g. `NumKeyOps` is incremented by GetKeyInfo, all
 * FSO ops and all multipart ops as well as plain key ops) and the snapshot
 * state/gauge counts. Cross-validated against `OMMetrics.java`.
 */
const NON_OP_KEYS = new Set<string>([
  'NumVolumeOps',
  'NumBucketOps',
  'NumKeyOps',
  'NumFSOps',
  'NumTenantOps',
  'NumSnapshotActive',
  'NumSnapshotDeleted',
  'NumSnapshotCacheSize',
]);

/**
 * Every `*Fails` failure counter OM exposes, mapped to the exact request counter it
 * belongs to, so a failure is recognised by name and joined to its request row
 * explicitly — no plural-stripping guesswork. Cross-validated against the field
 * declarations in `OMMetrics.java`; note the two irregular names: the request for
 * `NumVolumeCheckAccessFails` is `NumVolumeCheckAccesses` (an -es plural that a
 * trailing-`s` heuristic gets wrong), and the `GetKeyInfo` failure field is
 * misnamed in OM so it is exposed as `GetNumGetKeyInfoFails` (no `Num` prefix).
 */
const FAILURE_TO_REQUEST: Record<string, string> = {
  NumVolumeCreateFails: 'NumVolumeCreates',
  NumVolumeUpdateFails: 'NumVolumeUpdates',
  NumVolumeInfoFails: 'NumVolumeInfos',
  NumVolumeDeleteFails: 'NumVolumeDeletes',
  NumVolumeCheckAccessFails: 'NumVolumeCheckAccesses',
  NumVolumeListFails: 'NumVolumeLists',
  NumBucketCreateFails: 'NumBucketCreates',
  NumBucketInfoFails: 'NumBucketInfos',
  NumBucketUpdateFails: 'NumBucketUpdates',
  NumBucketDeleteFails: 'NumBucketDeletes',
  NumBucketListFails: 'NumBucketLists',
  NumBucketS3CreateFails: 'NumBucketS3Creates',
  NumBucketS3DeleteFails: 'NumBucketS3Deletes',
  NumBucketS3ListFails: 'NumBucketS3Lists',
  NumKeyAllocateFails: 'NumKeyAllocate',
  NumKeyLookupFails: 'NumKeyLookup',
  NumKeyRenameFails: 'NumKeyRenames',
  NumKeyDeleteFails: 'NumKeyDeletes',
  NumKeyListFails: 'NumKeyLists',
  NumKeyCommitFails: 'NumKeyCommits',
  NumBlockAllocationFails: 'NumBlockAllocations',
  NumGetServiceListFails: 'NumGetServiceLists',
  NumInitiateMultipartUploadFails: 'NumInitiateMultipartUploads',
  NumCommitMultipartUploadPartFails: 'NumCommitMultipartUploadParts',
  NumCompleteMultipartUploadFails: 'NumCompleteMultipartUploads',
  NumAbortMultipartUploadFails: 'NumAbortMultipartUploads',
  NumListMultipartUploadPartFails: 'NumListMultipartUploadParts',
  NumListMultipartUploadFails: 'NumListMultipartUploads',
  NumOpenKeyDeleteRequestFails: 'NumOpenKeyDeleteRequests',
  NumExpiredMPUAbortRequestFails: 'NumExpiredMPUAbortRequests',
  NumSnapshotCreateFails: 'NumSnapshotCreates',
  NumSnapshotRenameFails: 'NumSnapshotRenames',
  NumSnapshotDeleteFails: 'NumSnapshotDeletes',
  NumSnapshotListFails: 'NumSnapshotLists',
  NumSnapshotDiffJobFails: 'NumSnapshotDiffJobs',
  NumSnapshotInfoFails: 'NumSnapshotInfos',
  NumCancelSnapshotDiffFails: 'NumCancelSnapshotDiffs',
  NumListSnapshotDiffJobFails: 'NumListSnapshotDiffJobs',
  NumTenantCreateFails: 'NumTenantCreates',
  NumTenantDeleteFails: 'NumTenantDeletes',
  NumTenantAssignUserFails: 'NumTenantAssignUsers',
  NumTenantRevokeUserFails: 'NumTenantRevokeUsers',
  NumTenantAssignAdminFails: 'NumTenantAssignAdmins',
  NumTenantRevokeAdminFails: 'NumTenantRevokeAdmins',
  NumGetFileStatusFails: 'NumGetFileStatus',
  NumCreateDirectoryFails: 'NumCreateDirectory',
  NumCreateFileFails: 'NumCreateFile',
  NumLookupFileFails: 'NumLookupFile',
  NumListStatusFails: 'NumListStatus',
  NumListOpenFilesFails: 'NumListOpenFiles',
  GetNumGetKeyInfoFails: 'NumGetKeyInfo',
  NumGetObjectTaggingFails: 'NumGetObjectTagging',
  NumPutObjectTaggingFails: 'NumPutObjectTagging',
  NumDeleteObjectTaggingFails: 'NumDeleteObjectTagging',
  NumRecoverLeaseFails: 'NumRecoverLease',
};

/**
 * Failure counters that have no matching request counter — an aggregate failure for
 * a whole category. Rendered as a failure-only row (0 requests) under its type.
 * `NumTrashFails` is OM's single generic trash-processing failure counter.
 */
const ORPHAN_FAILURES: Record<string, { type: string; name: string }> = {
  NumTrashFails: { type: 'Trash', name: 'Trash' },
};

function statusFor(requests: number, failures: number): OperationStatus {
  if (failures > 0) {
    return 'Warning';
  }
  if (requests > 0) {
    return 'Active';
  }
  return 'Inactive';
}

/** Split a request key into its `{ type, name }` (e.g. `NumKeyCommits` → Key/Commits). */
function parseRequestKey(key: string): { type: string; name: string } | null {
  const match = key.match(REQUEST_KEY_RE);
  return match ? { type: match[1], name: match[2] } : null;
}

interface OpRow {
  type: string;
  name: string;
  requests: number;
  failures: number;
}

/**
 * Parse the `OMMetrics` bean into per-type operation data and the object-count
 * summary.
 *
 * Metric identity is resolved from OM's actual JMX names rather than guessed:
 * failures are recognised via {@link FAILURE_TO_REQUEST}/{@link ORPHAN_FAILURES}
 * and joined to their request row by exact name; cross-category aggregate totals and
 * snapshot state counts ({@link NON_OP_KEYS}) are excluded so they never inflate a
 * request total. Everything else matching `Num<Type><Op>` is a request operation,
 * grouped by its `<Type>`. A failure whose request counter is absent still surfaces
 * as a failure-only row, and an unknown `*Fails` counter is treated as a failure
 * (never a phantom request op), so future OM metrics degrade gracefully.
 */
export function parseOmMetrics(bean: OMMetricsBean | undefined): ParsedOmMetrics {
  const data = bean ?? {};

  const numeric = (key: string): number => {
    const value = data[key];
    return typeof value === 'number' ? value : 0;
  };

  const summary: MetricsSummary = {
    volumes: numeric('NumVolumes'),
    buckets: numeric('NumBuckets'),
    keys: numeric('NumKeys'),
    totalCommittedBytes: numeric('TotalDataCommitted'),
  };

  // Request op value keyed by its request JMX name, failure value keyed by the
  // request JMX name it belongs to, and failure-only rows with no request counter.
  const requestByKey = new Map<string, number>();
  const failureByRequestKey = new Map<string, number>();
  const orphanRows: OpRow[] = [];

  const addFailure = (requestKey: string, value: number) => {
    failureByRequestKey.set(requestKey, (failureByRequestKey.get(requestKey) ?? 0) + value);
  };

  for (const [key, value] of Object.entries(data)) {
    if (typeof value !== 'number' || NON_OP_KEYS.has(key)) {
      continue;
    }
    const mappedRequest = FAILURE_TO_REQUEST[key];
    if (mappedRequest) {
      addFailure(mappedRequest, value);
    } else if (ORPHAN_FAILURES[key]) {
      const { type, name } = ORPHAN_FAILURES[key];
      orphanRows.push({ type, name, requests: 0, failures: value });
    } else if (key.endsWith('Fails')) {
      // Unknown failure counter: surface it as a failure rather than misreading the
      // `Fails` suffix as an operation name. Best-effort type/name from the base.
      const base = `Num${key.replace(/^Num/, '').replace(/Fails$/, '')}`;
      const parsed = parseRequestKey(base);
      orphanRows.push({
        type: parsed?.type ?? 'Other',
        name: parsed?.name ?? key,
        requests: 0,
        failures: value,
      });
    } else if (REQUEST_KEY_RE.test(key)) {
      requestByKey.set(key, (requestByKey.get(key) ?? 0) + value);
    }
  }

  // Assemble operation rows: request rows (joined with their failure), leftover
  // failures whose request counter was absent, and orphan failure rows.
  const rows: OpRow[] = [];
  for (const [key, requests] of requestByKey) {
    const parsed = parseRequestKey(key);
    if (!parsed) {
      continue;
    }
    rows.push({
      type: parsed.type,
      name: parsed.name,
      requests,
      failures: failureByRequestKey.get(key) ?? 0,
    });
    failureByRequestKey.delete(key);
  }
  for (const [key, failures] of failureByRequestKey) {
    const parsed = parseRequestKey(key);
    if (parsed) {
      rows.push({ type: parsed.type, name: parsed.name, requests: 0, failures });
    }
  }
  rows.push(...orphanRows);

  const rowsByType = new Map<string, OpRow[]>();
  for (const row of rows) {
    const list = rowsByType.get(row.type);
    if (list) {
      list.push(row);
    } else {
      rowsByType.set(row.type, [row]);
    }
  }

  // Build every known type plus any type discovered in the bean, so a new OM metric
  // category is surfaced rather than dropped.
  const types = new Set<string>([...METRIC_TYPES, ...rowsByType.keys()]);
  const byType: Record<string, MetricTypeData> = {};
  for (const type of types) {
    const shown = (rowsByType.get(type) ?? [])
      .filter((op) => op.requests > 0 || op.failures > 0)
      .map((op) => ({
        key: op.name,
        name: op.name,
        requests: op.requests,
        failures: op.failures,
        status: statusFor(op.requests, op.failures),
      }))
      .sort((a, b) => b.requests - a.requests);

    byType[type] = {
      type,
      totalRequests: shown.reduce((sum, op) => sum + op.requests, 0),
      operations: shown,
      enabled: shown.length > 0,
    };
  }

  return { summary, byType };
}

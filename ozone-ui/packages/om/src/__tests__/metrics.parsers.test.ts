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

import { describe, expect, it } from 'vitest';
import { parseOmMetrics, type OMMetricsBean } from '../api/metrics';

// All keys use the exact JMX names OM exposes (see OMMetrics.java).
const bean: OMMetricsBean = {
  name: 'Hadoop:service=OzoneManager,name=OMMetrics',
  'tag.Hostname': 'node1',
  NumVolumes: 1,
  NumBuckets: 2,
  NumKeys: 485,
  TotalDataCommitted: 62259,
  // Key — cross-category aggregate plus per-op counters.
  NumKeyOps: 2895,
  NumKeyAllocate: 965,
  NumKeyAllocateFails: 0,
  NumKeyCommits: 965,
  NumKeyCommitFails: 0,
  NumKeyDeletes: 965,
  NumKeyDeleteFails: 5,
  NumKeyHSyncs: 0,
  NumKeyLists: 0,
  NumKeyListFails: 100,
  NumGetServiceLists: 990,
};

describe('parseOmMetrics — summary', () => {
  it('extracts the object-count summary', () => {
    const { summary } = parseOmMetrics(bean);
    expect(summary).toEqual({
      volumes: 1,
      buckets: 2,
      keys: 485,
      totalCommittedBytes: 62259,
    });
  });

  it('is null-safe', () => {
    expect(parseOmMetrics(undefined).summary.keys).toBe(0);
  });
});

describe('parseOmMetrics — Key operations', () => {
  const key = parseOmMetrics(bean).byType.Key;

  it('joins a request counter to its failure counter into one row', () => {
    const commit = key.operations.find((o) => o.name === 'Commits');
    const del = key.operations.find((o) => o.name === 'Deletes');
    expect(commit).toMatchObject({ requests: 965, failures: 0, status: 'Active' });
    // NumKeyDeletes has 5 NumKeyDeleteFails → Warning.
    expect(del).toMatchObject({ requests: 965, failures: 5, status: 'Warning' });
  });

  it('surfaces a request with 0 count but nonzero failures as Warning', () => {
    const list = key.operations.find((o) => o.name === 'Lists');
    expect(list).toMatchObject({ requests: 0, failures: 100, status: 'Warning' });
  });

  it('omits operations with no activity (0 requests, 0 failures)', () => {
    // NumKeyHSyncs = 0 with no failures → not shown.
    expect(key.operations.find((o) => o.name === 'HSyncs')).toBeUndefined();
  });

  it('never treats the aggregate NumKeyOps as an operation', () => {
    expect(key.operations.find((o) => o.name === 'Ops')).toBeUndefined();
  });

  it('sorts operations by requests descending', () => {
    const requests = key.operations.map((o) => o.requests);
    expect(requests).toEqual([...requests].sort((a, b) => b - a));
  });
});

describe('parseOmMetrics — total requests are the sum of shown operations (not NumKeyOps)', () => {
  it('excludes the cross-category NumKeyOps aggregate from the total', () => {
    const { byType } = parseOmMetrics({
      NumKeyOps: 9999, // superset counter (Get/FSO/MPU); must not be the total
      NumKeyCommits: 10,
      NumKeyDeletes: 5,
    });
    expect(byType.Key.totalRequests).toBe(15);
    const chartSum = byType.Key.operations
      .filter((o) => o.requests > 0)
      .reduce((sum, o) => sum + o.requests, 0);
    expect(byType.Key.totalRequests).toBe(chartSum);
  });
});

describe('parseOmMetrics — GetKeyInfo failure (misspelled JMX name)', () => {
  // OM exposes the GetKeyInfo failure counter as `GetNumGetKeyInfoFails` (its Java
  // field is misnamed), so it does not start with `Num`.
  const { byType } = parseOmMetrics({ NumGetKeyInfo: 1200, GetNumGetKeyInfoFails: 7 });

  it('joins GetNumGetKeyInfoFails onto the NumGetKeyInfo row', () => {
    const getInfo = byType.Get.operations.filter((o) => o.name === 'KeyInfo');
    expect(getInfo).toHaveLength(1);
    expect(getInfo[0]).toMatchObject({ requests: 1200, failures: 7, status: 'Warning' });
  });
});

describe('parseOmMetrics — CheckAccess (-es plural) pairs into one row', () => {
  const { byType } = parseOmMetrics({
    NumVolumeCheckAccesses: 30,
    NumVolumeCheckAccessFails: 4,
  });

  it('does not split NumVolumeCheckAccesses / NumVolumeCheckAccessFails into two rows', () => {
    expect(byType.Volume.operations).toHaveLength(1);
    expect(byType.Volume.operations[0]).toMatchObject({
      name: 'CheckAccesses',
      requests: 30,
      failures: 4,
      status: 'Warning',
    });
  });
});

describe('parseOmMetrics — NumTrashFails is an aggregate failure, not a request', () => {
  const { byType } = parseOmMetrics({
    NumTrashRenames: 12,
    NumTrashFails: 3,
  });

  it('shows NumTrashFails as a failure-only Warning row (not a request named "Fails")', () => {
    expect(byType.Trash.operations.find((o) => o.name === 'Fails')).toBeUndefined();
    const trashFail = byType.Trash.operations.find((o) => o.name === 'Trash');
    expect(trashFail).toMatchObject({ requests: 0, failures: 3, status: 'Warning' });
  });

  it('excludes the failure count from the request total', () => {
    expect(byType.Trash.totalRequests).toBe(12);
  });
});

describe('parseOmMetrics — snapshot state counts do not inflate requests', () => {
  // NumSnapshotActive/Deleted/CacheSize are state/gauge counts, not requests, and OM
  // exposes no NumSnapshotOps.
  const { byType } = parseOmMetrics({
    NumSnapshotCreates: 8,
    NumSnapshotCreateFails: 1,
    NumSnapshotActive: 5,
    NumSnapshotDeleted: 2,
    NumSnapshotCacheSize: 4,
  });

  it('shows only the request operation and ignores the state counts', () => {
    expect(byType.Snapshot.operations).toHaveLength(1);
    expect(byType.Snapshot.operations[0]).toMatchObject({
      name: 'Creates',
      requests: 8,
      failures: 1,
    });
  });

  it('keeps the total equal to the request count (8), not 8+5+2+4', () => {
    expect(byType.Snapshot.totalRequests).toBe(8);
  });
});

describe('parseOmMetrics — Ratis read-path metrics are surfaced', () => {
  const { byType } = parseOmMetrics({
    NumLinearizableRead: 5000,
    NumLeaderSkipLinearizableRead: 120,
    NumFollowerReadLocalLeaseSuccess: 800,
  });

  it('includes Linearizable / Leader / Follower categories', () => {
    expect(byType.Linearizable).toMatchObject({ enabled: true });
    expect(byType.Linearizable.operations[0]).toMatchObject({ name: 'Read', requests: 5000 });
    expect(byType.Leader.operations[0]).toMatchObject({
      name: 'SkipLinearizableRead',
      requests: 120,
    });
    expect(byType.Follower.operations[0]).toMatchObject({
      name: 'ReadLocalLeaseSuccess',
      requests: 800,
    });
  });
});

describe('parseOmMetrics — enabled flag', () => {
  const { byType } = parseOmMetrics(bean);

  it('enables types with activity', () => {
    expect(byType.Key.enabled).toBe(true);
    expect(byType.Get.enabled).toBe(true);
  });

  it('disables types with no metrics in the bean', () => {
    const empty = parseOmMetrics({ NumKeys: 485 }).byType;
    expect(empty.Snapshot.enabled).toBe(false);
    expect(empty.Snapshot.operations).toHaveLength(0);
  });
});

describe('parseOmMetrics — parsing safety', () => {
  it('does not treat NumKeys/NumVolumes/NumBuckets counts as operations', () => {
    const { byType } = parseOmMetrics({ NumKeys: 485, NumVolumes: 1, NumBuckets: 2 });
    expect(byType.Key.operations).toHaveLength(0);
    expect(byType.Volume.operations).toHaveLength(0);
    expect(byType.Bucket.operations).toHaveLength(0);
    expect(byType.Key.enabled).toBe(false);
  });

  it('ignores non-numeric bean values (tags, modelerType, name)', () => {
    const { byType } = parseOmMetrics({
      name: 'Hadoop:service=OzoneManager,name=OMMetrics',
      modelerType: 'OMMetrics',
      'tag.Hostname': 'node1',
    });
    expect(Object.values(byType).every((t) => t.operations.length === 0)).toBe(true);
  });

  it('surfaces a failure whose request counter is absent as a failure-only row', () => {
    const { byType } = parseOmMetrics({ NumRecoverLeaseFails: 3 });
    expect(byType.Recover.enabled).toBe(true);
    expect(byType.Recover.operations).toEqual([
      expect.objectContaining({ name: 'Lease', requests: 0, failures: 3, status: 'Warning' }),
    ]);
  });
});

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

const bean: OMMetricsBean = {
  name: 'Hadoop:service=OzoneManager,name=OMMetrics',
  'tag.Hostname': 'node1',
  NumVolumes: 1,
  NumBuckets: 2,
  NumKeys: 485,
  TotalDataCommitted: 62259,
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

  it('joins plural request names to their singular failure counterpart', () => {
    const commit = key.operations.find((o) => o.name === 'Commit');
    const del = key.operations.find((o) => o.name === 'Delete');
    expect(commit).toMatchObject({ requests: 965, failures: 0, status: 'Active' });
    // Deletes has 5 DeleteFails → Warning.
    expect(del).toMatchObject({ requests: 965, failures: 5, status: 'Warning' });
  });

  it('keeps a request name that has no failure counterpart', () => {
    const get = parseOmMetrics(bean).byType.Get;
    expect(get.operations.map((o) => o.name)).toContain('ServiceLists');
  });

  it('surfaces a failure-only operation (0 requests) with Warning status', () => {
    const list = key.operations.find((o) => o.name === 'List');
    expect(list).toMatchObject({ requests: 0, failures: 100, status: 'Warning' });
  });

  it('omits operations with no activity (0 requests, 0 failures)', () => {
    // NumKeyHSyncs = 0 with no failures → not shown.
    expect(key.operations.find((o) => o.name === 'HSync')).toBeUndefined();
  });

  it('uses Num<Type>Ops for totalRequests', () => {
    expect(key.totalRequests).toBe(2895);
  });

  it('sorts operations by requests descending', () => {
    const requests = key.operations.map((o) => o.requests);
    expect(requests).toEqual([...requests].sort((a, b) => b - a));
  });
});

describe('parseOmMetrics — enabled flag', () => {
  const { byType } = parseOmMetrics(bean);

  it('enables types with activity', () => {
    expect(byType.Key.enabled).toBe(true);
    expect(byType.Get.enabled).toBe(true);
  });

  it('disables types with no metrics in the bean', () => {
    expect(byType.Snapshot.enabled).toBe(false);
    expect(byType.Snapshot.operations).toHaveLength(0);
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

  it('shows a failure-only operation type and marks it enabled', () => {
    const { byType } = parseOmMetrics({ NumRecoverLeaseFails: 3 });
    expect(byType.Recover.enabled).toBe(true);
    expect(byType.Recover.operations).toEqual([
      expect.objectContaining({ name: 'Lease', requests: 0, failures: 3, status: 'Warning' }),
    ]);
  });

  it('falls back to summing requests when Num<Type>Ops is absent', () => {
    const { byType } = parseOmMetrics({ NumGetServiceLists: 990, NumGetAcl: 10 });
    expect(byType.Get.totalRequests).toBe(1000);
  });
});

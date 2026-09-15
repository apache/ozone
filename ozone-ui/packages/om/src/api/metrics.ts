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
 * drives the metric-type dropdown; a type with no activity is disabled there.
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
  'Initiate',
  'Key',
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
  /** Total requests: the bean's `Num<Type>Ops` if present, else the sum of requests. */
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

/** `Num<Type><Op>` with an optional `Fails` suffix; `Num<Type>Ops` is the total. */
const METRIC_KEY_RE = /^Num([A-Z][a-z]+)([A-Z].+?)(Fails)?$/;

function numeric(bean: OMMetricsBean, key: string): number {
  const value = bean[key];
  return typeof value === 'number' ? value : 0;
}

/** Drop a trailing plural `s` so request names line up with failure names. */
function singular(name: string): string {
  return name.endsWith('s') ? name.slice(0, -1) : name;
}

function statusFor(requests: number, failures: number): OperationStatus {
  if (failures > 0) {
    return 'Warning';
  }
  if (requests > 0) {
    return 'Active';
  }
  return 'Inactive';
}

interface TypeAccumulator {
  ops?: number;
  requests: Map<string, number>;
  failures: Map<string, number>;
}

/**
 * Parse the `OMMetrics` bean into per-type operation data and the object-count
 * summary. Request counters (`NumKeyCommits`) are joined to their failure
 * counterpart (`NumKeyCommitFails`) into a single operation row — matching by the
 * failure name or the request name minus a trailing `s`, so `Commits`→`Commit`
 * while an op with no failure counterpart (e.g. `ServiceLists`) keeps its name.
 * Failures with no matching request become their own rows (requests = 0).
 */
export function parseOmMetrics(bean: OMMetricsBean | undefined): ParsedOmMetrics {
  const data = bean ?? {};

  const summary: MetricsSummary = {
    volumes: numeric(data, 'NumVolumes'),
    buckets: numeric(data, 'NumBuckets'),
    keys: numeric(data, 'NumKeys'),
    totalCommittedBytes: numeric(data, 'TotalDataCommitted'),
  };

  const acc: Record<string, TypeAccumulator> = {};
  for (const [key, value] of Object.entries(data)) {
    if (typeof value !== 'number') {
      continue;
    }
    const match = key.match(METRIC_KEY_RE);
    if (!match) {
      continue;
    }
    const [, type, name, failed] = match;
    const bucket = (acc[type] ??= { requests: new Map(), failures: new Map() });
    if (failed) {
      bucket.failures.set(name, (bucket.failures.get(name) ?? 0) + value);
    } else if (name === 'Ops') {
      bucket.ops = value;
    } else {
      bucket.requests.set(name, (bucket.requests.get(name) ?? 0) + value);
    }
  }

  const byType: Record<string, MetricTypeData> = {};
  for (const type of METRIC_TYPES) {
    const bucket = acc[type];
    const requests = bucket?.requests ?? new Map<string, number>();
    const failures = new Map(bucket?.failures ?? new Map<string, number>());
    const operations: MetricOperation[] = [];

    for (const [reqName, reqVal] of requests) {
      let label = reqName;
      let failVal = 0;
      const sg = singular(reqName);
      if (failures.has(reqName)) {
        failVal = failures.get(reqName) ?? 0;
        failures.delete(reqName);
      } else if (failures.has(sg)) {
        label = sg;
        failVal = failures.get(sg) ?? 0;
        failures.delete(sg);
      }
      operations.push({
        key: label,
        name: label,
        requests: reqVal,
        failures: failVal,
        status: statusFor(reqVal, failVal),
      });
    }

    // Failures with no matching request — shown in the table, excluded from the bar.
    for (const [failName, failVal] of failures) {
      operations.push({
        key: failName,
        name: failName,
        requests: 0,
        failures: failVal,
        status: statusFor(0, failVal),
      });
    }

    const shown = operations
      .filter((op) => op.requests > 0 || op.failures > 0)
      .sort((a, b) => b.requests - a.requests);
    const sumRequests = [...requests.values()].reduce((sum, n) => sum + n, 0);

    byType[type] = {
      type,
      totalRequests: bucket?.ops ?? sumRequests,
      operations: shown,
      enabled: shown.length > 0,
    };
  }

  return { summary, byType };
}

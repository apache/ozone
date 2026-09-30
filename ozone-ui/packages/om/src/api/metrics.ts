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

import { METRIC_TYPES, OM_OPERATIONS, OTHER_TYPE, SUMMARY_KEYS } from './metricsCatalog';

export { METRIC_TYPES, OTHER_TYPE } from './metricsCatalog';
export type { MetricType, OmOperationDef } from './metricsCatalog';

/** JMX query for the OM metrics bean (RPC operation counters + object counts). */
export const OM_METRICS_QUERY = 'Hadoop:service=OzoneManager,name=OMMetrics';

/** Raw OM metrics JMX bean — dynamic `Num*`/count keys plus string tags. */
export type OMMetricsBean = Record<string, number | string>;

export type OperationStatus = 'Active' | 'Warning' | 'Inactive';

export interface MetricOperation {
  key: string;
  /** Human-readable operation label (e.g. `Commit`, `Check Access`). */
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

function statusFor(requests: number, failures: number): OperationStatus {
  if (failures > 0) {
    return 'Warning';
  }
  if (requests > 0) {
    return 'Active';
  }
  return 'Inactive';
}

/** `Num<Type><Op>` shape, used only to derive a readable label for "Other" rows. */
const OTHER_NAME_RE = /^Num([A-Z][a-z]+)([A-Z].+)$/;

/** Readable label for an unlisted metric: `Num<Type><Op>` → "Type Op", else the raw name. */
function otherDisplayName(key: string): string {
  const match = key.match(OTHER_NAME_RE);
  return match ? `${match[1]} ${match[2]}` : key;
}

interface OpRow {
  type: string;
  key: string;
  name: string;
  requests: number;
  failures: number;
}

/**
 * Parse the `OMMetrics` bean into per-type operation data and the object-count
 * summary.
 *
 * Operation identity comes from the explicit {@link OM_OPERATIONS} catalog (keyed by
 * exact JMX name), so counts/gauges and cross-category aggregates are never mistaken
 * for requests and failures are joined to their request by exact name. Any numeric
 * metric the catalog does not describe (aggregate `Num<Type>Ops`, internal counters,
 * or a future metric) is surfaced under the {@link OTHER_TYPE} category — labelled via
 * a regex when it fits `Num<Type><Op>`, otherwise by its raw JMX name — so nothing is
 * silently dropped.
 */
export function parseOmMetrics(bean: OMMetricsBean | undefined): ParsedOmMetrics {
  const data = bean ?? {};

  const numeric = (key: string): number => {
    const value = data[key];
    return typeof value === 'number' ? value : 0;
  };

  const summary: MetricsSummary = {
    volumes: numeric(SUMMARY_KEYS.volumes),
    buckets: numeric(SUMMARY_KEYS.buckets),
    keys: numeric(SUMMARY_KEYS.keys),
    totalCommittedBytes: numeric(SUMMARY_KEYS.totalCommittedBytes),
  };

  // Keys the catalog/summary already account for — everything else numeric is "Other".
  const referenced = new Set<string>(Object.values(SUMMARY_KEYS));
  const rows: OpRow[] = OM_OPERATIONS.map((op) => {
    if (op.requestKey) {
      referenced.add(op.requestKey);
    }
    if (op.failureKey) {
      referenced.add(op.failureKey);
    }
    return {
      type: op.type,
      key: op.requestKey ?? op.failureKey ?? op.name,
      name: op.name,
      requests: op.requestKey ? numeric(op.requestKey) : 0,
      failures: op.failureKey ? numeric(op.failureKey) : 0,
    };
  });

  for (const [key, value] of Object.entries(data)) {
    if (typeof value !== 'number' || referenced.has(key)) {
      continue;
    }
    const isFailure = key.endsWith('Fails');
    rows.push({
      type: OTHER_TYPE,
      key,
      name: otherDisplayName(key),
      requests: isFailure ? 0 : value,
      failures: isFailure ? value : 0,
    });
  }

  const rowsByType = new Map<string, OpRow[]>();
  for (const row of rows) {
    const list = rowsByType.get(row.type);
    if (list) {
      list.push(row);
    } else {
      rowsByType.set(row.type, [row]);
    }
  }

  // Every known type plus any discovered in the bean (e.g. "Other"), so a new metric
  // category is surfaced rather than dropped.
  const types = new Set<string>([...METRIC_TYPES, ...rowsByType.keys()]);
  const byType: Record<string, MetricTypeData> = {};
  for (const type of types) {
    const shown = (rowsByType.get(type) ?? [])
      .filter((op) => op.requests > 0 || op.failures > 0)
      .map((op) => ({
        key: op.key,
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

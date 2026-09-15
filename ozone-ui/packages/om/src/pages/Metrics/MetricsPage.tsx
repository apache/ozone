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

import React, { Suspense, useMemo, useState } from 'react';
import { Empty, Select, Skeleton, type TableColumnsType } from 'antd';
import filesize from 'filesize';
import {
  Card,
  Chip,
  chartPalette,
  DataTable,
  KeyValuePair,
  PageHeader,
  QueryErrorBoundary,
  StackedBar,
  spacing,
  type ChipColor,
} from '@ozone-ui/shared';
import {
  METRIC_TYPES,
  OM_METRICS_QUERY,
  parseOmMetrics,
  type MetricOperation,
  type OMMetricsBean,
  type OperationStatus,
} from '../../api/metrics';
import { useSuspenseJmxBean } from '../../api/useJmx';

const summaryGridStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
  gap: `${spacing.lg}px ${spacing.xl}px`,
};

const statusColor: Record<OperationStatus, ChipColor> = {
  Active: 'green',
  Warning: 'orange',
  Inactive: 'neutral',
};

const columns: TableColumnsType<MetricOperation> = [
  {
    title: 'Status',
    dataIndex: 'status',
    key: 'status',
    width: 170,
    render: (status: OperationStatus) => (
      <Chip color={statusColor[status]} size="small">
        {status}
      </Chip>
    ),
  },
  { title: 'Operational Action', dataIndex: 'name', key: 'name' },
  {
    title: 'Failures',
    dataIndex: 'failures',
    key: 'failures',
    width: 170,
    render: (failures: number) => failures.toLocaleString('en-US'),
  },
];

const MetricsContent: React.FC = () => {
  const { data: bean, isEmpty } = useSuspenseJmxBean<OMMetricsBean>(OM_METRICS_QUERY);
  const { summary, byType } = useMemo(() => parseOmMetrics(bean), [bean]);

  const firstEnabled = METRIC_TYPES.find((t) => byType[t]?.enabled);
  const [selectedType, setSelectedType] = useState<string>(firstEnabled ?? 'Key');
  const selected = byType[selectedType] ?? byType.Key;

  const barSegments = useMemo(
    () =>
      (selected?.operations ?? [])
        .filter((op) => op.requests > 0)
        .map((op, i) => ({
          label: op.name,
          value: op.requests,
          color: chartPalette[i % chartPalette.length],
        })),
    [selected]
  );

  // Active types first (preserving METRIC_TYPES order), then the greyed-out
  // inactive ones — so the dropdown surfaces the types that have data.
  const options = [
    ...METRIC_TYPES.filter((type) => byType[type]?.enabled),
    ...METRIC_TYPES.filter((type) => !byType[type]?.enabled),
  ].map((type) => ({
    value: type,
    label: type,
    disabled: !byType[type]?.enabled,
  }));

  // No OMMetrics bean returned (e.g. servlet returned `{ beans: [] }`).
  if (isEmpty || !bean) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No JMX data available" />;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.xxl }}>
      {/* Object-count summary */}
      <Card>
        <div style={summaryGridStyle}>
          <KeyValuePair label="Volumes" value={summary.volumes.toLocaleString('en-US')} />
          <KeyValuePair label="Buckets" value={summary.buckets.toLocaleString('en-US')} />
          <KeyValuePair label="Keys" value={summary.keys.toLocaleString('en-US')} />
          <KeyValuePair
            label="Total Committed"
            value={filesize(summary.totalCommittedBytes, { round: 1 })}
          />
        </div>
      </Card>

      {/* Active operations for the selected metric type */}
      <Card title="Active Operations">
        <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.xl }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.md }}>
            <Select
              value={selectedType}
              onChange={setSelectedType}
              options={options}
              style={{ width: 237 }}
            />
            <KeyValuePair
              label="TOTAL REQUESTS"
              value={`${(selected?.totalRequests ?? 0).toLocaleString('en-US')} ops`}
            />
          </div>

          {barSegments.length > 0 && <StackedBar segments={barSegments} />}

          {selected && selected.operations.length > 0 ? (
            <DataTable<MetricOperation>
              columns={columns}
              dataSource={selected.operations}
              rowKey="key"
              size="middle"
            />
          ) : (
            <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No operations recorded" />
          )}
        </div>
      </Card>
    </div>
  );
};

/**
 * OM Metrics page: object-count summary plus a per-operation-type breakdown
 * (requests bar + status/failure table) sourced from the `OMMetrics` JMX bean.
 */
export const MetricsPage: React.FC = () => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.xxl }}>
    <PageHeader
      title="Ozone Manager"
      subtitle="Real-time transactional operation telemetry and object volume statistics"
    />
    <QueryErrorBoundary>
      <Suspense fallback={<Skeleton active paragraph={{ rows: 6 }} />}>
        <MetricsContent />
      </Suspense>
    </QueryErrorBoundary>
  </div>
);

export default MetricsPage;

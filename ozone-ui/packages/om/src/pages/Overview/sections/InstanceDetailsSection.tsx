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

import React, { Suspense } from 'react';
import { Divider, Empty, Skeleton } from 'antd';
import { useSuspenseQueries } from '@tanstack/react-query';
import { Card, KeyValuePair, Section } from '@ozone-ui/shared';
import {
  JMX_QUERY,
  formatElapsed,
  formatStarted,
  parseRatisRoles,
  type LeaderElectionCountBean,
  type LeaderElectionElapsedBean,
  type OzoneManagerInfoBean,
  type RatisServerBean,
} from '../../../api/overview';
import { jmxQueryOptions } from '../../../api/useJmx';

const kvGridStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
  gap: '16px 24px',
};

const InstanceDetailsContent: React.FC = () => {
  // Fetch all four beans in parallel (avoids an intra-component suspense waterfall).
  const [omInfoQ, ratisQ, countQ, elapsedQ] = useSuspenseQueries({
    queries: [
      jmxQueryOptions<OzoneManagerInfoBean>(JMX_QUERY.omInfo),
      jmxQueryOptions<RatisServerBean>(JMX_QUERY.ratisServer),
      jmxQueryOptions<LeaderElectionCountBean>(JMX_QUERY.leaderElectionCount),
      jmxQueryOptions<LeaderElectionElapsedBean>(JMX_QUERY.leaderElectionElapsed),
    ],
  });

  const omInfo = omInfoQ.data[0];
  const ratis = ratisQ.data[0];

  if (!omInfo) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No JMX data available" />;
  }

  const currentHost = parseRatisRoles(omInfo.RatisRoles, ratis?.Id).find(
    (r) => r.isCurrent
  )?.hostName;

  const count = countQ.data[0]?.Count;
  const elapsed = elapsedQ.data[0]?.Value;
  const electionCount = count != null && count !== -1 ? String(count) : '—';
  const electionElapsed = elapsed != null && elapsed !== -1 ? formatElapsed(elapsed) : '—';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      <div style={kvGridStyle}>
        <KeyValuePair label="Host" value={currentHost ?? '—'} />
        {omInfo.Namespace && <KeyValuePair label="Namespace" value={omInfo.Namespace} />}
        <KeyValuePair label="Started" value={formatStarted(omInfo.StartedTimeInMillis)} />
        <KeyValuePair label="Version" value={omInfo.Version} copyable />
        <KeyValuePair label="Compiled" value={omInfo.CompileInfo} />
      </div>
      <Divider style={{ margin: 0 }} />
      <div style={kvGridStyle}>
        <KeyValuePair label="Remote Procedure Call Port" value={omInfo.RpcPort} />
        <KeyValuePair label="Group ID" value={ratis?.GroupId ?? '—'} />
        <KeyValuePair label="Election Count" value={electionCount} />
        <KeyValuePair label="Last Election Elapsed Time" value={electionElapsed} />
      </div>
    </div>
  );
};

/**
 * "Instance Details" card. The top row identifies the instance (host, namespace,
 * build); the bottom row (below a divider) shows this node's runtime details —
 * RPC port, Ratis group and leader-election metrics — which are exposed only by
 * the OM node serving the UI.
 */
export const InstanceDetailsSection: React.FC = () => (
  <Section title="Instance Details">
    <Card>
      <Suspense fallback={<Skeleton active paragraph={{ rows: 3 }} />}>
        <InstanceDetailsContent />
      </Suspense>
    </Card>
  </Section>
);

export default InstanceDetailsSection;

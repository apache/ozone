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

import React from 'react';
import type { TableColumnsType } from 'antd';
import { Chip, DataTable, KeyValuePair, Section, TextLink } from '@ozone-ui/shared';
import {
  JMX_QUERY,
  formatElapsed,
  parseRatisRoles,
  type LeaderElectionCountBean,
  type LeaderElectionElapsedBean,
  type OzoneManagerInfoBean,
  type RatisRole,
  type RatisServerBean,
} from '../../../api/overview';
import { useJmxBean } from '../../../api/useJmx';
import SectionBody from '../SectionBody';
import type { SectionProps } from './InstanceDetailsSection';

/** Grid for the per-host details revealed when a role row is expanded. */
const detailsGridStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
  gap: '16px 24px',
  padding: '4px 8px 8px',
};

const columns: TableColumnsType<RatisRole> = [
  {
    title: 'Host Name',
    dataIndex: 'hostName',
    key: 'hostName',
    render: (hostName: string, row) => (
      <TextLink href="#" style={{ fontWeight: row.isCurrent ? 600 : undefined }}>
        {hostName}
      </TextLink>
    ),
  },
  { title: 'Node ID', dataIndex: 'nodeId', key: 'nodeId' },
  { title: 'Ratis Port', dataIndex: 'ratisPort', key: 'ratisPort' },
  {
    title: 'Role',
    dataIndex: 'role',
    key: 'role',
    render: (role: string) => (
      <Chip color={role === 'LEADER' ? 'blue' : 'neutral'} size="small">
        {role.charAt(0) + role.slice(1).toLowerCase()}
      </Chip>
    ),
  },
  {
    title: 'Leader Readiness',
    dataIndex: 'readiness',
    key: 'readiness',
    render: (readiness: RatisRole['readiness']) =>
      readiness ? (
        <Chip color={readiness === 'Synced' ? 'green' : 'orange'} size="small">
          {readiness}
        </Chip>
      ) : (
        '—'
      ),
  },
];

/** "Ozone Manager Roles" HA table. Sourced from the OM ServerRuntime bean. */
export const RolesSection: React.FC<SectionProps> = ({ refreshToken }) => {
  const {
    data: omInfo,
    loading,
    error,
  } = useJmxBean<OzoneManagerInfoBean>(JMX_QUERY.omInfo, refreshToken);
  const { data: ratis } = useJmxBean<RatisServerBean>(JMX_QUERY.ratisServer, refreshToken);
  const { data: electionCount } = useJmxBean<LeaderElectionCountBean>(
    JMX_QUERY.leaderElectionCount,
    refreshToken
  );
  const { data: electionElapsed } = useJmxBean<LeaderElectionElapsedBean>(
    JMX_QUERY.leaderElectionElapsed,
    refreshToken
  );

  const roles = omInfo ? parseRatisRoles(omInfo.RatisRoles, ratis?.Id) : [];

  // These details (RPC port, group id, leader-election metrics) are exposed only
  // by the OM node serving the UI — so only the current node's row is
  // expandable. Election count / elapsed time are hidden when absent or -1,
  // mirroring the legacy OM UI.
  const count = electionCount?.Count;
  const elapsed = electionElapsed?.Value;
  const showCount = count != null && count !== -1;
  const showElapsed = elapsed != null && elapsed !== -1;

  const renderHostDetails = () => (
    <div style={detailsGridStyle}>
      <KeyValuePair label="Remote Procedure Call Port" value={omInfo?.RpcPort ?? '—'} />
      <KeyValuePair label="Group ID" value={ratis?.GroupId ?? '—'} />
      {showCount && <KeyValuePair label="Election Count" value={String(count)} />}
      {showElapsed && (
        <KeyValuePair label="Last Election Elapsed Time" value={formatElapsed(elapsed)} />
      )}
    </div>
  );

  return (
    <Section title="Ozone Manager Roles" description="High Availability">
      <SectionBody loading={loading} error={error} skeletonRows={3}>
        <DataTable<RatisRole>
          columns={columns}
          dataSource={roles}
          rowKey="key"
          size="middle"
          expandable={{
            expandedRowRender: renderHostDetails,
            rowExpandable: (record) => record.isCurrent,
          }}
        />
      </SectionBody>
    </Section>
  );
};

export default RolesSection;

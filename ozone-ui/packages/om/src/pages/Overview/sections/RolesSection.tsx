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
import { Skeleton, type TableColumnsType } from 'antd';
import { Chip, DataTable, Section } from '@ozone-ui/shared';
import {
  JMX_QUERY,
  parseRatisRoles,
  type OzoneManagerInfoBean,
  type RatisRole,
  type RatisServerBean,
} from '../../../api/overview';
import { useSuspenseJmxBean } from '../../../api/useJmx';

const columns: TableColumnsType<RatisRole> = [
  {
    title: 'Host Name',
    dataIndex: 'hostName',
    key: 'hostName',
    render: (hostName: string, row) => (
      <span style={{ fontWeight: row.isCurrent ? 600 : undefined }}>{hostName}</span>
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

const RolesContent: React.FC = () => {
  const { data: omInfo } = useSuspenseJmxBean<OzoneManagerInfoBean>(JMX_QUERY.omInfo);
  const { data: ratis } = useSuspenseJmxBean<RatisServerBean>(JMX_QUERY.ratisServer);

  const roles = omInfo ? parseRatisRoles(omInfo.RatisRoles, ratis?.Id) : [];

  return <DataTable<RatisRole> columns={columns} dataSource={roles} rowKey="key" size="middle" />;
};

/** "Ozone Manager Roles" HA table. Sourced from the OM ServerRuntime bean. */
export const RolesSection: React.FC = () => (
  <Section title="Ozone Manager Roles" description="High Availability">
    <Suspense fallback={<Skeleton active paragraph={{ rows: 3 }} />}>
      <RolesContent />
    </Suspense>
  </Section>
);

export default RolesSection;

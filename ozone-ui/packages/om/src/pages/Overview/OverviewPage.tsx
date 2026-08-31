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
import { PageHeader, QueryErrorBoundary, spacing } from '@ozone-ui/shared';
import InstanceDetailsSection from './sections/InstanceDetailsSection';
import RolesSection from './sections/RolesSection';
import MetadataVolumeSection from './sections/MetadataVolumeSection';
import JvmSection from './sections/JvmSection';

/**
 * OM Overview page. Each section fetches its own JMX MBean lazily via TanStack
 * Query (with `useSuspenseQuery`); sections that share a query are de-duplicated
 * to a single request by query key. A single `QueryErrorBoundary` wraps all
 * sections so a `/jmx` transport or server failure shows one page-level error
 * state rather than per-section alerts. Refresh is driven from the utility bar
 * via the SyncChip.
 */
export const OverviewPage: React.FC = () => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.xxl }}>
    <PageHeader title="Overview" />
    <QueryErrorBoundary>
      <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.xxl }}>
        <InstanceDetailsSection />
        <RolesSection />
        <MetadataVolumeSection />
        <JvmSection />
      </div>
    </QueryErrorBoundary>
  </div>
);

export default OverviewPage;

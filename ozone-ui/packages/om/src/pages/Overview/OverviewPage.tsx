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

import React, { useState } from 'react';
import { Button } from 'antd';
import { PageHeader, Icon } from '@ozone-ui/shared';
import { clearJmxCache } from '../../api/jmx';
import InstanceDetailsSection from './sections/InstanceDetailsSection';
import RolesSection from './sections/RolesSection';
import MetadataVolumeSection from './sections/MetadataVolumeSection';
import JvmSection from './sections/JvmSection';

/**
 * OM Overview page. Each section fetches its own JMX MBean lazily; sections that
 * share a query (the OM ServerRuntime bean feeds three of them) are de-duplicated
 * to a single request by the JMX cache. Refresh clears the cache and re-fetches.
 */
export const OverviewPage: React.FC = () => {
  const [refreshToken, setRefreshToken] = useState(0);

  const refresh = () => {
    clearJmxCache();
    setRefreshToken((t) => t + 1);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 32 }}>
      <PageHeader
        title="Overview"
        actions={
          <Button icon={<Icon name="reports" size={16} />} onClick={refresh}>
            Refresh
          </Button>
        }
      />
      <InstanceDetailsSection refreshToken={refreshToken} />
      <RolesSection refreshToken={refreshToken} />
      <MetadataVolumeSection refreshToken={refreshToken} />
      <JvmSection refreshToken={refreshToken} />
    </div>
  );
};

export default OverviewPage;

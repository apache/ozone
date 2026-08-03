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
import { Empty, Skeleton } from 'antd';
import { Card, KeyValuePair, Section } from '@ozone-ui/shared';
import { JMX_QUERY, type OzoneManagerInfoBean } from '../../../api/overview';
import { useSuspenseJmxBean } from '../../../api/useJmx';

const gridStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
  gap: '16px 24px',
};

const MetadataVolumeContent: React.FC = () => {
  const { data: omInfo, isEmpty } = useSuspenseJmxBean<OzoneManagerInfoBean>(JMX_QUERY.omInfo);

  if (isEmpty || !omInfo) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No JMX data available" />;
  }

  return (
    <div style={gridStyle}>
      <KeyValuePair label="RATIS LOG DIRECTORY" value={omInfo.RatisLogDirectory} copyable />
      <KeyValuePair label="ROCKSDB DIRECTORY" value={omInfo.RocksDbDirectory} copyable />
    </div>
  );
};

/** "Metadata Volume Information" card. Sourced from the OM ServerRuntime bean. */
export const MetadataVolumeSection: React.FC = () => (
  <Section title="Metadata Volume Information">
    <Card>
      <Suspense fallback={<Skeleton active paragraph={{ rows: 1 }} />}>
        <MetadataVolumeContent />
      </Suspense>
    </Card>
  </Section>
);

export default MetadataVolumeSection;

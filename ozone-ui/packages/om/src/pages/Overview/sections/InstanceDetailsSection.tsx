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
import { Card, KeyValuePair, Section } from '@ozone-ui/shared';
import {
  JMX_QUERY,
  formatStarted,
  parseRatisRoles,
  type OzoneManagerInfoBean,
  type RatisServerBean,
} from '../../../api/overview';
import { useJmxBean } from '../../../api/useJmx';
import SectionBody from '../SectionBody';

const kvGridStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
  gap: '16px 24px',
};

/**
 * "Instance Details" card. Sourced from the OM ServerRuntime bean (shared with
 * the Roles and Metadata Volume sections) plus this node's Ratis bean.
 */
export const InstanceDetailsSection: React.FC = () => {
  const {
    data: omInfo,
    isLoading,
    error,
    isEmpty,
  } = useJmxBean<OzoneManagerInfoBean>(JMX_QUERY.omInfo);
  const { data: ratis } = useJmxBean<RatisServerBean>(JMX_QUERY.ratisServer);

  const currentHost = omInfo
    ? parseRatisRoles(omInfo.RatisRoles, ratis?.Id).find((r) => r.isCurrent)?.hostName
    : undefined;

  return (
    <Section title="Instance Details">
      <Card>
        <SectionBody
          loading={isLoading}
          error={error ?? undefined}
          isEmpty={isEmpty}
          skeletonRows={2}
        >
          {omInfo && (
            <div style={kvGridStyle}>
              <KeyValuePair label="Host" value={currentHost ?? '—'} />
              <KeyValuePair label="Namespace" value={ratis?.GroupId ?? '—'} />
              <KeyValuePair label="RPC Port" value={omInfo.RpcPort} />
              <KeyValuePair label="Started" value={formatStarted(omInfo.StartedTimeInMillis)} />
              <KeyValuePair label="Version" value={omInfo.Version} copyable />
              <KeyValuePair label="Compiled" value={omInfo.CompileInfo} />
            </div>
          )}
        </SectionBody>
      </Card>
    </Section>
  );
};

export default InstanceDetailsSection;

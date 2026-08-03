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
import { Empty, Skeleton } from 'antd';
import { Alert } from '@ozone-ui/shared';

export interface SectionBodyProps {
  loading: boolean;
  error?: Error;
  /** Query succeeded but returned no data — renders an explicit empty state. */
  isEmpty?: boolean;
  /** Message for the empty state. Defaults to "No JMX data available". */
  emptyMessage?: string;
  /** Number of skeleton rows to show while loading. Defaults to 2. */
  skeletonRows?: number;
  children: React.ReactNode;
}

/**
 * Renders a section's async state: a skeleton while loading, an error alert on
 * failure, an explicit empty state when the query returned no data, or the
 * resolved content.
 */
export const SectionBody: React.FC<SectionBodyProps> = ({
  loading,
  error,
  isEmpty = false,
  emptyMessage = 'No JMX data available',
  skeletonRows = 2,
  children,
}) => {
  if (error) {
    return <Alert type="error" showIcon message="Failed to load" description={error.message} />;
  }
  if (loading) {
    return <Skeleton active paragraph={{ rows: skeletonRows }} />;
  }
  if (isEmpty) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={emptyMessage} />;
  }
  return <>{children}</>;
};

export default SectionBody;

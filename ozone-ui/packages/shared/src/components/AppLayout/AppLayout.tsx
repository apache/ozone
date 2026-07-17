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
import { Layout, Typography } from 'antd';
import { semanticColors, spacing, textStyles } from '../../theme/tokens';

const { Header, Content } = Layout;

export interface AppLayoutProps {
  /** Navigation rail, typically the shared `Sidebar`. */
  sider?: React.ReactNode;
  /** Page/section title rendered in the header. */
  title?: React.ReactNode;
  /** Right-aligned header content (actions, user menu, breadcrumbs, ...). */
  headerExtra?: React.ReactNode;
  /** Main page content. */
  children?: React.ReactNode;
  /** Constrain content width and centre it (useful for form/detail pages). */
  maxContentWidth?: number;
}

/**
 * Application shell: a fixed navigation rail on the left, a top header with a
 * title and optional actions, and a scrollable content area on the design-system
 * layout background. Compose with the shared `Sidebar` for the `sider` slot.
 */
export const AppLayout: React.FC<AppLayoutProps> = ({
  sider,
  title,
  headerExtra,
  children,
  maxContentWidth,
}) => {
  return (
    <Layout style={{ minHeight: '100vh' }}>
      {sider}
      <Layout>
        {(title || headerExtra) && (
          <Header
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              background: semanticColors.bgContainer,
              borderBottom: `1px solid ${semanticColors.border}`,
              paddingInline: spacing.xl,
            }}
          >
            {typeof title === 'string' ? (
              <Typography.Title
                level={4}
                style={{
                  margin: 0,
                  fontSize: textStyles.h2.fontSize,
                  lineHeight: `${textStyles.h2.lineHeight}px`,
                  color: semanticColors.textPrimary,
                }}
              >
                {title}
              </Typography.Title>
            ) : (
              title
            )}
            {headerExtra && (
              <div style={{ display: 'flex', alignItems: 'center', gap: spacing.md }}>
                {headerExtra}
              </div>
            )}
          </Header>
        )}
        <Content
          style={{
            padding: spacing.xl,
            background: semanticColors.bgLayout,
            overflow: 'auto',
          }}
        >
          <div
            style={{
              maxWidth: maxContentWidth,
              marginInline: maxContentWidth ? 'auto' : undefined,
              width: '100%',
            }}
          >
            {children}
          </div>
        </Content>
      </Layout>
    </Layout>
  );
};

export default AppLayout;

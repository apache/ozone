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
import { Typography } from 'antd';
import { semanticColors, spacing, textStyles } from '../../theme/tokens';

export interface PageHeaderProps {
  /** Page title. */
  title: React.ReactNode;
  /** Optional supporting text under the title. */
  subtitle?: React.ReactNode;
  /** Optional content rendered above the title (e.g. breadcrumbs). */
  breadcrumb?: React.ReactNode;
  style?: React.CSSProperties;
}

/**
 * Page header. Renders the page title with optional breadcrumb, subtitle and
 * right-aligned actions, matching the "Page Header" component used at the top of
 * the Ozone content area.
 */
export const PageHeader: React.FC<PageHeaderProps> = ({ title, subtitle, breadcrumb, style }) => {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: spacing.xs,
        marginBottom: spacing.xl,
        ...style,
      }}
    >
      {breadcrumb}
      <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.xxs }}>
        <Typography.Title
          level={1}
          style={{
            margin: 0,
            fontSize: textStyles.h1.fontSize,
            lineHeight: `${textStyles.h1.lineHeight}px`,
            fontWeight: textStyles.h1.fontWeight,
            color: semanticColors.textPrimary,
          }}
        >
          {title}
        </Typography.Title>
        {subtitle && (
          <Typography.Text
            style={{
              color: semanticColors.textSecondary,
              fontSize: textStyles.bodyStandard.fontSize,
              lineHeight: `${textStyles.bodyStandard.lineHeight}px`,
            }}
          >
            {subtitle}
          </Typography.Text>
        )}
      </div>
    </div>
  );
};

export default PageHeader;

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

export interface SectionProps {
  /** Section heading. */
  title: React.ReactNode;
  /** Optional supporting text rendered under the title (e.g. "High Availability"). */
  description?: React.ReactNode;
  /** Right-aligned actions rendered on the header row. */
  actions?: React.ReactNode;
  /** Section body. */
  children?: React.ReactNode;
  style?: React.CSSProperties;
}

/**
 * A labelled content section: a title (with optional supporting text and
 * right-aligned actions) followed by its content. Matches the "section-header"
 * pattern that groups the cards and tables on the Ozone detail screens.
 */
export const Section: React.FC<SectionProps> = ({
  title,
  description,
  actions,
  children,
  style,
}) => (
  <section style={{ display: 'flex', flexDirection: 'column', gap: spacing.lg, ...style }}>
    <div
      style={{
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'space-between',
        gap: spacing.lg,
      }}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.xxs }}>
        <Typography.Title
          level={3}
          style={{
            margin: 0,
            fontSize: textStyles.h3.fontSize,
            lineHeight: `${textStyles.h3.lineHeight}px`,
            fontWeight: textStyles.h3.fontWeight,
            color: semanticColors.textPrimary,
          }}
        >
          {title}
        </Typography.Title>
        {description && (
          <Typography.Text
            style={{
              color: semanticColors.textSecondary,
              fontSize: textStyles.bodySmall.fontSize,
              lineHeight: `${textStyles.bodySmall.lineHeight}px`,
            }}
          >
            {description}
          </Typography.Text>
        )}
      </div>
      {actions && (
        <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>{actions}</div>
      )}
    </div>
    {children}
  </section>
);

export default Section;

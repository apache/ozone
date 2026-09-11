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
import { Tooltip, Typography } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import { colors, semanticColors, spacing, textStyles } from '../../theme/tokens';

export interface KeyValuePairProps {
  /** The label (key) text. */
  label: React.ReactNode;
  /** The value. If `href` is set, it renders as a text link. */
  value: React.ReactNode;
  /** Render the value as a link to this destination ("Standard Link" variant). */
  href?: string;
  /** Layout direction. `vertical` (default) stacks label over value. */
  layout?: 'vertical' | 'horizontal';
  /** Width of the label column when `layout="horizontal"`. */
  labelWidth?: number | string;
  /** Allow the value to be copied (adds an inline copy affordance). */
  copyable?: boolean;
  /** Optional help text shown via an info (i) icon next to the label. */
  tooltip?: React.ReactNode;
  style?: React.CSSProperties;
}

/**
 * Key-value pair. Displays a small secondary label with its value, matching
 * the "Key-Value Pair / Standard" and "Standard Link" components used across the
 * Ozone detail cards.
 */
export const KeyValuePair: React.FC<KeyValuePairProps> = ({
  label,
  value,
  href,
  layout = 'vertical',
  labelWidth = 160,
  copyable = false,
  tooltip,
  style,
}) => {
  const isHorizontal = layout === 'horizontal';

  const labelNode = (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: spacing.xs,
        color: semanticColors.textSecondary,
        fontSize: textStyles.bodySmall.fontSize,
        lineHeight: `${textStyles.bodySmall.lineHeight}px`,
        flex: isHorizontal
          ? `0 0 ${typeof labelWidth === 'number' ? `${labelWidth}px` : labelWidth}`
          : undefined,
      }}
    >
      {label}
      {tooltip && (
        <Tooltip title={tooltip}>
          <InfoCircleOutlined
            aria-label="More information"
            style={{ color: colors.green[500], cursor: 'help', fontSize: 12 }}
          />
        </Tooltip>
      )}
    </span>
  );

  const valueNode = href ? (
    <Typography.Link
      href={href}
      style={{
        fontSize: textStyles.bodyStandard.fontSize,
        lineHeight: `${textStyles.bodyStandard.lineHeight}px`,
      }}
    >
      {value}
    </Typography.Link>
  ) : (
    <Typography.Text
      copyable={copyable}
      style={{
        color: semanticColors.textPrimary,
        fontSize: textStyles.bodyStandard.fontSize,
        lineHeight: `${textStyles.bodyStandard.lineHeight}px`,
      }}
    >
      {value}
    </Typography.Text>
  );

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: isHorizontal ? 'row' : 'column',
        alignItems: isHorizontal ? 'baseline' : 'flex-start',
        gap: isHorizontal ? spacing.md : spacing.xxs,
        ...style,
      }}
    >
      {labelNode}
      {valueNode}
    </div>
  );
};

export default KeyValuePair;

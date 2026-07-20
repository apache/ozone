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
import { Button, Card as AntCard, type CardProps as AntCardProps } from 'antd';
import { DownOutlined, UpOutlined } from '@ant-design/icons';
import { radius, semanticColors } from '../../theme/tokens';

export interface CardProps extends AntCardProps {
  /**
   * Visual emphasis of the card surface.
   * - `outlined` (default): white surface with a subtle border.
   * - `elevated`: white surface with a soft shadow and no border.
   * - `filled`: muted pewter surface, useful for nested/secondary panels.
   */
  emphasis?: 'outlined' | 'elevated' | 'filled';
  /**
   * Enable the "Card With Header" collapse affordance: a caret toggle in the
   * header hides/shows the card body. Requires a `title`.
   */
  collapsible?: boolean;
  /** Initial collapsed state when `collapsible` (uncontrolled). */
  defaultCollapsed?: boolean;
}

const emphasisStyles: Record<NonNullable<CardProps['emphasis']>, React.CSSProperties> = {
  outlined: {
    border: `1px solid ${semanticColors.border}`,
    boxShadow: 'none',
    background: semanticColors.bgContainer,
  },
  elevated: {
    border: 'none',
    boxShadow: '0 1px 2px rgba(35, 43, 48, 0.06), 0 4px 12px rgba(35, 43, 48, 0.08)',
    background: semanticColors.bgElevated,
  },
  filled: {
    border: `1px solid ${semanticColors.border}`,
    boxShadow: 'none',
    background: semanticColors.bgLayout,
  },
};

/**
 * Surface container. Thin wrapper over Ant Design's `Card` that applies the
 * design-system radius, borders and elevation presets, plus an optional
 * collapsible header. All standard `Card` props are supported.
 */
export const Card: React.FC<CardProps> = ({
  emphasis = 'outlined',
  collapsible = false,
  defaultCollapsed = false,
  extra,
  children,
  style,
  styles,
  ...rest
}) => {
  const [collapsed, setCollapsed] = useState(defaultCollapsed);

  const collapseToggle = collapsible ? (
    <Button
      type="text"
      size="small"
      aria-label={collapsed ? 'Expand' : 'Collapse'}
      icon={collapsed ? <DownOutlined /> : <UpOutlined />}
      onClick={() => setCollapsed((c) => !c)}
    />
  ) : null;

  const mergedExtra = collapsible ? (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
      {extra}
      {collapseToggle}
    </span>
  ) : (
    extra
  );

  return (
    <AntCard
      variant="borderless"
      extra={mergedExtra}
      style={{ borderRadius: radius.lg, ...emphasisStyles[emphasis], ...style }}
      styles={{ header: { border: 'none' }, ...styles }}
      {...rest}
    >
      {collapsible && collapsed ? null : children}
    </AntCard>
  );
};

export default Card;

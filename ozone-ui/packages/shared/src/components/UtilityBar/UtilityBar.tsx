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
import { BellOutlined, QuestionCircleOutlined, UserOutlined } from '@ant-design/icons';
import { fontFamilies, semanticColors, spacing, textStyles } from '../../theme/tokens';
import IconButton from '../IconButton/IconButton';

export interface UtilityBarProps {
  /** Left slot, e.g. an app switcher or menu button. */
  leading?: React.ReactNode;
  /** Product branding shown next to the leading slot (name/logo + host chip). */
  branding?: React.ReactNode;
  /** @deprecated Use `branding`. Kept for back-compat; rendered when `branding` is unset. */
  title?: React.ReactNode;
  /** Optional centre slot (e.g. global search). */
  center?: React.ReactNode;
  /**
   * Right slot. When omitted, the bar renders the standard Help / Notifications
   * / Profile actions (wire them up via the `on*` handlers below).
   */
  actions?: React.ReactNode;
  /** Handler for the standard Help action (used when `actions` is not provided). */
  onHelp?: () => void;
  /** Handler for the standard Notifications action. */
  onNotifications?: () => void;
  /** Handler for the standard Profile action. */
  onProfile?: () => void;
  /** Height in px. Defaults to 48. */
  height?: number;
  style?: React.CSSProperties;
}

/**
 * Global top utility bar (the app chrome at the very top of every screen).
 * Provides a leading slot, product `branding`, an optional centre slot and
 * right-aligned actions — defaulting to the standard Help / Notifications /
 * Profile buttons when `actions` is not supplied.
 */
export const UtilityBar: React.FC<UtilityBarProps> = ({
  leading,
  branding,
  title,
  center,
  actions,
  onHelp,
  onNotifications,
  onProfile,
  height = 48,
  style,
}) => {
  const brand = branding ?? title;
  const rightContent = actions ?? (
    <>
      <IconButton
        icon={<QuestionCircleOutlined style={{ fontSize: 18 }} />}
        label="Help"
        onClick={onHelp}
      />
      <IconButton
        icon={<BellOutlined style={{ fontSize: 18 }} />}
        label="Notifications"
        onClick={onNotifications}
      />
      <IconButton
        icon={<UserOutlined style={{ fontSize: 18 }} />}
        label="Profile"
        onClick={onProfile}
      />
    </>
  );

  return (
  <div
    style={{
      display: 'flex',
      alignItems: 'center',
      gap: spacing.md,
      height,
      paddingInline: spacing.md,
      background: semanticColors.bgTopbar,
      color: semanticColors.textPrimary,
      ...style,
    }}
  >
    <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
      {leading}
      {brand && (
        <span
          style={{
            fontFamily: fontFamilies.appTitle,
            fontSize: textStyles.appTitle.fontSize,
            fontWeight: textStyles.appTitle.fontWeight,
            lineHeight: `${textStyles.appTitle.lineHeight}px`,
            color: semanticColors.textPrimary,
          }}
        >
          {brand}
        </span>
      )}
    </div>

    {center && <div style={{ flex: 1, display: 'flex', justifyContent: 'center' }}>{center}</div>}

    <div
      style={{
        marginLeft: center ? 0 : 'auto',
        display: 'flex',
        alignItems: 'center',
        gap: spacing.xs,
        color: semanticColors.textSecondary,
      }}
    >
      {rightContent}
    </div>
  </div>
  );
};

export default UtilityBar;

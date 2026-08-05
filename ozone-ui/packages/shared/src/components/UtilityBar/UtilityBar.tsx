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
import { QuestionCircleOutlined } from '@ant-design/icons';
import { fontFamilies, semanticColors, spacing, textStyles } from '../../theme/tokens';
import IconButton from '../IconButton/IconButton';
import SyncChip, { type DbSyncConfig } from '../SyncChip/SyncChip';

export interface UtilityBarProps {
  /** Left slot, e.g. an app switcher or menu button. */
  leading?: React.ReactNode;
  /** Product branding shown next to the leading slot (name/logo + host chip). */
  branding?: React.ReactNode;
  /** Optional centre slot (e.g. global search). */
  center?: React.ReactNode;
  /** Handler for the Help icon button. */
  onHelp?: () => void;
  /** Timestamp of the last data refresh; forwarded to the embedded `SyncChip`. */
  lastRefreshedAt?: Date;
  /**
   * Recon-only: configuration for the "Database Sync" row in the `SyncChip`
   * dropdown. Omit for OM, SCM and DN — the row is hidden when absent.
   */
  dbSyncConfig?: DbSyncConfig;
  /** Height in px. Defaults to 48. */
  height?: number;
  style?: React.CSSProperties;
}

/**
 * Global top utility bar. Renders a leading slot, product branding, an optional
 * centre slot, a Help button and the `SyncChip` auto-refresh control. Requires a
 * `SyncConfigProvider` ancestor so the chip can read and toggle the refresh state.
 */
export const UtilityBar: React.FC<UtilityBarProps> = ({
  leading,
  branding,
  center,
  onHelp,
  lastRefreshedAt,
  dbSyncConfig,
  height = 48,
  style,
}) => (
  <div
    style={{
      display: 'flex',
      alignItems: 'center',
      gap: spacing.md,
      height,
      paddingInline: spacing.md,
      background: semanticColors.bgTopbar,
      color: semanticColors.textPrimary,
      borderBottom: `1px solid ${semanticColors.border}`,
      ...style,
    }}
  >
    <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
      {leading}
      {branding && (
        <span
          style={{
            fontFamily: fontFamilies.appTitle,
            fontSize: textStyles.appTitle.fontSize,
            fontWeight: textStyles.appTitle.fontWeight,
            lineHeight: `${textStyles.appTitle.lineHeight}px`,
            color: semanticColors.textPrimary,
          }}
        >
          {branding}
        </span>
      )}
    </div>

    {center && <div style={{ flex: 1, display: 'flex', justifyContent: 'center' }}>{center}</div>}

    <div
      style={{
        marginLeft: center ? 0 : 'auto',
        display: 'flex',
        alignItems: 'center',
        gap: spacing.sm,
        color: semanticColors.textSecondary,
      }}
    >
      <IconButton
        icon={<QuestionCircleOutlined style={{ fontSize: 18 }} />}
        label="Help"
        onClick={onHelp}
      />
      <SyncChip lastRefreshedAt={lastRefreshedAt} dbSync={dbSyncConfig} />
    </div>
  </div>
);

export default UtilityBar;

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
import { Dropdown, Switch, Tooltip, Typography } from 'antd';
import { colors, radius, semanticColors, spacing, textStyles } from '../../theme/tokens';
import { useSyncConfig } from '../../data/SyncConfigContext';
import { fetchJson } from '../../data/fetchJson';
import Icon from '../Icon/Icon';
import IconButton from '../IconButton/IconButton';

/**
 * Configuration for the optional "Database Sync" row in the dropdown. This row
 * is Recon-specific and must be omitted for OM, SCM and DN — the row is hidden
 * when this prop is absent.
 */
export interface DbSyncConfig {
  /** Row label, e.g. `"Database Sync"`. */
  label: string;
  /** Status description, e.g. `"Delta update 1s ago, 3:01 PM"`. */
  description?: string;
  /** Tooltip on the sync icon button. */
  tooltip?: string;
  /**
   * Endpoint to call when the user clicks the sync button.
   * `SyncChip` issues a `POST` via `fetchJson` and manages the loading state.
   */
  url: string;
}

export interface SyncChipProps {
  /** Timestamp of the last data refresh; shown as "Refreshed at …" under Auto Refresh. */
  lastRefreshedAt?: Date;
  /**
   * Optional Recon-specific "Database Sync" row. Omit for OM, SCM and DN.
   */
  dbSync?: DbSyncConfig;
}

function formatRefreshed(d: Date): string {
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  });
}

const dropdownRowStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'flex-start',
  justifyContent: 'space-between',
  gap: spacing.xl,
  padding: `${spacing.sm}px ${spacing.md}px`,
};

const rowLabelStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: spacing.xs,
  fontSize: textStyles.bodyStandard.fontSize,
  fontWeight: 600,
  color: semanticColors.textPrimary,
};

const rowDescStyle: React.CSSProperties = {
  fontSize: textStyles.bodySmall.fontSize,
  color: semanticColors.textSecondary,
  lineHeight: `${textStyles.bodySmall.lineHeight}px`,
  marginTop: spacing.xxs,
  maxWidth: 200,
};

/**
 * Utility-bar chip showing the current auto-refresh state. Reads
 * `enabled`/`setEnabled` from the nearest `SyncConfigProvider`.
 *
 * - **Live Sync** (auto-refresh on): green pill — bg `green[50]`, text `green[950]`.
 * - **Manual Sync** (off): grey pill — bg `pewter[50]`, text `pewter[950]`.
 */
export const SyncChip: React.FC<SyncChipProps> = ({ lastRefreshedAt, dbSync }) => {
  const { enabled, setEnabled } = useSyncConfig();
  const [open, setOpen] = useState(false);
  const [dbSyncing, setDbSyncing] = useState(false);

  const bgColor = enabled ? colors.green[50] : colors.pewter[50];
  const textColor = enabled ? colors.green[950] : colors.pewter[950];
  const dotColor = enabled ? colors.green[600] : colors.pewter[400];
  const chipLabel = enabled ? 'Live Sync' : 'Manual Sync';

  const handleDbSync = async () => {
    if (!dbSync || dbSyncing) {
      return;
    }
    setDbSyncing(true);
    try {
      await fetchJson(dbSync.url, { method: 'POST' });
    } finally {
      setDbSyncing(false);
      setOpen(false);
    }
  };

  const dropdownContent = (
    <div
      style={{
        width: 280,
        background: semanticColors.bgElevated,
        borderRadius: radius.lg,
        border: `1px solid ${semanticColors.border}`,
        boxShadow: '0 4px 16px rgba(35, 43, 48, 0.12)',
        padding: `${spacing.xs}px 0`,
        overflow: 'hidden',
      }}
    >
      {/* Auto Refresh row */}
      <div style={dropdownRowStyle}>
        <div style={{ flex: 1 }}>
          <div style={rowLabelStyle}>
            <span>Auto Refresh</span>
            <Tooltip title="Toggles automatic background polling for on-screen metrics, tables, and event streams.">
              <span style={{ display: 'inline-flex', color: semanticColors.textTertiary }}>
                <Icon name="info" size={14} />
              </span>
            </Tooltip>
          </div>
          {lastRefreshedAt && (
            <Typography.Text style={rowDescStyle}>
              Refreshed at {formatRefreshed(lastRefreshedAt)}
            </Typography.Text>
          )}
        </div>
        <Switch checked={enabled} onChange={setEnabled} size="default" />
      </div>

      {/* Database Sync row — Recon only */}
      {dbSync && (
        <>
          <div
            style={{ height: 1, background: semanticColors.border, marginBlock: spacing.xs }}
            aria-hidden
          />
          <div style={dropdownRowStyle}>
            <div style={{ flex: 1 }}>
              <div style={rowLabelStyle}>
                <span>{dbSync.label}</span>
              </div>
              {dbSync.description && (
                <Typography.Text style={rowDescStyle}>{dbSync.description}</Typography.Text>
              )}
            </div>
            <IconButton
              icon={<Icon name="reports" size={16} />}
              label={dbSync.label}
              tooltip={dbSync.tooltip ?? `Trigger ${dbSync.label}`}
              loading={dbSyncing}
              onClick={handleDbSync}
            />
          </div>
        </>
      )}
    </div>
  );

  return (
    <Dropdown
      open={open}
      onOpenChange={setOpen}
      overlay={dropdownContent}
      trigger={['click']}
      placement="bottomRight"
    >
      <button
        aria-label={`Sync: ${chipLabel}. Click to open sync settings`}
        aria-expanded={open}
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: spacing.xs,
          height: 24,
          paddingInline: spacing.sm,
          borderRadius: radius.pill,
          background: bgColor,
          border: 'none',
          cursor: 'pointer',
          outline: 'none',
          fontSize: textStyles.bodySmall.fontSize,
          fontWeight: 400,
          color: textColor,
          whiteSpace: 'nowrap',
        }}
      >
        <span
          aria-hidden
          style={{
            width: 10,
            height: 10,
            borderRadius: radius.pill,
            background: dotColor,
            flexShrink: 0,
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          {enabled && (
            <span
              style={{
                width: 0,
                height: 0,
                borderTop: '3px solid transparent',
                borderBottom: '3px solid transparent',
                borderLeft: `4px solid ${colors.green[50]}`,
                marginLeft: 1,
              }}
            />
          )}
        </span>
        {chipLabel}
        <Icon
          name="chevron-down"
          size={12}
          style={{ color: textColor, marginLeft: spacing.xxs }}
          aria-hidden
        />
      </button>
    </Dropdown>
  );
};

export default SyncChip;

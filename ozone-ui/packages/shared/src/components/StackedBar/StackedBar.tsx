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
import { radius, semanticColors, spacing, textStyles } from '../../theme/tokens';

export interface StackedBarSegment {
  /** Legend label for the segment. */
  label: string;
  /** Numeric value; the segment width is `value / total`. */
  value: number;
  /** Fill colour (e.g. from `chartPalette`). */
  color: string;
}

export interface StackedBarProps {
  segments: StackedBarSegment[];
  /** Show the label/value legend above the bar. Defaults to `true`. */
  showLegend?: boolean;
  /** Format a segment value for the legend. Defaults to `toLocaleString`. */
  formatValue?: (value: number) => string;
  /** Track height in px. Defaults to 8. */
  height?: number;
  style?: React.CSSProperties;
}

const defaultFormat = (v: number) => v.toLocaleString('en-US');

/**
 * Horizontal proportional (stacked) bar with an optional legend. Each segment's
 * width is its share of the total. Used for the OM metrics "requests by
 * operation" breakdown, and general enough for any categorical proportion.
 */
export const StackedBar: React.FC<StackedBarProps> = ({
  segments,
  showLegend = true,
  formatValue = defaultFormat,
  height = 8,
  style,
}) => {
  const total = segments.reduce((sum, s) => sum + (s.value > 0 ? s.value : 0), 0);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.sm, ...style }}>
      {showLegend && segments.length > 0 && (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: `${spacing.xs}px ${spacing.lg}px` }}>
          {segments.map((s) => (
            <span
              key={s.label}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: spacing.xs,
                fontSize: textStyles.bodySmall.fontSize,
                lineHeight: `${textStyles.bodySmall.lineHeight}px`,
              }}
            >
              <span
                aria-hidden
                style={{
                  width: 6,
                  height: 6,
                  borderRadius: radius.pill,
                  backgroundColor: s.color,
                  flexShrink: 0,
                }}
              />
              <span style={{ color: semanticColors.textSecondary }}>{s.label}</span>
              <span style={{ color: semanticColors.textPrimary, fontWeight: 600 }}>
                {formatValue(s.value)}
              </span>
            </span>
          ))}
        </div>
      )}

      <div
        role="img"
        aria-label={segments.map((s) => `${s.label}: ${formatValue(s.value)}`).join(', ')}
        style={{
          display: 'flex',
          width: '100%',
          height,
          borderRadius: radius.pill,
          overflow: 'hidden',
          background: semanticColors.fill,
          gap: total > 0 ? 2 : 0,
        }}
      >
        {total > 0 &&
          segments
            .filter((s) => s.value > 0)
            .map((s) => (
              <div
                key={s.label}
                title={`${s.label}: ${formatValue(s.value)}`}
                style={{ width: `${(s.value / total) * 100}%`, backgroundColor: s.color }}
              />
            ))}
      </div>
    </div>
  );
};

export default StackedBar;

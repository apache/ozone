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
import { colors, fontFamilies, semanticColors, spacing, textStyles } from '../../theme/tokens';

export interface UtilityBarProps {
  /** Left slot, e.g. an app switcher or menu button. */
  leading?: React.ReactNode;
  /** Product / app title shown next to the leading slot. */
  title?: React.ReactNode;
  /** Optional centre slot (e.g. global search). */
  center?: React.ReactNode;
  /** Right slot, e.g. notification/user icon buttons. */
  actions?: React.ReactNode;
  /** Height in px. Defaults to 48. */
  height?: number;
  style?: React.CSSProperties;
}

/**
 * Global top utility bar (the dark chrome at the very top of every screen).
 * Provides leading/title, an optional centre slot and right-aligned actions.
 */
export const UtilityBar: React.FC<UtilityBarProps> = ({
  leading,
  title,
  center,
  actions,
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
      background: colors.pewter[950],
      color: 'rgb(255, 255, 255)',
      ...style,
    }}
  >
    <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
      {leading}
      {title && (
        <span
          style={{
            fontFamily: fontFamilies.appTitle,
            fontSize: textStyles.appTitle.fontSize,
            fontWeight: textStyles.appTitle.fontWeight,
            lineHeight: `${textStyles.appTitle.lineHeight}px`,
            color: 'rgb(255, 255, 255)',
          }}
        >
          {title}
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
        color: semanticColors.textDisabled,
      }}
    >
      {actions}
    </div>
  </div>
);

export default UtilityBar;

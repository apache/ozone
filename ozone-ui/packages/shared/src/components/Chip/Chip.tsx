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
import { Tag, type TagProps } from 'antd';
import { colors, radius } from '../../theme/tokens';

/**
 * Chip colours. Neutral, Blue, Green and Orange are the ones used across the
 * Ozone mockups; the rest complete the palette.
 */
export type ChipColor = 'neutral' | 'blue' | 'green' | 'orange' | 'red' | 'amber';

/** `full` = solid pill fill; `dot` = pill with a leading status dot. */
export type ChipVariant = 'full' | 'dot';

/** Chip sizes. */
export type ChipSize = 'standard' | 'small';

interface ChipPalette {
  bg: string;
  fg: string;
  border: string;
  dot: string;
}

const palettes: Record<ChipColor, ChipPalette> = {
  neutral: {
    bg: colors.pewter[50],
    fg: colors.pewter[700],
    border: colors.pewter[100],
    dot: colors.pewter[500],
  },
  blue: {
    bg: colors.blueNova[50],
    fg: colors.blueNova[700],
    border: colors.blueNova[100],
    dot: colors.blueNova[600],
  },
  green: {
    bg: colors.green[50],
    fg: colors.green[700],
    border: colors.green[100],
    dot: colors.green[500],
  },
  orange: {
    bg: colors.orange[50],
    fg: colors.orange[600],
    border: colors.orange[100],
    dot: colors.orange[400],
  },
  red: { bg: colors.red[50], fg: colors.red[700], border: colors.red[100], dot: colors.red[500] },
  amber: {
    bg: colors.amber[100],
    fg: colors.amber[700],
    border: colors.amber[200],
    dot: colors.amber[300],
  },
};

const sizeStyles: Record<ChipSize, React.CSSProperties> = {
  standard: { height: 22, paddingInline: 10, fontSize: 12, lineHeight: '20px' },
  small: { height: 18, paddingInline: 8, fontSize: 11, lineHeight: '16px' },
};

export interface ChipProps extends Omit<TagProps, 'color'> {
  /** Colour family. Defaults to `neutral`. */
  color?: ChipColor;
  /** `full` (solid pill) or `dot` (with a leading status dot). Defaults to `full`. */
  variant?: ChipVariant;
  /** Chip size. Defaults to `standard`. */
  size?: ChipSize;
  /** Selected state renders a stronger fill. */
  selected?: boolean;
}

/**
 * Chip / status label. A pill-shaped `Tag` mapped to the design-system colours
 * and sizes, supporting the `full`/`dot` variants, `standard`/`small` sizes, a
 * `selected` state and Ant Design's `closable` (removable filter chips).
 */
export const Chip: React.FC<ChipProps> = ({
  color = 'neutral',
  variant = 'full',
  size = 'standard',
  selected = false,
  style,
  children,
  ...rest
}) => {
  const palette = palettes[color];
  const dims = sizeStyles[size];

  return (
    <Tag
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
        margin: 0,
        borderRadius: radius.pill,
        fontWeight: 500,
        backgroundColor: selected ? palette.border : palette.bg,
        color: palette.fg,
        border: `1px solid ${selected ? palette.fg : palette.border}`,
        ...dims,
        ...style,
      }}
      {...rest}
    >
      {variant === 'dot' && (
        <span
          aria-hidden
          style={{
            width: 6,
            height: 6,
            borderRadius: radius.pill,
            backgroundColor: palette.dot,
            display: 'inline-block',
          }}
        />
      )}
      {children}
    </Tag>
  );
};

export default Chip;
